-- Every local authority mutation advances one transactional version. A COPY
-- admission cannot revive after revoke/re-grant, role reassignment or profile
-- recreation, even when the final privilege happens to look the same.
CREATE TABLE pangrova_authorization_revision (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    revision BIGINT NOT NULL CHECK (revision > 0)
);
INSERT INTO pangrova_authorization_revision (singleton, revision) VALUES (TRUE, 1);

CREATE OR REPLACE FUNCTION pangrova_authority_mutation_gate()
RETURNS trigger LANGUAGE plpgsql AS $gate$
BEGIN
    IF TG_OP='TRUNCATE' THEN
        RAISE EXCEPTION 'Authority tables cannot be truncated; use gated DELETE';
    END IF;
    PERFORM pg_advisory_xact_lock(207578817, 1);
    UPDATE pangrova_authorization_revision SET revision = revision + 1 WHERE singleton = TRUE;
    RETURN NULL;
END
$gate$;

-- Ordinary SQL cannot reset the version to revive an old bearer ticket. The
-- revision row is advanced only as a nested effect of a protected authority
-- table statement. Schema owners remain trusted: they can alter triggers.
CREATE FUNCTION guard_pangrova_authorization_revision()
RETURNS trigger LANGUAGE plpgsql AS $guard$
BEGIN
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'Authorization revision is trigger-managed';
    END IF;
    IF pg_trigger_depth() < 2 OR NEW.singleton IS DISTINCT FROM OLD.singleton
            OR NEW.revision IS DISTINCT FROM OLD.revision + 1 THEN
        RAISE EXCEPTION 'Authorization revision is trigger-managed';
    END IF;
    RETURN NEW;
END
$guard$;

CREATE TRIGGER pangrova_authorization_revision_update_guard
BEFORE INSERT OR UPDATE OR DELETE ON pangrova_authorization_revision
FOR EACH ROW EXECUTE FUNCTION guard_pangrova_authorization_revision();

CREATE TRIGGER pangrova_authorization_revision_truncate_guard
BEFORE TRUNCATE ON pangrova_authorization_revision
FOR EACH STATEMENT EXECUTE FUNCTION guard_pangrova_authorization_revision();

-- A COPY source read is a short-lived, one-task bearer capability issued only
-- after the Coordinator checks current READ_STORAGE_PROFILE under the shared
-- authorization gate. Only SHA-256 of the 256-bit nonce is persisted.
CREATE TABLE copy_source_read_admissions (
    nonce_sha256 CHAR(64) PRIMARY KEY CHECK (nonce_sha256 ~ '^[0-9a-f]{64}$'),
    tenant_id TEXT NOT NULL CHECK (length(tenant_id) > 0),
    principal TEXT NOT NULL CHECK (length(principal) > 0),
    identity_groups JSONB NOT NULL,
    identity_attributes JSONB NOT NULL,
    authenticated_at TIMESTAMPTZ NOT NULL,
    authorization_revision BIGINT NOT NULL CHECK (authorization_revision > 0),
    profile_name VARCHAR(255) NOT NULL,
    profile_incarnation_id UUID NOT NULL,
    profile_revision BIGINT NOT NULL CHECK (profile_revision > 0),
    canonical_source_url TEXT NOT NULL CHECK (length(canonical_source_url) > 0
        AND octet_length(canonical_source_url) <= 1093),
    managed_storage_root_uri TEXT NOT NULL CHECK (length(managed_storage_root_uri) > 0
        AND octet_length(managed_storage_root_uri) <= 1024),
    managed_backend_identity_sha256 CHAR(64) NOT NULL
        CHECK (managed_backend_identity_sha256 ~ '^[0-9a-f]{64}$'),
    execution_id TEXT NOT NULL CHECK (length(execution_id) > 0),
    staging_attempt_id TEXT NOT NULL CHECK (length(staging_attempt_id) > 0),
    staging_command_sha256 CHAR(64) NOT NULL CHECK (staging_command_sha256 ~ '^[0-9a-f]{64}$'),
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    owner_incarnation TEXT NOT NULL CHECK (length(owner_incarnation) > 0),
    control_epoch BIGINT NOT NULL CHECK (control_epoch > 0),
    owner_fencing_token BIGINT NOT NULL CHECK (owner_fencing_token > 0),
    deadline_at TIMESTAMPTZ NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    redeemed_at TIMESTAMPTZ,
    staged_source_version_id TEXT,
    staged_managed_object_uri TEXT,
    staged_content_sha256 CHAR(64),
    staged_content_size BIGINT,
    staged_tenant_scope CHAR(64),
    staged_at TIMESTAMPTZ,
    CONSTRAINT chk_copy_source_staged_proof CHECK (
        (staged_source_version_id IS NULL AND staged_managed_object_uri IS NULL
            AND staged_content_sha256 IS NULL
            AND staged_content_size IS NULL AND staged_tenant_scope IS NULL AND staged_at IS NULL)
        OR (redeemed_at IS NOT NULL AND staged_source_version_id IS NOT NULL
            AND staged_source_version_id <> '' AND staged_source_version_id <> 'null'
            AND octet_length(staged_source_version_id) <= 1024
            AND staged_managed_object_uri IS NOT NULL
            AND length(staged_managed_object_uri) > 0
            AND octet_length(staged_managed_object_uri) <= 2048
            AND staged_content_sha256 IS NOT NULL
            AND staged_content_sha256 ~ '^[0-9a-f]{64}$'
            AND staged_content_size IS NOT NULL AND staged_content_size >= 0
            AND staged_tenant_scope IS NOT NULL AND staged_tenant_scope ~ '^[0-9a-f]{64}$'
            AND staged_at IS NOT NULL)),
    FOREIGN KEY (profile_name, profile_incarnation_id, profile_revision)
        REFERENCES storage_profile_revisions (name, incarnation_id, revision)
);

CREATE INDEX copy_source_read_admissions_execution
    ON copy_source_read_admissions (tenant_id, execution_id);

-- A retry must reuse the private ticket for its one durable attempt. A fresh
-- nonce for the same attempt could stage a second, conflicting source version.
CREATE UNIQUE INDEX copy_source_read_admissions_attempt
    ON copy_source_read_admissions (tenant_id, execution_id, staging_attempt_id);

-- No caller can rewrite the subject, source, task or profile after issuance.
-- The application may only mark the first successful exact redemption.
CREATE FUNCTION guard_copy_source_read_admission_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' OR TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'COPY source read admissions cannot be deleted or truncated';
    END IF;
    IF (to_jsonb(NEW) - 'redeemed_at' - 'staged_source_version_id' - 'staged_managed_object_uri'
            - 'staged_content_sha256' - 'staged_content_size' - 'staged_tenant_scope' - 'staged_at')
        IS DISTINCT FROM (to_jsonb(OLD) - 'redeemed_at' - 'staged_source_version_id'
            - 'staged_managed_object_uri'
            - 'staged_content_sha256' - 'staged_content_size' - 'staged_tenant_scope' - 'staged_at')
            OR (OLD.redeemed_at IS NULL AND NEW.redeemed_at IS NULL)
            OR (OLD.redeemed_at IS NOT NULL AND NEW.redeemed_at IS DISTINCT FROM OLD.redeemed_at)
            OR (OLD.staged_at IS NOT NULL AND (
                NEW.staged_source_version_id IS DISTINCT FROM OLD.staged_source_version_id
                OR NEW.staged_managed_object_uri IS DISTINCT FROM OLD.staged_managed_object_uri
                OR NEW.staged_content_sha256 IS DISTINCT FROM OLD.staged_content_sha256
                OR NEW.staged_content_size IS DISTINCT FROM OLD.staged_content_size
                OR NEW.staged_tenant_scope IS DISTINCT FROM OLD.staged_tenant_scope
                OR NEW.staged_at IS DISTINCT FROM OLD.staged_at)) THEN
        RAISE EXCEPTION 'COPY source read admission identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER copy_source_read_admissions_no_delete
BEFORE DELETE ON copy_source_read_admissions
FOR EACH ROW EXECUTE FUNCTION guard_copy_source_read_admission_mutation();

CREATE TRIGGER copy_source_read_admissions_no_truncate
BEFORE TRUNCATE ON copy_source_read_admissions
FOR EACH STATEMENT EXECUTE FUNCTION guard_copy_source_read_admission_mutation();

CREATE TRIGGER copy_source_read_admissions_update_guard
BEFORE UPDATE ON copy_source_read_admissions
FOR EACH ROW EXECUTE FUNCTION guard_copy_source_read_admission_mutation();
