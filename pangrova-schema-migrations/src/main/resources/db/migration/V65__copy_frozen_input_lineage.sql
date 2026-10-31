-- Derived task capabilities inherit an immutable root proof before Worker redemption.
ALTER TABLE copy_source_read_admissions
    ADD COLUMN parent_nonce_sha256 CHAR(64) REFERENCES copy_source_read_admissions(nonce_sha256);
ALTER TABLE copy_source_read_admissions DROP CONSTRAINT chk_copy_source_staged_proof;
ALTER TABLE copy_source_read_admissions
ADD CONSTRAINT chk_copy_source_staged_proof CHECK (
        (staged_source_version_id IS NULL AND staged_managed_object_uri IS NULL
            AND staged_content_sha256 IS NULL
            AND staged_content_size IS NULL AND staged_tenant_scope IS NULL AND staged_at IS NULL)
        OR ((redeemed_at IS NOT NULL OR parent_nonce_sha256 IS NOT NULL)
            AND staged_source_version_id IS NOT NULL
            AND staged_source_version_id <> '' AND staged_source_version_id <> 'null'
            AND octet_length(staged_source_version_id) <= 1024
            AND staged_managed_object_uri IS NOT NULL
            AND length(staged_managed_object_uri) > 0
            AND octet_length(staged_managed_object_uri) <= 2048
            AND staged_content_sha256 IS NOT NULL
            AND staged_content_sha256 ~ '^[0-9a-f]{64}$'
            AND staged_content_size IS NOT NULL AND staged_content_size >= 0
            AND staged_tenant_scope IS NOT NULL AND staged_tenant_scope ~ '^[0-9a-f]{64}$'
            AND staged_at IS NOT NULL));

-- Roots retain one-ticket-per-attempt semantics. Derived delivery capabilities
-- may be reissued for the exact same scope without persisting bearer secrets.
DROP INDEX copy_source_read_admissions_attempt;
CREATE UNIQUE INDEX copy_source_read_admissions_root_attempt
    ON copy_source_read_admissions (tenant_id, execution_id, staging_attempt_id)
    WHERE parent_nonce_sha256 IS NULL;
CREATE INDEX copy_source_read_admissions_input_attempt
    ON copy_source_read_admissions (tenant_id, execution_id, staging_attempt_id);

CREATE FUNCTION guard_copy_source_input_lineage()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE root copy_source_read_admissions%ROWTYPE;
BEGIN
    -- Hash collisions only serialize unrelated attempts; the immutable row
    -- comparison below still checks their full identities.
    PERFORM pg_advisory_xact_lock(207578818,
        hashtext(json_build_array(NEW.tenant_id,NEW.execution_id,NEW.staging_attempt_id)::text));
    IF EXISTS (SELECT 1 FROM copy_source_read_admissions a
            WHERE a.tenant_id=NEW.tenant_id AND a.execution_id=NEW.execution_id
                AND a.staging_attempt_id=NEW.staging_attempt_id
                AND (NEW.parent_nonce_sha256 IS NULL OR a.parent_nonce_sha256 IS NULL
                    OR a.parent_nonce_sha256 IS DISTINCT FROM NEW.parent_nonce_sha256
                    OR a.staging_command_sha256 IS DISTINCT FROM NEW.staging_command_sha256)) THEN
        RAISE EXCEPTION 'COPY task attempt is already bound to another input scope';
    END IF;
    IF NEW.parent_nonce_sha256 IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO root FROM copy_source_read_admissions WHERE nonce_sha256=NEW.parent_nonce_sha256;
    IF NOT FOUND OR root.parent_nonce_sha256 IS NOT NULL OR root.redeemed_at IS NULL
            OR root.staged_at IS NULL
            OR (NEW.tenant_id,NEW.principal,NEW.identity_groups,NEW.identity_attributes,
                NEW.authenticated_at,NEW.authorization_revision,NEW.execution_id,
                NEW.profile_name,NEW.profile_incarnation_id,NEW.profile_revision,
                NEW.canonical_source_url,NEW.managed_storage_root_uri,NEW.managed_backend_identity_sha256,
                NEW.staged_source_version_id,NEW.staged_managed_object_uri,NEW.staged_content_sha256,
                NEW.staged_content_size,NEW.staged_tenant_scope)
            IS DISTINCT FROM
               (root.tenant_id,root.principal,root.identity_groups,root.identity_attributes,
                root.authenticated_at,root.authorization_revision,root.execution_id,
                root.profile_name,root.profile_incarnation_id,root.profile_revision,
                root.canonical_source_url,root.managed_storage_root_uri,root.managed_backend_identity_sha256,
                root.staged_source_version_id,root.staged_managed_object_uri,root.staged_content_sha256,
                root.staged_content_size,root.staged_tenant_scope) THEN
        RAISE EXCEPTION 'COPY input must inherit one exact frozen root proof';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER copy_source_read_admissions_lineage_guard
BEFORE INSERT ON copy_source_read_admissions
FOR EACH ROW EXECUTE FUNCTION guard_copy_source_input_lineage();
