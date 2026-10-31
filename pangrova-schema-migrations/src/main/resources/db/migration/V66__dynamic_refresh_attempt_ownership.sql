-- An immutable refresh attempt owns one hidden candidate before Native work starts.
-- Attempts survive logical DROP so stale work cannot acquire a new incarnation.
CREATE TABLE dynamic_table_refresh_attempts (
    tenant_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(128) NOT NULL UNIQUE,
    logical_name VARCHAR(512) NOT NULL,
    incarnation_id UUID NOT NULL,
    base_revision BIGINT NOT NULL CHECK (base_revision >= 0),
    principal VARCHAR(512) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    owner_incarnation VARCHAR(64) NOT NULL,
    owner_token BIGINT NOT NULL CHECK (owner_token > 0),
    owner_acquired_at TIMESTAMPTZ(6) NOT NULL,
    owner_expires_at TIMESTAMPTZ(6) NOT NULL,
    control_epoch BIGINT NOT NULL CHECK (control_epoch > 0),
    candidate_table VARCHAR(512) NOT NULL UNIQUE,
    lifecycle_state VARCHAR(32) NOT NULL CHECK (lifecycle_state IN ('PREPARING', 'PUBLISHED')),
    commit_fingerprint VARCHAR(64),
    published_version BIGINT,
    manifest_sha256 BYTEA,
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, execution_id),
    CHECK ((lifecycle_state = 'PREPARING' AND commit_fingerprint IS NULL
                AND published_version IS NULL AND manifest_sha256 IS NULL)
        OR (lifecycle_state = 'PUBLISHED' AND commit_fingerprint IS NOT NULL
                AND commit_fingerprint ~ '^[0-9a-f]{64}$' AND published_version IS NOT NULL
                AND published_version > 0 AND manifest_sha256 IS NOT NULL
                AND octet_length(manifest_sha256) = 32))
);
CREATE INDEX dynamic_refresh_logical_incarnation
    ON dynamic_table_refresh_attempts (tenant_id, logical_name, incarnation_id, base_revision);

ALTER TABLE dynamic_table_publications
    ADD COLUMN last_refresh_execution_id VARCHAR(128),
    ADD COLUMN publisher_owner_id VARCHAR(128),
    ADD COLUMN publisher_owner_incarnation VARCHAR(64),
    ADD COLUMN publisher_owner_token BIGINT,
    ADD COLUMN publication_control_epoch BIGINT;

CREATE FUNCTION validate_dynamic_refresh_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    definition dynamic_table_publications%ROWTYPE;
    anchor execution_anchor%ROWTYPE;
    leader distributed_lease%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (to_jsonb(NEW) - ARRAY['lifecycle_state','commit_fingerprint','published_version','manifest_sha256'])
            IS DISTINCT FROM
            (to_jsonb(OLD) - ARRAY['lifecycle_state','commit_fingerprint','published_version','manifest_sha256'])
            OR OLD.lifecycle_state <> 'PREPARING' OR NEW.lifecycle_state <> 'PUBLISHED'
            OR pg_trigger_depth() < 2 THEN
            RAISE EXCEPTION 'Dynamic Table refresh identity or outcome cannot be replaced';
        END IF;
    ELSE
        IF NEW.lifecycle_state <> 'PREPARING' THEN
            RAISE EXCEPTION 'Dynamic Table refresh must begin before publication';
        END IF;
        SELECT * INTO definition FROM dynamic_table_publications p
            WHERE p.tenant_id=NEW.tenant_id AND p.logical_name=NEW.logical_name FOR SHARE;
        IF NOT FOUND OR (definition.incarnation_id,definition.revision)
            IS DISTINCT FROM (NEW.incarnation_id,NEW.base_revision)
            OR split_part(NEW.candidate_table,'.',1) <> split_part(NEW.logical_name,'.',1)
            OR split_part(NEW.candidate_table,'.',2) <> split_part(NEW.logical_name,'.',2)
            OR split_part(NEW.candidate_table,'.',3) !~ '^__pg_dt_[0-9a-f]{32}$'
            OR array_length(string_to_array(NEW.candidate_table,'.'),1) <> 3 THEN
            RAISE EXCEPTION 'Dynamic Table candidate must belong to the exact logical head';
        END IF;
        SELECT * INTO anchor FROM execution_anchor a WHERE a.execution_id=NEW.execution_id FOR SHARE;
        IF NOT FOUND OR (anchor.owner_id,anchor.owner_incarnation,anchor.fencing_token)
            IS DISTINCT FROM (NEW.owner_id,NEW.owner_incarnation,NEW.owner_token) THEN
            RAISE EXCEPTION 'Dynamic Table refresh must reserve under its actual execution owner';
        END IF;
        SELECT * INTO leader FROM distributed_lease WHERE lease_key='coordinator_leader' FOR SHARE;
        IF NOT FOUND OR leader.epoch IS DISTINCT FROM NEW.control_epoch
            OR leader.expires_at <= clock_timestamp() OR anchor.expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'Dynamic Table refresh reservation requires live execution and control leases';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER dynamic_refresh_attempt_identity
    BEFORE INSERT OR UPDATE ON dynamic_table_refresh_attempts
    FOR EACH ROW EXECUTE FUNCTION validate_dynamic_refresh_attempt();

-- Compact confirmed Native evidence survives the ordinary redo journal's retirement.
CREATE TABLE dynamic_table_refresh_native_commits (
    tenant_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(128) NOT NULL,
    commit_fingerprint VARCHAR(64) NOT NULL CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$'),
    published_version BIGINT NOT NULL CHECK (published_version > 0),
    manifest_sha256 BYTEA NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    PRIMARY KEY (tenant_id, execution_id),
    FOREIGN KEY (tenant_id, execution_id) REFERENCES dynamic_table_refresh_attempts (tenant_id, execution_id)
);

CREATE FUNCTION validate_dynamic_refresh_native_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Dynamic Table Native commit evidence is immutable';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM write_commit_journal w JOIN dynamic_table_refresh_attempts a
            ON a.tenant_id=w.tenant_id AND a.execution_id=w.query_id
        WHERE a.tenant_id=NEW.tenant_id AND a.execution_id=NEW.execution_id AND w.state='COMMITTED'
            AND w.commit_fingerprint=NEW.commit_fingerprint AND w.published_version=NEW.published_version
            AND w.manifest_sha256=NEW.manifest_sha256
            AND (w.principal,w.owner_id,w.owner_incarnation,w.execution_fencing_token)
                IS NOT DISTINCT FROM (a.principal,a.owner_id,a.owner_incarnation,a.owner_token)
            AND w.catalog_name || '.' || w.table_name=a.candidate_table) THEN
        RAISE EXCEPTION 'Dynamic Table Native evidence must match its exact committed intent';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER dynamic_refresh_native_commit_identity
    BEFORE INSERT OR UPDATE OR DELETE ON dynamic_table_refresh_native_commits
    FOR EACH ROW EXECUTE FUNCTION validate_dynamic_refresh_native_commit();

CREATE FUNCTION capture_dynamic_refresh_native_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO dynamic_table_refresh_native_commits
        (tenant_id,execution_id,commit_fingerprint,published_version,manifest_sha256)
        SELECT a.tenant_id,a.execution_id,NEW.commit_fingerprint,NEW.published_version,NEW.manifest_sha256
        FROM dynamic_table_refresh_attempts a
        WHERE a.tenant_id=NEW.tenant_id AND a.execution_id=NEW.query_id
            AND NEW.published_version IS NOT NULL AND NEW.manifest_sha256 IS NOT NULL
            AND (NEW.principal,NEW.owner_id,NEW.owner_incarnation,NEW.execution_fencing_token)
                IS NOT DISTINCT FROM (a.principal,a.owner_id,a.owner_incarnation,a.owner_token)
            AND NEW.catalog_name || '.' || NEW.table_name=a.candidate_table
        ON CONFLICT DO NOTHING;
    IF EXISTS (SELECT 1 FROM dynamic_table_refresh_native_commits p
        WHERE p.tenant_id=NEW.tenant_id AND p.execution_id=NEW.query_id
            AND (p.commit_fingerprint,p.published_version,p.manifest_sha256)
                IS DISTINCT FROM (NEW.commit_fingerprint,NEW.published_version,NEW.manifest_sha256)) THEN
        RAISE EXCEPTION 'Dynamic Table Native commit conflicts with retained evidence';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER dynamic_refresh_capture_native_commit
    AFTER INSERT OR UPDATE OF state,published_version,manifest_sha256 ON write_commit_journal
    FOR EACH ROW WHEN (NEW.state='COMMITTED') EXECUTE FUNCTION capture_dynamic_refresh_native_commit();

CREATE FUNCTION validate_dynamic_table_publication() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    attempt dynamic_table_refresh_attempts%ROWTYPE;
    native_commit dynamic_table_refresh_native_commits%ROWTYPE;
    anchor execution_anchor%ROWTYPE;
    leader distributed_lease%ROWTYPE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.lifecycle_state <> 'CREATING' OR NEW.revision <> 0 OR NEW.published_table IS NOT NULL
            OR EXISTS (SELECT 1 FROM dynamic_table_refresh_attempts a
                WHERE a.tenant_id=NEW.tenant_id AND a.incarnation_id=NEW.incarnation_id) THEN
            RAISE EXCEPTION 'Dynamic Table creation must reserve a new unpublished incarnation';
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.tenant_id,NEW.logical_name,NEW.incarnation_id,NEW.definition_sql,
            NEW.binding_catalog,NEW.binding_schema,NEW.created_at)
        IS DISTINCT FROM
       (OLD.tenant_id,OLD.logical_name,OLD.incarnation_id,OLD.definition_sql,
            OLD.binding_catalog,OLD.binding_schema,OLD.created_at) THEN
        RAISE EXCEPTION 'Dynamic Table definition identity is immutable';
    END IF;
    IF NEW.revision <> OLD.revision+1 OR NEW.lifecycle_state <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Dynamic Table publication must advance exactly one revision';
    END IF;
    SELECT * INTO attempt FROM dynamic_table_refresh_attempts a
        WHERE a.tenant_id=OLD.tenant_id AND a.logical_name=OLD.logical_name
            AND a.incarnation_id=OLD.incarnation_id AND a.base_revision=OLD.revision
            AND a.execution_id=NEW.last_refresh_execution_id AND a.candidate_table=NEW.published_table;
    IF NOT FOUND OR attempt.lifecycle_state <> 'PREPARING' THEN
        RAISE EXCEPTION 'Dynamic Table candidate has no exact preparing attempt';
    END IF;
    SELECT * INTO anchor FROM execution_anchor a WHERE a.execution_id=attempt.execution_id FOR SHARE;
    IF NOT FOUND OR (anchor.owner_id,anchor.owner_incarnation,anchor.fencing_token)
        IS DISTINCT FROM (NEW.publisher_owner_id,NEW.publisher_owner_incarnation,NEW.publisher_owner_token)
        OR anchor.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Dynamic Table publisher owner is absent, replaced or expired';
    END IF;
    SELECT * INTO leader FROM distributed_lease WHERE lease_key='coordinator_leader' FOR SHARE;
    IF NOT FOUND OR leader.epoch IS DISTINCT FROM NEW.publication_control_epoch
        OR leader.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Dynamic Table publisher control term is absent, replaced or expired';
    END IF;
    SELECT * INTO native_commit FROM dynamic_table_refresh_native_commits p
        WHERE p.tenant_id=attempt.tenant_id AND p.execution_id=attempt.execution_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Dynamic Table candidate has no exact committed Native publication';
    END IF;
    UPDATE dynamic_table_refresh_attempts SET lifecycle_state='PUBLISHED',
        commit_fingerprint=native_commit.commit_fingerprint, published_version=native_commit.published_version,
        manifest_sha256=native_commit.manifest_sha256
        WHERE tenant_id=attempt.tenant_id AND execution_id=attempt.execution_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Dynamic Table refresh ownership disappeared during publication';
    END IF;
    -- The candidate/receipt locks can wait. Check the authoritative clock after the final write.
    IF anchor.expires_at <= clock_timestamp() OR leader.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'Dynamic Table publication lease expired while waiting for Native evidence';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER dynamic_table_publication_identity
    BEFORE INSERT OR UPDATE ON dynamic_table_publications
    FOR EACH ROW EXECUTE FUNCTION validate_dynamic_table_publication();
