-- All journaled distributed writes move to one fingerprint encoding. An old
-- intent cannot be replayed under the new encoding, including a COMMITTED row
-- that still anchors deferred work or an unprocessed publication event.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM write_commit_journal) THEN
        RAISE EXCEPTION 'Drain every write_commit_journal row before the single fingerprint upgrade';
    END IF;
END $$;

-- The stable file key remains tenant + table UUID + URI + S3 version. A later
-- authorized reload is a distinct generation; generation > 0 stays disabled
-- until the catalog can atomically fence its expected head.
ALTER TABLE copy_file_claims
    ADD COLUMN load_generation BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT chk_copy_file_generation CHECK (load_generation >= 0);
ALTER TABLE copy_file_claims ALTER COLUMN load_generation DROP DEFAULT;
ALTER TABLE copy_file_claims DROP CONSTRAINT copy_file_claims_pkey;
ALTER TABLE copy_file_claims ADD PRIMARY KEY (claim_key, load_generation);

ALTER TABLE write_commit_journal
    ADD COLUMN copy_claim_key CHAR(64),
    ADD COLUMN copy_load_generation BIGINT,
    ADD COLUMN copy_claim_digest CHAR(64),
    ADD COLUMN copy_table_uuid UUID,
    ADD CONSTRAINT chk_write_copy_binding CHECK (
        (copy_claim_key IS NULL AND copy_load_generation IS NULL
            AND copy_claim_digest IS NULL AND copy_table_uuid IS NULL)
        OR (copy_claim_key ~ '^[0-9a-f]{64}$'
            AND copy_load_generation >= 0
            AND copy_claim_digest ~ '^[0-9a-f]{64}$'
            AND copy_table_uuid IS NOT NULL AND write_mode = 'INSERT')),
    ADD CONSTRAINT fk_write_copy_claim FOREIGN KEY (copy_claim_key, copy_load_generation)
        REFERENCES copy_file_claims (claim_key, load_generation);

CREATE TABLE copy_file_publications (
    claim_key CHAR(64) NOT NULL,
    load_generation BIGINT NOT NULL,
    claim_digest CHAR(64) NOT NULL CHECK (claim_digest ~ '^[0-9a-f]{64}$'),
    execution_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    table_uuid UUID NOT NULL,
    commit_fingerprint CHAR(64) NOT NULL UNIQUE CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$'),
    snapshot_id BIGINT NOT NULL CHECK (snapshot_id > 0),
    published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (claim_key, load_generation),
    FOREIGN KEY (claim_key, load_generation)
        REFERENCES copy_file_claims (claim_key, load_generation)
);

CREATE FUNCTION validate_write_copy_binding()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    admitted copy_file_claims%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' AND (
            OLD.copy_claim_key IS DISTINCT FROM NEW.copy_claim_key
            OR OLD.copy_load_generation IS DISTINCT FROM NEW.copy_load_generation
            OR OLD.copy_claim_digest IS DISTINCT FROM NEW.copy_claim_digest
            OR OLD.copy_table_uuid IS DISTINCT FROM NEW.copy_table_uuid) THEN
        RAISE EXCEPTION 'COPY journal binding is immutable';
    END IF;
    IF NEW.copy_claim_key IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.state = 'ABORTED' THEN
        RAISE EXCEPTION 'COPY outcome cannot be declared aborted without catalog proof';
    END IF;
    IF TG_OP = 'UPDATE' AND (
            OLD.query_id IS DISTINCT FROM NEW.query_id
            OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
            OR OLD.commit_fingerprint IS DISTINCT FROM NEW.commit_fingerprint
            OR OLD.write_output_binding IS DISTINCT FROM NEW.write_output_binding) THEN
        RAISE EXCEPTION 'COPY write identity is immutable';
    END IF;
    SELECT * INTO admitted FROM copy_file_claims
        WHERE claim_key = NEW.copy_claim_key
            AND load_generation = NEW.copy_load_generation FOR KEY SHARE;
    IF NOT FOUND OR admitted.claim_digest <> NEW.copy_claim_digest
            OR admitted.execution_id <> NEW.query_id
            OR admitted.tenant_id <> NEW.tenant_id
            OR admitted.table_uuid <> NEW.copy_table_uuid THEN
        RAISE EXCEPTION 'COPY journal intent differs from durable file admission';
    END IF;
    IF NEW.state = 'COMMITTED' AND NOT EXISTS (
        SELECT 1 FROM copy_file_publications p
        WHERE p.claim_key = NEW.copy_claim_key
            AND p.load_generation = NEW.copy_load_generation
            AND p.commit_fingerprint = NEW.commit_fingerprint
            AND p.claim_digest = NEW.copy_claim_digest) THEN
        RAISE EXCEPTION 'COPY journal cannot commit before durable publication proof';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER write_commit_copy_binding_guard
BEFORE INSERT OR UPDATE ON write_commit_journal
FOR EACH ROW EXECUTE FUNCTION validate_write_copy_binding();

CREATE FUNCTION validate_copy_file_publication()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    intent write_commit_journal%ROWTYPE;
BEGIN
    SELECT * INTO intent FROM write_commit_journal
        WHERE query_id = NEW.execution_id FOR KEY SHARE;
    IF NOT FOUND OR intent.state NOT IN ('PENDING', 'COMMITTED')
            OR intent.copy_claim_key <> NEW.claim_key
            OR intent.copy_load_generation <> NEW.load_generation
            OR intent.copy_claim_digest <> NEW.claim_digest
            OR intent.copy_table_uuid <> NEW.table_uuid
            OR intent.tenant_id <> NEW.tenant_id
            OR intent.commit_fingerprint <> NEW.commit_fingerprint THEN
        RAISE EXCEPTION 'COPY publication differs from its durable write intent';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER copy_file_publication_guard
BEFORE INSERT ON copy_file_publications
FOR EACH ROW EXECUTE FUNCTION validate_copy_file_publication();

CREATE TRIGGER copy_file_publications_no_update_or_delete
BEFORE UPDATE OR DELETE ON copy_file_publications
FOR EACH ROW EXECUTE FUNCTION reject_copy_file_claim_mutation();

CREATE TRIGGER copy_file_publications_no_truncate
BEFORE TRUNCATE ON copy_file_publications
FOR EACH STATEMENT EXECUTE FUNCTION reject_copy_file_claim_mutation();

-- A direct SQL delete must not erase the only redo anchor for an unknown COPY
-- outcome. The application purge path is legal only after publication and
-- deferred work have both been durably acknowledged.
CREATE FUNCTION validate_copy_write_journal_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.copy_claim_key IS NOT NULL AND (
            OLD.state <> 'COMMITTED' OR NOT OLD.deferred_work_acknowledged
            OR NOT EXISTS (SELECT 1 FROM copy_file_publications p
                WHERE p.claim_key = OLD.copy_claim_key
                    AND p.load_generation = OLD.copy_load_generation
                    AND p.commit_fingerprint = OLD.commit_fingerprint)
            OR EXISTS (SELECT 1 FROM index_commit_log e
                WHERE e.commit_fingerprint = OLD.commit_fingerprint AND NOT e.processed)) THEN
        RAISE EXCEPTION 'COPY journal redo cannot be deleted before durable completion';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER write_commit_copy_delete_guard
BEFORE DELETE ON write_commit_journal
FOR EACH ROW EXECUTE FUNCTION validate_copy_write_journal_delete();

CREATE FUNCTION reject_write_commit_journal_truncate()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Write commit journal cannot be truncated';
END;
$$;

CREATE TRIGGER write_commit_journal_no_truncate
BEFORE TRUNCATE ON write_commit_journal
FOR EACH STATEMENT EXECUTE FUNCTION reject_write_commit_journal_truncate();
