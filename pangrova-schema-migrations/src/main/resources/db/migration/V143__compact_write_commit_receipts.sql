-- The compact identity is reserved with redo admission; final evidence survives redo retirement.
-- No receipt can be reconstructed for writes retired before this migration. Keep receipts until
-- the source-specific replay horizon can be proved; query-journal expiration is not that proof.
CREATE TABLE write_commit_receipts (
    query_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(1024) NOT NULL,
    write_mode VARCHAR(64),
    base_snapshot_id BIGINT NOT NULL CHECK (base_snapshot_id >= -1),
    commit_fingerprint VARCHAR(64) NOT NULL UNIQUE CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$'),
    state VARCHAR(32) NOT NULL CHECK (state IN ('PENDING', 'COMMITTED', 'ABORTED')),
    finalized_at_ms BIGINT,
    copy_write BOOLEAN NOT NULL,
    cdc_write BOOLEAN NOT NULL,
    published_version BIGINT,
    manifest_sha256 BYTEA,
    CHECK ((state = 'PENDING' AND finalized_at_ms IS NULL) OR
           (state <> 'PENDING' AND write_mode IS NOT NULL AND finalized_at_ms IS NOT NULL AND finalized_at_ms > 0)),
    CHECK ((published_version IS NULL AND manifest_sha256 IS NULL) OR
           (state = 'COMMITTED' AND published_version IS NOT NULL AND published_version > 0
            AND manifest_sha256 IS NOT NULL AND octet_length(manifest_sha256) = 32))
);

INSERT INTO write_commit_receipts (
    query_id, tenant_id, catalog_name, table_name, write_mode, base_snapshot_id,
    commit_fingerprint, state, finalized_at_ms, copy_write, cdc_write, published_version, manifest_sha256)
SELECT query_id, tenant_id, catalog_name, table_name, write_mode, base_snapshot_id,
    commit_fingerprint, state, CASE WHEN state <> 'PENDING' THEN timestamp_ms END,
    copy_claim_key IS NOT NULL, cdc_stream_id IS NOT NULL,
    CASE WHEN state = 'COMMITTED' THEN published_version END,
    CASE WHEN state = 'COMMITTED' THEN manifest_sha256 END
FROM write_commit_journal;

CREATE FUNCTION synchronize_write_commit_receipt()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- An existing terminal receipt alone conflicts with re-admission after redo is gone.
        -- The unique reservation makes this safe even across a concurrent retirement.
        INSERT INTO write_commit_receipts (
            query_id, tenant_id, catalog_name, table_name, write_mode, base_snapshot_id,
            commit_fingerprint, state, finalized_at_ms, copy_write, cdc_write, published_version, manifest_sha256)
        VALUES (NEW.query_id, NEW.tenant_id, NEW.catalog_name, NEW.table_name, NEW.write_mode,
            NEW.base_snapshot_id, NEW.commit_fingerprint, NEW.state,
            CASE WHEN NEW.state <> 'PENDING' THEN NEW.timestamp_ms END,
            NEW.copy_claim_key IS NOT NULL, NEW.cdc_stream_id IS NOT NULL,
            CASE WHEN NEW.state = 'COMMITTED' THEN NEW.published_version END,
            CASE WHEN NEW.state = 'COMMITTED' THEN NEW.manifest_sha256 END);
    ELSIF NEW.state <> OLD.state THEN
        IF OLD.state <> 'PENDING' OR NEW.state NOT IN ('COMMITTED', 'ABORTED') THEN
            RAISE EXCEPTION 'Write receipt terminal outcome cannot be changed';
        END IF;
        UPDATE write_commit_receipts SET state = NEW.state,
            finalized_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint,
            published_version = NEW.published_version, manifest_sha256 = NEW.manifest_sha256
        WHERE query_id = NEW.query_id AND commit_fingerprint = NEW.commit_fingerprint AND state = 'PENDING'
            AND tenant_id = NEW.tenant_id AND catalog_name = NEW.catalog_name AND table_name = NEW.table_name
            AND write_mode IS NOT DISTINCT FROM NEW.write_mode AND base_snapshot_id = NEW.base_snapshot_id
            AND copy_write = (NEW.copy_claim_key IS NOT NULL) AND cdc_write = (NEW.cdc_stream_id IS NOT NULL);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Write terminal receipt requires its exact pending identity';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER synchronize_write_commit_receipt
AFTER INSERT OR UPDATE OF state ON write_commit_journal
FOR EACH ROW EXECUTE FUNCTION synchronize_write_commit_receipt();

CREATE FUNCTION validate_write_commit_receipt_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM write_commit_journal w
                WHERE w.query_id = NEW.query_id AND w.commit_fingerprint = NEW.commit_fingerprint
                    AND w.state = NEW.state AND w.tenant_id = NEW.tenant_id
                    AND w.catalog_name = NEW.catalog_name AND w.table_name = NEW.table_name
                    AND w.write_mode IS NOT DISTINCT FROM NEW.write_mode
                    AND w.base_snapshot_id = NEW.base_snapshot_id
                    AND (w.copy_claim_key IS NOT NULL) = NEW.copy_write
                    AND (w.cdc_stream_id IS NOT NULL) = NEW.cdc_write
                    AND (NEW.state = 'PENDING' OR
                        (w.published_version IS NOT DISTINCT FROM NEW.published_version
                            AND w.manifest_sha256 IS NOT DISTINCT FROM NEW.manifest_sha256))) THEN
            RAISE EXCEPTION 'Write receipt insertion requires its exact journal authority';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP <> 'UPDATE' OR OLD.state <> 'PENDING' OR NEW.state NOT IN ('COMMITTED', 'ABORTED') THEN
        RAISE EXCEPTION 'Write commit receipt is immutable';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM write_commit_journal w
            WHERE w.query_id = OLD.query_id AND w.commit_fingerprint = OLD.commit_fingerprint
                AND w.state = NEW.state
                AND w.published_version IS NOT DISTINCT FROM NEW.published_version
                AND w.manifest_sha256 IS NOT DISTINCT FROM NEW.manifest_sha256)
            OR ROW(OLD.query_id, OLD.tenant_id, OLD.catalog_name, OLD.table_name, OLD.write_mode,
                OLD.base_snapshot_id, OLD.commit_fingerprint, OLD.copy_write, OLD.cdc_write)
                IS DISTINCT FROM ROW(NEW.query_id, NEW.tenant_id, NEW.catalog_name, NEW.table_name,
                NEW.write_mode, NEW.base_snapshot_id, NEW.commit_fingerprint, NEW.copy_write, NEW.cdc_write) THEN
        RAISE EXCEPTION 'Write receipt mutation differs from its terminal journal';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER validate_write_commit_receipt_mutation
BEFORE INSERT OR UPDATE OR DELETE ON write_commit_receipts
FOR EACH ROW EXECUTE FUNCTION validate_write_commit_receipt_mutation();

CREATE TRIGGER write_commit_receipts_no_truncate
BEFORE TRUNCATE ON write_commit_receipts
FOR EACH STATEMENT EXECUTE FUNCTION reject_write_commit_journal_truncate();
