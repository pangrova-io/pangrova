ALTER TABLE index_commit_log
    ADD COLUMN commit_fingerprint VARCHAR(64);

ALTER TABLE index_commit_log
    ADD CONSTRAINT chk_icl_commit_fingerprint_sha256
    CHECK (commit_fingerprint IS NULL OR commit_fingerprint ~ '^[0-9a-f]{64}$');

CREATE UNIQUE INDEX uq_icl_journaled_commit
    ON index_commit_log (
        tenant_id, catalog_name, schema_name, table_name, commit_fingerprint)
    WHERE commit_fingerprint IS NOT NULL;
