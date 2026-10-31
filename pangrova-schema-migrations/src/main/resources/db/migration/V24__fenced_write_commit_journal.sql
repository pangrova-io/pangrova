CREATE TABLE IF NOT EXISTS write_commit_journal (
    query_id VARCHAR(255) PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(1024) NOT NULL,
    state VARCHAR(32) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    fragments TEXT,
    write_mode VARCHAR(64),
    idempotency_key VARCHAR(512) NOT NULL,
    owner_id VARCHAR(255),
    owner_incarnation VARCHAR(255),
    execution_fencing_token BIGINT NOT NULL,
    CONSTRAINT chk_write_commit_owner CHECK (
        (owner_id IS NULL AND owner_incarnation IS NULL
            AND execution_fencing_token = 0)
        OR
        (owner_id IS NOT NULL AND owner_incarnation IS NOT NULL
            AND execution_fencing_token > 0)
    )
);

CREATE INDEX IF NOT EXISTS idx_write_commit_recovery
    ON write_commit_journal (state, query_id, timestamp_ms);
