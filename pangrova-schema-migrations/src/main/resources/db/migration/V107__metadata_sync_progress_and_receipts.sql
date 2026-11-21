-- Synchronization checkpoints are source coverage, not diagnostic completion wall clocks.
-- Text Instant values preserve nanosecond precision; comparisons are made under the deployment lock.
CREATE TABLE metadata_sync_progress (
    catalog_name VARCHAR(255) NOT NULL,
    incarnation_id VARCHAR(255) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    checkpoint TEXT NOT NULL,
    PRIMARY KEY (catalog_name, incarnation_id, revision)
);
CREATE TABLE metadata_sync_runs (
    run_id UUID PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    incarnation_id VARCHAR(255) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    expected_checkpoint TEXT,
    next_checkpoint TEXT,
    sync_type VARCHAR(32) NOT NULL,
    sync_time TEXT NOT NULL,
    changes_applied INTEGER NOT NULL CHECK (changes_applied >= 0),
    errors INTEGER NOT NULL CHECK (errors >= 0),
    error_messages JSONB NOT NULL,
    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
    CHECK (next_checkpoint IS NULL OR errors = 0)
);
CREATE INDEX idx_metadata_sync_runs_deployment
    ON metadata_sync_runs(catalog_name, incarnation_id, revision);
