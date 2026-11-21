-- Immutable completion requests survive process loss before checkpoint/receipt publication.
-- No catalog FK: an obsolete deployment is explicitly fenced before its request is rejected.
CREATE TABLE metadata_sync_intents (
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
    state VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'APPLIED', 'REJECTED')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (next_checkpoint IS NULL OR errors = 0)
);
CREATE UNIQUE INDEX uq_metadata_sync_pending_catalog
    ON metadata_sync_intents(catalog_name) WHERE state = 'PENDING';
