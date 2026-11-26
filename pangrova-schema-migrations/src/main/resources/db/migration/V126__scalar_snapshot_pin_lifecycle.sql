-- A scalar construction attempt must prove its source-reading stack has drained
-- before cleanup can release its exact opaque connector snapshot hold.
CREATE TABLE index_scalar_snapshot_pins (
    index_id VARCHAR(255) NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    attempt_owner VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    source_snapshot VARCHAR(255) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('INTENDED', 'DRAINED', 'RELEASING', 'RELEASED')),
    created_at TIMESTAMPTZ(6) NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL,
    next_attempt_at TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (index_id, generation, attempt_owner)
);
CREATE INDEX idx_scalar_snapshot_pins_cleanup
    ON index_scalar_snapshot_pins (next_attempt_at, created_at, index_id, generation, attempt_owner)
    WHERE state IN ('DRAINED', 'RELEASING');
