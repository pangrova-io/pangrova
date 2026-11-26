-- Bind every usable index to the exact indexed-column schema observed at creation.
-- Existing active indexes have no trustworthy binding and must be rebuilt before use.
ALTER TABLE index_descriptors
    ADD COLUMN source_schema_fingerprint VARCHAR(64);

UPDATE index_descriptors
SET state = 'STALE',
    stale_reason = 'source_schema_fingerprint_required',
    updated_at = clock_timestamp()
WHERE state IN ('READY','STALE')
  AND source_schema_fingerprint IS NULL;

-- Write-ahead schema journal. Delivery never depends on the bounded in-memory
-- metadata event dispatcher; descriptor fencing and the intent commit together.
CREATE TABLE index_schema_evolution_outbox (
    operation_id      UUID PRIMARY KEY,
    index_id          VARCHAR(255),
    tenant_id         VARCHAR(255) NOT NULL,
    catalog_name      VARCHAR(255) NOT NULL,
    schema_name       VARCHAR(255) NOT NULL,
    table_name        VARCHAR(255) NOT NULL,
    event_type        VARCHAR(32) NOT NULL,
    old_table_name    VARCHAR(255),
    old_column_name   VARCHAR(255),
    new_column_name   VARCHAR(255),
    old_column_type   VARCHAR(255),
    new_column_type   VARCHAR(255),
    new_schema_name   VARCHAR(255),
    state             VARCHAR(16) NOT NULL,
    created_at        TIMESTAMPTZ(6) NOT NULL,
    updated_at        TIMESTAMPTZ(6) NOT NULL,
    applied_at        TIMESTAMPTZ(6),
    CONSTRAINT chk_index_schema_evolution_state CHECK
        (state IN ('INTENDED', 'COMMITTED', 'APPLIED', 'ABORTED')),
    CONSTRAINT chk_index_schema_evolution_applied CHECK
        ((state = 'APPLIED') = (applied_at IS NOT NULL))
);

CREATE INDEX idx_index_schema_evolution_pending
    ON index_schema_evolution_outbox (state, created_at, operation_id);
CREATE INDEX idx_index_schema_evolution_table
    ON index_schema_evolution_outbox (tenant_id, catalog_name, schema_name, table_name, created_at);
