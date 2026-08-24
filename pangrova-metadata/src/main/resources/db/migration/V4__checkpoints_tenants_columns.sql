-- ============================================================================
-- V4: Add checkpoints table and missing tenants columns.
--
-- checkpoints: new table for JdbcCheckpointStore (task/component checkpoint state).
-- tenants: add quota columns (daily limits, storage), namespace restrictions,
--          resource pool, properties, and creator tracking.
-- ============================================================================

-- ── Checkpoints Table ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS checkpoints (
    component_id   VARCHAR(255) NOT NULL PRIMARY KEY,
    state          BYTEA        NOT NULL,
    saved_at       TIMESTAMP    NOT NULL
);

-- ── Tenants: Missing Quota Columns ──────────────────────────────────

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_daily_query_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_daily_scan_bytes  BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_storage_bytes     BIGINT NOT NULL DEFAULT 0;

-- ── Tenants: Namespace & Resource Columns ───────────────────────────

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS allowed_schemas TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS resource_pool   VARCHAR(255);
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS properties      TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS created_by      VARCHAR(255);
