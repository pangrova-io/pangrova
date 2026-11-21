-- ============================================================================
-- V3: Add missing columns to coordinator_registry.
--
-- Columns added:
--   status               — operational status (ONLINE, DRAINING, OFFLINE)
--   last_processed_invalidation_id — tracks cache invalidation progress
--   capabilities         — JSON/text describing coordinator capabilities
--   version              — optimistic concurrency control version counter
--
-- Uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS (PostgreSQL 9.6+) for
-- idempotent application.
-- ============================================================================

-- Operational status for graceful shutdown / drain signaling.
ALTER TABLE coordinator_registry
    ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'ONLINE';

-- Highest invalidation_log ID processed by this coordinator's
-- JdbcInvalidationPoller. Used to determine DDL propagation progress.
ALTER TABLE coordinator_registry
    ADD COLUMN IF NOT EXISTS last_processed_invalidation_id BIGINT DEFAULT NULL;

-- Coordinator capabilities (e.g., supported features, protocol versions).
ALTER TABLE coordinator_registry
    ADD COLUMN IF NOT EXISTS capabilities TEXT DEFAULT NULL;

-- Optimistic concurrency control version for safe concurrent updates.
ALTER TABLE coordinator_registry
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
