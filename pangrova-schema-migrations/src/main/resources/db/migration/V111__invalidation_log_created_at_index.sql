-- ============================================================================
-- V111: Index the invalidation log's cleanup predicate
--
-- The retention purge deletes by age:
--   DELETE FROM invalidation_log
--   WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '24 hours' AND id <= ?
-- Without an index on created_at that predicate is a sequential scan of the
-- whole log on every purge, which grows with ingest rate.
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_invalidation_log_created_at
    ON invalidation_log (created_at);
