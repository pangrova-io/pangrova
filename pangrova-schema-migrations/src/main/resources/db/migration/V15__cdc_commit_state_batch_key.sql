-- A-EOS-01: the CDC 2PC dedup key was a single partition's offset range
-- (source_id, partition_num, start_offset, end_offset). For a multi-partition batch that
-- collides two DISTINCT batches sharing the same primary-partition range, silently mis-deduping
-- the second batch and losing its other-partition data. Replace the dedup key with a collision-free
-- full-batch fingerprint (batch_key) computed over ALL partitions of the batch.
--
-- The (partition_num, start_offset, end_offset) columns are retained: they still record the
-- per-batch representative range used by findLastCommittedOffsets for JDBC-side offset restore.

ALTER TABLE cdc_commit_state ADD COLUMN batch_key VARCHAR(255);

-- Backfill in-flight (transient) rows so the new unique index is consistent. The backfill mirrors
-- the previous single-range identity so existing PREPARED/COMMITTING rows keep their dedup semantics.
UPDATE cdc_commit_state
   SET batch_key = source_id || ':' || partition_num || ':' || start_offset || ':' || end_offset
 WHERE batch_key IS NULL;

-- Swap the dedup unique index from the single-partition range to the full-batch fingerprint.
DROP INDEX IF EXISTS idx_cdc_commit_state_dedup;

CREATE UNIQUE INDEX idx_cdc_commit_state_dedup
    ON cdc_commit_state (source_id, batch_key);
