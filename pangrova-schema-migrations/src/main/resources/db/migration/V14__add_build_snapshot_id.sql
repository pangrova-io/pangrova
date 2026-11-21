-- Add build_snapshot_id to index_descriptors for snapshot-bound index versioning.
-- Tracks the snapshot at which each index generation was initially built.
ALTER TABLE index_descriptors
    ADD COLUMN IF NOT EXISTS build_snapshot_id VARCHAR(255);
