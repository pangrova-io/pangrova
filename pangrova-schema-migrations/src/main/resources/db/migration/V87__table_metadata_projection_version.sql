-- table_metadata is a schema projection, not the authority for table identity.
-- The authority is table_control_registry; a controlled table's projection is
-- only usable while metadata_version matches the registry revision.
--
-- metadata_version zero means "legacy projection, never versioned". Readers must
-- reload from the connector rather than treating a zero-version projection as
-- current.

ALTER TABLE table_metadata ADD COLUMN IF NOT EXISTS table_id UUID;
ALTER TABLE table_metadata ADD COLUMN IF NOT EXISTS metadata_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE table_metadata ADD COLUMN IF NOT EXISTS projection_hash VARCHAR(64);

CREATE UNIQUE INDEX IF NOT EXISTS uq_table_metadata_table_id
    ON table_metadata (table_id)
    WHERE table_id IS NOT NULL;
