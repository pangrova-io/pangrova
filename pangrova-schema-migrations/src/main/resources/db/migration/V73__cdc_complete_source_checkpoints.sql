-- Preserve old representative columns for operator reconciliation. Their topic
-- and other partitions cannot be reconstructed from source_id or a batch hash.
ALTER TABLE cdc_commit_state ADD COLUMN offset_ranges JSONB;
ALTER TABLE cdc_commit_state ALTER COLUMN partition_num DROP NOT NULL;
ALTER TABLE cdc_commit_state ALTER COLUMN start_offset DROP NOT NULL;
ALTER TABLE cdc_commit_state ALTER COLUMN end_offset DROP NOT NULL;

-- Existing rows remain inspectable, but every new intent needs exact ranges.
-- Recovery rejects old active/committed rows until they are reconciled explicitly.
ALTER TABLE cdc_commit_state ADD CONSTRAINT cdc_complete_checkpoint
    CHECK (offset_ranges IS NOT NULL
           AND jsonb_typeof(offset_ranges) = 'array'
           AND jsonb_array_length(offset_ranges) > 0) NOT VALID;
