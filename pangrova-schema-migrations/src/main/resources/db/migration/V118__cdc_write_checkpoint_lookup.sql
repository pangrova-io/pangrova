CREATE INDEX idx_write_commit_cdc_checkpoint
    ON write_commit_journal (
        tenant_id, cdc_stream_id, cdc_source_epoch, (state = 'PENDING') DESC, timestamp_ms DESC, query_id DESC
    )
    WHERE cdc_stream_id IS NOT NULL AND state IN ('PENDING', 'COMMITTED');
