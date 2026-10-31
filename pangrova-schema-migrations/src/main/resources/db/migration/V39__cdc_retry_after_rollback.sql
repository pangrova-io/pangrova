-- A failed prepare may be retried with the same stable source batch key.
-- Keep the old rolled-back row for diagnosis, but enforce one live/committed
-- identity per source batch across coordinator processes.
DROP INDEX IF EXISTS idx_cdc_commit_state_dedup;

CREATE UNIQUE INDEX idx_cdc_commit_state_dedup
    ON cdc_commit_state (source_id, batch_key)
    WHERE state <> 'ROLLED_BACK';
