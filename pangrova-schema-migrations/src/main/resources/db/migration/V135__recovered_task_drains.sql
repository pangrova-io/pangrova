ALTER TABLE task_state_journal
    ADD COLUMN recovery_drain_receipt BYTEA,
    ADD CONSTRAINT task_state_journal_recovery_drain_shape CHECK (
        recovery_drain_receipt IS NULL
        OR (admission_seal IS NOT NULL AND octet_length(recovery_drain_receipt) BETWEEN 1 AND 65536)
    );

CREATE INDEX idx_tsj_recovery_pending ON task_state_journal (query_id, id)
    WHERE admission_seal IS NOT NULL AND recovery_drain_receipt IS NULL;
