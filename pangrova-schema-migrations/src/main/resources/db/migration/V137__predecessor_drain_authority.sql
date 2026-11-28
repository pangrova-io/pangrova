ALTER TABLE execution_anchor
    ADD COLUMN recovery_drain_state SMALLINT NOT NULL DEFAULT 0
    CHECK (recovery_drain_state IN (0, 1, 2));

COMMENT ON COLUMN execution_anchor.recovery_drain_state IS
    '0 fresh execution, 1 successor awaiting predecessor exit, 2 terminal SQL predecessor proven drained';
