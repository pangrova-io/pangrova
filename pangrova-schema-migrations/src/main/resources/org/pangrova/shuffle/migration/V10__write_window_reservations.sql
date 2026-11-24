ALTER TABLE shuffle_write_intent DROP COLUMN state;

CREATE INDEX shuffle_write_intent_allocation_idx
    ON shuffle_write_intent (shuffle_id, recovery_after);
