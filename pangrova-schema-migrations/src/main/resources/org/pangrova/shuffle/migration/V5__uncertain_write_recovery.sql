ALTER TABLE shuffle_write_intent
    ADD COLUMN recovery_after TIMESTAMPTZ NOT NULL
        DEFAULT (clock_timestamp() + INTERVAL '10 minutes');

CREATE TABLE shuffle_uncertain_write_tombstone (
    shuffle_id VARCHAR(128) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    next_sweep_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner UUID,
    lease_expires_at TIMESTAMPTZ
);

CREATE INDEX shuffle_uncertain_write_tombstone_due_idx
    ON shuffle_uncertain_write_tombstone (next_sweep_at, lease_expires_at);
