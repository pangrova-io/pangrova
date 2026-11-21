CREATE TABLE shuffle_write_intent (
    reservation_id UUID PRIMARY KEY,
    shuffle_id VARCHAR(128) NOT NULL REFERENCES shuffle_allocation(shuffle_id) ON DELETE CASCADE,
    object_key VARCHAR(2048) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('IN_PROGRESS', 'ABORTED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX shuffle_write_intent_active_idx
    ON shuffle_write_intent (shuffle_id) WHERE state = 'IN_PROGRESS';
