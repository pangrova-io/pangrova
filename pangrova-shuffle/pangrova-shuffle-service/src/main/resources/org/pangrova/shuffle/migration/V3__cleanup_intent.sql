CREATE TABLE shuffle_cleanup_intent (
    query_id VARCHAR(512) PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
