CREATE TABLE shuffle_read_pin (
    pin_id UUID PRIMARY KEY,
    query_id VARCHAR(512) NOT NULL REFERENCES shuffle_query_scope(query_id) ON DELETE CASCADE,
    read_until_ms BIGINT NOT NULL CHECK (read_until_ms > 0),
    protect_until_ms BIGINT NOT NULL CHECK (protect_until_ms > read_until_ms)
);

CREATE INDEX shuffle_read_pin_query_expiry ON shuffle_read_pin (query_id, protect_until_ms);
