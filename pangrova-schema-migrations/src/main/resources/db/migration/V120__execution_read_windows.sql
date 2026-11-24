-- A read window survives retirement of its anchor. A successor waits until
-- the window expires or the exact reader releases it after its I/O drains.
CREATE TABLE execution_read_window (
    read_id UUID PRIMARY KEY,
    execution_id VARCHAR(128) NOT NULL,
    protected_until TIMESTAMPTZ(6) NOT NULL
);

CREATE INDEX idx_execution_read_window_execution
    ON execution_read_window (execution_id, protected_until);
CREATE INDEX idx_execution_read_window_expiry
    ON execution_read_window (protected_until, read_id);
