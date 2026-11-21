-- Durable active-active ownership for queries and background executions.
-- A cached sequence allocates never-reused fencing tokens without serializing
-- every acquisition through one counter row.
CREATE SEQUENCE IF NOT EXISTS execution_anchor_token_seq
    AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 1000;

CREATE TABLE IF NOT EXISTS execution_anchor (
    execution_id      VARCHAR(128) PRIMARY KEY,
    owner_id          VARCHAR(128) NOT NULL,
    owner_incarnation VARCHAR(64) NOT NULL,
    fencing_token     BIGINT NOT NULL CHECK (fencing_token > 0),
    acquired_at       TIMESTAMPTZ(6) NOT NULL,
    expires_at        TIMESTAMPTZ(6) NOT NULL,
    version           BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_execution_anchor_expiry
    ON execution_anchor (expires_at, execution_id);

CREATE INDEX IF NOT EXISTS idx_execution_anchor_owner
    ON execution_anchor (owner_id, owner_incarnation, expires_at);
