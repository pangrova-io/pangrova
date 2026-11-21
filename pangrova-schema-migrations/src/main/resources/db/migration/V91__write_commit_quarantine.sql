-- Durable quarantine for distributed write intents whose outcome cannot be proven.
--
-- Recovery classifies an intent as QUARANTINED when the connector cannot rehydrate
-- the exact write, the catalog deployment differs, or a commit attempt failed
-- ambiguously. Such an intent must not be retried automatically: a retry could
-- double-commit or destroy evidence. The journal row stays PENDING for forensics;
-- this table makes the decision durable so later recovery passes skip the exact
-- fingerprint instead of re-attempting it forever.
--
-- Keyed by query_id and validated against commit_fingerprint: a new intent that
-- reuses a query id is not silently quarantined by an older decision.

CREATE TABLE IF NOT EXISTS write_commit_quarantine (
    query_id           VARCHAR(255)  NOT NULL,
    commit_fingerprint VARCHAR(64)   NOT NULL,
    catalog_name       VARCHAR(255)  NOT NULL,
    table_name         VARCHAR(1024) NOT NULL,
    reason             VARCHAR(1024) NOT NULL,
    quarantined_at     TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (query_id, commit_fingerprint),
    CONSTRAINT chk_write_commit_quarantine_fingerprint
        CHECK (commit_fingerprint ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_write_commit_quarantine_table
    ON write_commit_quarantine (catalog_name, table_name);
