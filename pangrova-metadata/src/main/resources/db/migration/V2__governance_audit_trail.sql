-- ============================================================================
-- V2: Governance audit trail table for persistent audit logging.
--
-- Replaces InMemoryGovernanceAuditTrail with durable JDBC storage.
-- Target dialect: PostgreSQL 15+
-- ============================================================================

CREATE TABLE IF NOT EXISTS governance_audit_trail (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_utc   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actor           VARCHAR(255) NOT NULL,
    operation_type  VARCHAR(64)  NOT NULL,
    resource_path   VARCHAR(1024) NOT NULL,
    before_state    JSONB        NOT NULL DEFAULT '{}',
    after_state     JSONB        NOT NULL DEFAULT '{}',
    details         TEXT         NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_gat_resource ON governance_audit_trail (resource_path, timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_gat_actor ON governance_audit_trail (actor, timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_gat_timestamp ON governance_audit_trail (timestamp_utc DESC);
