-- ============================================================================
-- V7: Query Audit Records table for JdbcAuditStore
-- Replaces in-memory ConcurrentHashMap storage in LogAuditStore with durable
-- JDBC-backed persistence for compliance and post-hoc analysis.
-- Target dialect: PostgreSQL 15+
-- ============================================================================

CREATE TABLE IF NOT EXISTS query_audit_records (
    query_id            VARCHAR(64)   NOT NULL PRIMARY KEY,
    user_id             VARCHAR(255),
    sql_text            TEXT,
    tenant_id           VARCHAR(255),
    stage_count         INT           NOT NULL DEFAULT 0,
    task_count          INT           NOT NULL DEFAULT 0,
    cpu_time_ms         BIGINT        NOT NULL DEFAULT 0,
    wall_time_ms        BIGINT        NOT NULL DEFAULT 0,
    peak_memory_bytes   BIGINT        NOT NULL DEFAULT 0,
    spilled_bytes       BIGINT        NOT NULL DEFAULT 0,
    input_rows          BIGINT        NOT NULL DEFAULT 0,
    output_rows         BIGINT        NOT NULL DEFAULT 0,
    input_bytes         BIGINT        NOT NULL DEFAULT 0,
    output_bytes        BIGINT        NOT NULL DEFAULT 0,
    shuffle_read_bytes  BIGINT        NOT NULL DEFAULT 0,
    shuffle_write_bytes BIGINT        NOT NULL DEFAULT 0,
    completed_at        TIMESTAMP,
    success             BOOLEAN       NOT NULL DEFAULT FALSE
);

-- Index for retention cleanup (deleteOlderThan queries by completed_at)
CREATE INDEX IF NOT EXISTS idx_qar_completed_at
    ON query_audit_records (completed_at);

-- Index for tenant-based audit queries
CREATE INDEX IF NOT EXISTS idx_qar_tenant_id
    ON query_audit_records (tenant_id);

-- Index for user-based audit queries
CREATE INDEX IF NOT EXISTS idx_qar_user_id
    ON query_audit_records (user_id);
