-- Maintenance authorization and history for LINKED and MANAGED tables.
--
-- A LINKED table starts with no enabled operations: adopting an external table
-- never grants maintenance rights implicitly. Operations must be enabled
-- explicitly, and every run is recorded with the version observed before it
-- started. The policy row is keyed by table_id so a drop/recreate under the same
-- name cannot inherit the previous authorization.

CREATE TABLE IF NOT EXISTS table_control_maintenance_policy (
    table_id                     UUID         PRIMARY KEY,
    enabled_operations           JSONB        NOT NULL DEFAULT '[]'::jsonb,
    retain_last                  INTEGER      NOT NULL DEFAULT 10,
    orphan_file_age_seconds      BIGINT       NOT NULL DEFAULT 259200,
    snapshot_retention_seconds   BIGINT       NOT NULL DEFAULT 604800,
    require_stable_external_version BOOLEAN   NOT NULL DEFAULT TRUE,
    updated_by                   VARCHAR(255) NOT NULL DEFAULT '',
    updated_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT table_control_maintenance_policy_retain_last_check
        CHECK (retain_last >= 1),
    -- Orphan deletion must never see in-flight writes as orphans.
    CONSTRAINT table_control_maintenance_policy_orphan_age_check
        CHECK (orphan_file_age_seconds >= 86400),
    CONSTRAINT table_control_maintenance_policy_snapshot_retention_check
        CHECK (snapshot_retention_seconds >= 86400)
);

CREATE TABLE IF NOT EXISTS table_control_maintenance_run (
    run_id          UUID         PRIMARY KEY,
    table_id        UUID         NOT NULL,
    operation       VARCHAR(32)  NOT NULL,
    mode            VARCHAR(16)  NOT NULL,
    state           VARCHAR(16)  NOT NULL,
    version_before  VARCHAR(1024),
    version_after   VARCHAR(1024),
    details         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    actor           VARCHAR(255) NOT NULL DEFAULT '',
    started_at      TIMESTAMPTZ  NOT NULL,
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_table_control_maintenance_run_table
    ON table_control_maintenance_run (table_id, started_at DESC);
