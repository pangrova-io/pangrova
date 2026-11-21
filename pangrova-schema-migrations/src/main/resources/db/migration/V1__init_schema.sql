-- ============================================================================
-- V1: Unified Pangrova schema — all tables for metadata, coordination,
--     governance, streaming, indexing, and audit.
--
-- Uses CREATE TABLE IF NOT EXISTS for idempotent initialization.
-- Target dialect: PostgreSQL 15+
-- ============================================================================

-- ── Metadata Catalog ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS catalog_definitions (
    name          VARCHAR(255) PRIMARY KEY,
    type          VARCHAR(50)  NOT NULL,
    provider      VARCHAR(50)  NOT NULL,
    state         VARCHAR(20)  NOT NULL DEFAULT 'CREATING',
    properties    JSONB        NOT NULL,
    comment       TEXT,
    allowed_tenants JSONB,
    created_by    VARCHAR(255),
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schema_metadata (
    catalog_name  VARCHAR(255) NOT NULL,
    schema_name   VARCHAR(255) NOT NULL,
    properties    JSONB,
    synced_at     TIMESTAMP    NOT NULL,
    PRIMARY KEY (catalog_name, schema_name),
    FOREIGN KEY (catalog_name) REFERENCES catalog_definitions(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS table_metadata (
    catalog_name     VARCHAR(255) NOT NULL,
    schema_name      VARCHAR(255) NOT NULL,
    table_name       VARCHAR(255) NOT NULL,
    columns          JSONB        NOT NULL,
    partition_spec   JSONB,
    sort_order       JSONB,
    properties       JSONB,
    table_created_at TIMESTAMP,
    table_updated_at TIMESTAMP,
    synced_at        TIMESTAMP    NOT NULL,
    PRIMARY KEY (catalog_name, schema_name, table_name),
    FOREIGN KEY (catalog_name, schema_name)
        REFERENCES schema_metadata(catalog_name, schema_name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sync_history (
    id             BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_name   VARCHAR(255) NOT NULL,
    sync_type      VARCHAR(20)  NOT NULL,
    sync_time      TIMESTAMP    NOT NULL,
    changes_applied INT         NOT NULL DEFAULT 0,
    errors         INT          NOT NULL DEFAULT 0,
    error_messages JSONB,
    duration_ms    BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_catalog_time ON sync_history (catalog_name, sync_time);

-- ── Coordination: State Journal ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS query_state_journal (
    id            BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    query_id      VARCHAR(64)  NOT NULL,
    sql_text      TEXT,
    session_json  TEXT,
    state         VARCHAR(32)  NOT NULL,
    timestamp_ms  BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_qsj_query_state ON query_state_journal (query_id, state);

CREATE TABLE IF NOT EXISTS task_state_journal (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    query_id        VARCHAR(64)  NOT NULL,
    task_id         VARCHAR(64)  NOT NULL,
    worker_id       VARCHAR(64),
    state           VARCHAR(32)  NOT NULL,
    stage_id        INT          NOT NULL,
    report_sequence BIGINT       NOT NULL DEFAULT 0,
    timestamp_ms    BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tsj_task_query ON task_state_journal (query_id, task_id);

-- ── Coordination: Orphaned Queries ──────────────────────────────────

CREATE TABLE IF NOT EXISTS orphaned_queries (
    query_id               VARCHAR(64)  NOT NULL PRIMARY KEY,
    original_coordinator   VARCHAR(128) NOT NULL,
    sql_text               TEXT,
    session_json           TEXT,
    released_at            BIGINT       NOT NULL,
    claimed_by             VARCHAR(128) DEFAULT NULL,
    claimed_at             BIGINT       DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_oq_unclaimed ON orphaned_queries (claimed_by, released_at);

-- ── Coordination: Service Registry ──────────────────────────────────

CREATE TABLE IF NOT EXISTS worker_registry (
    worker_id            VARCHAR(128) NOT NULL PRIMARY KEY,
    host                 VARCHAR(256) NOT NULL,
    http_port            INT          NOT NULL,
    transport_port       INT          NOT NULL,
    protocol_version     INT          NOT NULL DEFAULT 1,
    capabilities         TEXT,
    rack_id              VARCHAR(128) DEFAULT NULL,
    data_locality        TEXT,
    memory_pressure      VARCHAR(32)  NOT NULL DEFAULT 'NONE',
    cpu_utilization      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    running_task_count   INT          NOT NULL DEFAULT 0,
    available_memory_bytes BIGINT     NOT NULL DEFAULT 0,
    last_heartbeat_ms    BIGINT       NOT NULL,
    registered_at_ms     BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wr_heartbeat ON worker_registry (last_heartbeat_ms);

CREATE TABLE IF NOT EXISTS coordinator_registry (
    coordinator_id     VARCHAR(128) NOT NULL PRIMARY KEY,
    host               VARCHAR(256) NOT NULL,
    http_port          INT          NOT NULL,
    role               VARCHAR(32)  NOT NULL DEFAULT 'STANDBY',
    epoch              BIGINT       NOT NULL DEFAULT 0,
    last_heartbeat_ms  BIGINT       NOT NULL,
    registered_at_ms   BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cr_heartbeat ON coordinator_registry (last_heartbeat_ms);
CREATE INDEX IF NOT EXISTS idx_cr_role ON coordinator_registry (role);

-- ── Coordination: Distributed Lease ─────────────────────────────────

CREATE TABLE IF NOT EXISTS distributed_lease (
    lease_key   VARCHAR(255) PRIMARY KEY,
    holder_id   VARCHAR(255) NOT NULL,
    epoch       BIGINT       NOT NULL DEFAULT 0,
    acquired_at TIMESTAMP(3) NOT NULL,
    expires_at  TIMESTAMP(3) NOT NULL,
    version     BIGINT       NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dl_expires ON distributed_lease (expires_at);

-- ── Indexing: Descriptors & File Mappings ────────────────────────────

CREATE TABLE IF NOT EXISTS index_descriptors (
    index_id                VARCHAR(255) PRIMARY KEY,
    index_name              VARCHAR(255) NOT NULL,
    catalog_name            VARCHAR(255) NOT NULL,
    schema_name             VARCHAR(255) NOT NULL,
    table_name              VARCHAR(255) NOT NULL,
    column_names            JSONB        NOT NULL,
    index_type              VARCHAR(50)  NOT NULL,
    properties              JSONB        NOT NULL,
    state                   VARCHAR(20)  NOT NULL,
    stale_reason            VARCHAR(1024),
    generation              INT          NOT NULL DEFAULT 0,
    last_indexed_snapshot_id VARCHAR(255),
    build_owner             VARCHAR(255),
    build_heartbeat_at      TIMESTAMP,
    created_at              TIMESTAMP    NOT NULL,
    updated_at              TIMESTAMP    NOT NULL,
    CONSTRAINT uq_index_name_per_table
        UNIQUE (catalog_name, schema_name, table_name, index_name)
);

CREATE INDEX IF NOT EXISTS idx_index_descriptors_table_state
    ON index_descriptors (catalog_name, schema_name, table_name, state);
CREATE INDEX IF NOT EXISTS idx_descriptors_state_updated
    ON index_descriptors (state, updated_at);

CREATE TABLE IF NOT EXISTS index_file_mappings (
    id               BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    index_id         VARCHAR(255) NOT NULL,
    data_file_path   VARCHAR(2048) NOT NULL,
    index_file_path  VARCHAR(2048) NOT NULL,
    row_count        BIGINT       NOT NULL DEFAULT 0,
    index_size_bytes BIGINT       NOT NULL DEFAULT 0,
    checksum         VARCHAR(64),
    generation       INT          NOT NULL DEFAULT 0,
    snapshot_token   BIGINT       DEFAULT 0,
    sequence_num     BIGINT       NOT NULL DEFAULT 0,
    last_verified_at BIGINT       NOT NULL DEFAULT 0,
    created_at       TIMESTAMP    NOT NULL,
    CONSTRAINT fk_index_file_mapping_index
        FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_file_mappings_index_data
    ON index_file_mappings (index_id, data_file_path);
CREATE INDEX IF NOT EXISTS idx_file_mappings_snapshot
    ON index_file_mappings (index_id, data_file_path, snapshot_token);
CREATE INDEX IF NOT EXISTS idx_file_mappings_last_verified
    ON index_file_mappings (last_verified_at);
CREATE INDEX IF NOT EXISTS idx_file_mappings_verified_created
    ON index_file_mappings (last_verified_at, created_at);
CREATE INDEX IF NOT EXISTS idx_file_mappings_generation
    ON index_file_mappings (index_id, generation);

-- ── Indexing: Distributed Coordination ──────────────────────────────

CREATE TABLE IF NOT EXISTS index_maintenance_lease (
    lease_key   VARCHAR(255) PRIMARY KEY,
    holder_id   VARCHAR(255) NOT NULL,
    acquired_at TIMESTAMP    NOT NULL,
    expires_at  TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS index_commit_log (
    event_id     BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name  VARCHAR(255) NOT NULL,
    table_name   VARCHAR(255) NOT NULL,
    snapshot_id  VARCHAR(255),
    added_files  JSONB,
    removed_files JSONB,
    event_type   VARCHAR(50)  NOT NULL DEFAULT 'APPEND',
    processed    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_icl_unprocessed ON index_commit_log (processed, created_at);

CREATE TABLE IF NOT EXISTS index_build_claims (
    index_id     VARCHAR(255) NOT NULL,
    generation   INT          NOT NULL,
    owner_id     VARCHAR(255) NOT NULL,
    claimed_at   TIMESTAMP    NOT NULL,
    heartbeat_at TIMESTAMP    NOT NULL,
    PRIMARY KEY (index_id, generation)
);

CREATE TABLE IF NOT EXISTS index_cache_events (
    event_id     BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type   VARCHAR(50)  NOT NULL,
    index_id     VARCHAR(255),
    catalog_name VARCHAR(255),
    schema_name  VARCHAR(255),
    table_name   VARCHAR(255),
    created_at   TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS index_eval_breaker_state (
    index_id        VARCHAR(255) PRIMARY KEY,
    failure_count   INT          NOT NULL DEFAULT 0,
    last_failure_at TIMESTAMP,
    last_success_at TIMESTAMP,
    state           VARCHAR(20)  NOT NULL DEFAULT 'CLOSED'
);

-- ── Indexing: File Column Statistics (L0 partition metadata) ─────────

CREATE TABLE IF NOT EXISTS file_column_statistics (
    id               BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_name     VARCHAR(255) NOT NULL,
    schema_name      VARCHAR(255) NOT NULL,
    table_name       VARCHAR(255) NOT NULL,
    data_file_path   VARCHAR(2048) NOT NULL,
    snapshot_id      VARCHAR(255),
    column_name      VARCHAR(255) NOT NULL,
    min_value        VARCHAR(1024),
    max_value        VARCHAR(1024),
    null_count       BIGINT       NOT NULL DEFAULT 0,
    row_count        BIGINT       NOT NULL DEFAULT 0,
    approximate_ndv  BIGINT       NOT NULL DEFAULT 0,
    total_size_bytes BIGINT       NOT NULL DEFAULT 0,
    created_at       TIMESTAMP    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fcs_table
    ON file_column_statistics (catalog_name, schema_name, table_name, data_file_path);

-- ── Dynamic Tables ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dynamic_table_state (
    dt_name                  VARCHAR(512) PRIMARY KEY,
    tenant_id                VARCHAR(255) NOT NULL,
    lifecycle_state          VARCHAR(50)  NOT NULL,
    effective_refresh_mode   VARCHAR(50)  NOT NULL,
    target_lag_ms            BIGINT       NOT NULL,
    checkpoint               VARCHAR(1024),
    checkpoint_type          VARCHAR(50),
    source_schema_version    INT          NOT NULL DEFAULT 1,
    source_schema_snapshot   TEXT,
    creator_identity         VARCHAR(255),
    coordinator_id           VARCHAR(255),
    epoch                    BIGINT       NOT NULL DEFAULT 0,
    last_refresh_time        TIMESTAMP,
    total_refresh_count      BIGINT       NOT NULL DEFAULT 0,
    consecutive_failures     INT          NOT NULL DEFAULT 0,
    created_at               TIMESTAMP    NOT NULL,
    updated_at               TIMESTAMP    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dts_tenant_state
    ON dynamic_table_state (tenant_id, lifecycle_state);
CREATE INDEX IF NOT EXISTS idx_dt_state_name_epoch
    ON dynamic_table_state (dt_name, epoch);

CREATE TABLE IF NOT EXISTS dynamic_table_refresh_history (
    refresh_id      VARCHAR(255) PRIMARY KEY,
    dt_name         VARCHAR(512) NOT NULL,
    tenant_id       VARCHAR(255) NOT NULL,
    trigger_type    VARCHAR(50)  NOT NULL,
    start_time      TIMESTAMP    NOT NULL,
    end_time        TIMESTAMP    NOT NULL,
    status          VARCHAR(50)  NOT NULL,
    processed_rows  BIGINT       NOT NULL DEFAULT 0,
    bytes_written   BIGINT       NOT NULL DEFAULT 0,
    from_checkpoint VARCHAR(1024),
    to_checkpoint   VARCHAR(1024),
    error_message   VARCHAR(4096)
);

CREATE INDEX IF NOT EXISTS idx_dtrh_dt_start
    ON dynamic_table_refresh_history (dt_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_dtrh_tenant
    ON dynamic_table_refresh_history (tenant_id, start_time DESC);

-- ── Streaming: Ingestion Streams ────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingestion_stream_state (
    stream_name             VARCHAR(255) NOT NULL PRIMARY KEY,
    tenant_id               VARCHAR(255) NOT NULL,
    lifecycle_state         VARCHAR(32)  NOT NULL,
    target_table            VARCHAR(512) NOT NULL,
    source_properties       TEXT         NOT NULL,
    source_format           VARCHAR(64)  NOT NULL,
    stream_properties       TEXT,
    creator_identity        VARCHAR(255),
    coordinator_id          VARCHAR(255),
    epoch                   BIGINT       NOT NULL DEFAULT 0,
    schema_version          INT          NOT NULL DEFAULT 0,
    error_message           TEXT,
    records_consumed        BIGINT       NOT NULL DEFAULT 0,
    records_written         BIGINT       NOT NULL DEFAULT 0,
    records_dead_lettered   BIGINT       NOT NULL DEFAULT 0,
    last_offset_commit_time TIMESTAMP,
    created_at              TIMESTAMP    NOT NULL,
    updated_at              TIMESTAMP    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stream_tenant ON ingestion_stream_state (tenant_id);
CREATE INDEX IF NOT EXISTS idx_stream_state ON ingestion_stream_state (lifecycle_state);

CREATE TABLE IF NOT EXISTS ingestion_stream_offsets (
    stream_name      VARCHAR(255) NOT NULL,
    topic            VARCHAR(512) NOT NULL,
    partition_id     INT          NOT NULL,
    committed_offset BIGINT       NOT NULL,
    updated_at       TIMESTAMP    NOT NULL,
    PRIMARY KEY (stream_name, topic, partition_id),
    FOREIGN KEY (stream_name) REFERENCES ingestion_stream_state(stream_name)
        ON DELETE CASCADE
);

-- ── Streaming: Watermark Positions ──────────────────────────────────

CREATE TABLE IF NOT EXISTS watermark_positions (
    source_id  VARCHAR(512) NOT NULL PRIMARY KEY,
    position   TIMESTAMP    NOT NULL,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── Cache Invalidation ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS invalidation_log (
    id                BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cache_key         VARCHAR(1024) NOT NULL,
    invalidation_type VARCHAR(64)   NOT NULL,
    created_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── Governance: Quality Rules ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id          BIGINT        NOT NULL PRIMARY KEY,
    rule_name        VARCHAR(512)  NOT NULL,
    catalog_name     VARCHAR(255)  NOT NULL,
    schema_name      VARCHAR(255)  NOT NULL,
    table_name       VARCHAR(255)  NOT NULL,
    check_expression TEXT          NOT NULL,
    severity         VARCHAR(32)   NOT NULL,
    message          VARCHAR(1024),
    enabled          BOOLEAN       NOT NULL DEFAULT TRUE,
    schedule         VARCHAR(128),
    created_at       TIMESTAMP     NOT NULL,
    updated_at       TIMESTAMP     NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_check_results (
    result_id         BIGINT       NOT NULL PRIMARY KEY,
    rule_id           BIGINT       NOT NULL,
    rule_name         VARCHAR(512) NOT NULL,
    table_name        VARCHAR(255) NOT NULL,
    severity          VARCHAR(32)  NOT NULL,
    check_time        TIMESTAMP    NOT NULL,
    passed            BOOLEAN      NOT NULL,
    total_rows        BIGINT       NOT NULL DEFAULT 0,
    failed_rows       BIGINT       NOT NULL DEFAULT 0,
    execution_time_ms BIGINT       NOT NULL DEFAULT 0,
    error_message     VARCHAR(4096)
);

-- ── Governance: CDC Registrations ───────────────────────────────────

CREATE TABLE IF NOT EXISTS cdc_registrations (
    stream_id             VARCHAR(255) NOT NULL PRIMARY KEY,
    consumer_id           VARCHAR(255) NOT NULL,
    tenant_id             VARCHAR(255),
    state                 VARCHAR(32)  NOT NULL,
    max_concurrent_writes INT          NOT NULL DEFAULT 4,
    max_pending_bytes     BIGINT       NOT NULL DEFAULT 67108864,
    last_heartbeat        TIMESTAMP,
    created_at            TIMESTAMP    NOT NULL
);

-- ── Multi-Tenancy ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id                  VARCHAR(255) NOT NULL PRIMARY KEY,
    name                       VARCHAR(512) NOT NULL,
    status                     VARCHAR(32)  NOT NULL,
    max_memory_bytes           BIGINT       NOT NULL DEFAULT 0,
    max_concurrent_queries     INT          NOT NULL DEFAULT 0,
    max_queued_queries         INT          NOT NULL DEFAULT 0,
    max_cpu_time_per_query_ms  BIGINT       NOT NULL DEFAULT 0,
    max_scan_bytes_per_query   BIGINT       NOT NULL DEFAULT 0,
    allowed_catalogs           TEXT,
    created_at                 TIMESTAMP    NOT NULL,
    updated_at                 TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS tenant_user_memberships (
    user_principal VARCHAR(255) NOT NULL PRIMARY KEY,
    tenant_id      VARCHAR(255) NOT NULL,
    role           VARCHAR(32)  NOT NULL DEFAULT 'MEMBER'
);
