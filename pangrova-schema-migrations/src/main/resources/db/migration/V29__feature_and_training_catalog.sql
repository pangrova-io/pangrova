-- Governed Feature View and Training Dataset catalog objects. Definitions are
-- immutable and tenant scoped; all timestamps use PostgreSQL microsecond
-- precision so JDBC compare-and-set fences round-trip exactly.

CREATE TABLE feature_view_versions (
    tenant_id              VARCHAR(255) NOT NULL,
    catalog_name           VARCHAR(255) NOT NULL,
    schema_name            VARCHAR(255) NOT NULL,
    feature_view_name      VARCHAR(255) NOT NULL,
    version                BIGINT NOT NULL,
    owner_principal        VARCHAR(512) NOT NULL,
    definition_json        JSONB NOT NULL,
    definition_sha256      VARCHAR(64) NOT NULL,
    source_plan            BYTEA NOT NULL,
    source_plan_sha256     VARCHAR(64) NOT NULL,
    created_at             TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, catalog_name, schema_name, feature_view_name, version),
    CONSTRAINT uq_feature_view_definition UNIQUE
        (tenant_id, catalog_name, schema_name, feature_view_name, definition_sha256),
    CONSTRAINT chk_feature_view_version CHECK (version > 0),
    CONSTRAINT chk_feature_view_definition_sha CHECK
        (definition_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_feature_view_plan_sha CHECK
        (source_plan_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_feature_view_plan_nonempty CHECK (octet_length(source_plan) > 0)
);

CREATE TABLE feature_view_active_versions (
    tenant_id              VARCHAR(255) NOT NULL,
    catalog_name           VARCHAR(255) NOT NULL,
    schema_name            VARCHAR(255) NOT NULL,
    feature_view_name      VARCHAR(255) NOT NULL,
    active_version         BIGINT NOT NULL,
    publication_epoch      BIGINT NOT NULL,
    published_by           VARCHAR(512) NOT NULL,
    published_at           TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, catalog_name, schema_name, feature_view_name),
    CONSTRAINT fk_feature_view_active_version FOREIGN KEY
        (tenant_id, catalog_name, schema_name, feature_view_name, active_version)
        REFERENCES feature_view_versions
            (tenant_id, catalog_name, schema_name, feature_view_name, version),
    CONSTRAINT chk_feature_view_publication_epoch CHECK (publication_epoch > 0)
);

CREATE TABLE training_dataset_versions (
    tenant_id              VARCHAR(255) NOT NULL,
    catalog_name           VARCHAR(255) NOT NULL,
    schema_name            VARCHAR(255) NOT NULL,
    dataset_name           VARCHAR(255) NOT NULL,
    version                BIGINT NOT NULL,
    owner_principal        VARCHAR(512) NOT NULL,
    definition_json        JSONB NOT NULL,
    definition_sha256      VARCHAR(64) NOT NULL,
    normalized_plan        BYTEA NOT NULL,
    normalized_plan_sha256 VARCHAR(64) NOT NULL,
    split_kind             VARCHAR(16) NOT NULL,
    split_seed             BIGINT NOT NULL,
    created_at             TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, catalog_name, schema_name, dataset_name, version),
    CONSTRAINT uq_training_dataset_definition UNIQUE
        (tenant_id, catalog_name, schema_name, dataset_name, definition_sha256),
    CONSTRAINT chk_training_dataset_version CHECK (version > 0),
    CONSTRAINT chk_training_dataset_definition_sha CHECK
        (definition_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_training_dataset_plan_sha CHECK
        (normalized_plan_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_training_dataset_plan_nonempty CHECK (octet_length(normalized_plan) > 0),
    CONSTRAINT chk_training_dataset_split_kind CHECK
        (split_kind IN ('RANDOM', 'TEMPORAL', 'ENTITY'))
);

-- One durable export request is the immutable bridge from a catalog object to
-- the generic artifact state machine. Artifact bytes never enter this table.
CREATE TABLE catalog_export_requests (
    tenant_id              VARCHAR(255) NOT NULL,
    object_kind            VARCHAR(32) NOT NULL,
    catalog_name           VARCHAR(255) NOT NULL,
    schema_name            VARCHAR(255) NOT NULL,
    object_name            VARCHAR(255) NOT NULL,
    object_version         BIGINT NOT NULL,
    generation             VARCHAR(128) NOT NULL,
    artifact_id            VARCHAR(1024) NOT NULL,
    job_id                 VARCHAR(2048) NOT NULL,
    dependency_sha256      VARCHAR(64) NOT NULL,
    requested_by           VARCHAR(512) NOT NULL,
    requested_at           TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, object_kind, catalog_name, schema_name,
                 object_name, object_version, generation),
    CONSTRAINT uq_catalog_export_job UNIQUE (tenant_id, job_id),
    CONSTRAINT chk_catalog_export_kind CHECK
        (object_kind IN ('FEATURE_VIEW', 'TRAINING_DATASET')),
    CONSTRAINT chk_catalog_export_version CHECK (object_version > 0),
    CONSTRAINT chk_catalog_export_dependency_sha CHECK
        (dependency_sha256 ~ '^[0-9a-f]{64}$')
);

-- Generic publication metadata for immutable tabular exports. The manifest is
-- atomically committed with the artifact job's PUBLISHED transition.
CREATE TABLE tabular_export_generations (
    tenant_id              VARCHAR(255) NOT NULL,
    artifact_id            VARCHAR(1024) NOT NULL,
    generation             VARCHAR(128) NOT NULL,
    job_id                 VARCHAR(2048) NOT NULL,
    source_id              VARCHAR(2048) NOT NULL,
    source_snapshot        VARCHAR(1024) NOT NULL,
    format_id              VARCHAR(255) NOT NULL,
    format_major           INTEGER NOT NULL,
    format_minor           INTEGER NOT NULL,
    schema_sha256          VARCHAR(64) NOT NULL,
    writer_build_id        VARCHAR(255) NOT NULL,
    manifest_uri           VARCHAR(2048) NOT NULL,
    manifest_sha256        VARCHAR(64) NOT NULL,
    object_count           BIGINT NOT NULL,
    row_count              BIGINT NOT NULL,
    total_bytes            BIGINT NOT NULL,
    publication_epoch      BIGINT NOT NULL,
    state                  VARCHAR(16) NOT NULL,
    published_at           TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, artifact_id, generation),
    CONSTRAINT uq_tabular_export_job UNIQUE (tenant_id, job_id),
    CONSTRAINT chk_tabular_export_versions CHECK
        (format_major > 0 AND format_minor >= 0),
    CONSTRAINT chk_tabular_export_counts CHECK
        (object_count >= 0 AND row_count >= 0 AND total_bytes >= 0),
    CONSTRAINT chk_tabular_export_epoch CHECK (publication_epoch > 0),
    CONSTRAINT chk_tabular_export_state CHECK (state IN ('ACTIVE', 'RETIRED')),
    CONSTRAINT chk_tabular_export_manifest_sha CHECK
        (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_tabular_export_schema_sha CHECK
        (schema_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX uq_tabular_export_active
    ON tabular_export_generations (tenant_id, artifact_id)
    WHERE state = 'ACTIVE';
CREATE INDEX idx_catalog_export_object
    ON catalog_export_requests
        (tenant_id, object_kind, catalog_name, schema_name, object_name,
         object_version, requested_at DESC);
