-- Governed, immutable Semantic View versions. Definitions are metadata only;
-- query compilation and materialization remain separate control/data-plane concerns.

CREATE TABLE semantic_view_versions (
    tenant_id             VARCHAR(255) NOT NULL,
    catalog_name          VARCHAR(255) NOT NULL,
    schema_name           VARCHAR(255) NOT NULL,
    view_name             VARCHAR(255) NOT NULL,
    version               BIGINT NOT NULL,
    owner_principal       VARCHAR(512) NOT NULL,
    definition_json       JSONB NOT NULL,
    definition_sha256     VARCHAR(64) NOT NULL,
    validation_state      VARCHAR(16) NOT NULL,
    validation_message    VARCHAR(2048),
    created_at            TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, catalog_name, schema_name, view_name, version),
    CONSTRAINT uq_semantic_view_definition UNIQUE
        (tenant_id, catalog_name, schema_name, view_name, definition_sha256),
    CONSTRAINT chk_semantic_view_version CHECK (version > 0),
    CONSTRAINT chk_semantic_view_sha256 CHECK
        (definition_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT chk_semantic_view_validation CHECK
        (validation_state IN ('VALIDATED', 'INVALID')),
    CONSTRAINT chk_semantic_view_validation_message CHECK
        ((validation_state = 'VALIDATED' AND validation_message IS NULL)
         OR (validation_state = 'INVALID' AND validation_message IS NOT NULL))
);

CREATE TABLE semantic_view_active_versions (
    tenant_id          VARCHAR(255) NOT NULL,
    catalog_name       VARCHAR(255) NOT NULL,
    schema_name        VARCHAR(255) NOT NULL,
    view_name          VARCHAR(255) NOT NULL,
    active_version     BIGINT NOT NULL,
    publication_epoch  BIGINT NOT NULL,
    published_by       VARCHAR(512) NOT NULL,
    published_at       TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, catalog_name, schema_name, view_name),
    CONSTRAINT fk_semantic_view_active_version FOREIGN KEY
        (tenant_id, catalog_name, schema_name, view_name, active_version)
        REFERENCES semantic_view_versions
            (tenant_id, catalog_name, schema_name, view_name, version),
    CONSTRAINT chk_semantic_view_publication_epoch CHECK (publication_epoch > 0)
);

-- Evaluation datasets and runs pin every mutable input by immutable version.
-- Run identity/configuration never changes; only a single fenced terminal transition is allowed.

CREATE TABLE evaluation_dataset_versions (
    tenant_id          VARCHAR(255) NOT NULL,
    dataset_id         VARCHAR(255) NOT NULL,
    version            BIGINT NOT NULL,
    owner_principal    VARCHAR(512) NOT NULL,
    source_snapshot    VARCHAR(1024) NOT NULL,
    manifest_uri       VARCHAR(2048) NOT NULL,
    manifest_sha256    VARCHAR(64) NOT NULL,
    item_count         BIGINT NOT NULL,
    schema_json        JSONB NOT NULL,
    created_at         TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, dataset_id, version),
    CONSTRAINT uq_evaluation_dataset_manifest UNIQUE
        (tenant_id, dataset_id, manifest_sha256),
    CONSTRAINT chk_evaluation_dataset_version CHECK (version > 0),
    CONSTRAINT chk_evaluation_dataset_items CHECK (item_count >= 0),
    CONSTRAINT chk_evaluation_dataset_sha256 CHECK
        (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE evaluation_runs (
    tenant_id          VARCHAR(255) NOT NULL,
    run_id             VARCHAR(255) NOT NULL,
    dataset_id         VARCHAR(255) NOT NULL,
    dataset_version    BIGINT NOT NULL,
    subject_type       VARCHAR(32) NOT NULL,
    subject_id         VARCHAR(512) NOT NULL,
    subject_version    VARCHAR(255) NOT NULL,
    baseline_run_id    VARCHAR(255),
    policy_version     VARCHAR(255) NOT NULL,
    state              VARCHAR(16) NOT NULL,
    started_at         TIMESTAMPTZ(6) NOT NULL,
    completed_at       TIMESTAMPTZ(6),
    trace_reference    VARCHAR(2048),
    failure_class      VARCHAR(255),
    PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT fk_evaluation_run_dataset FOREIGN KEY
        (tenant_id, dataset_id, dataset_version)
        REFERENCES evaluation_dataset_versions (tenant_id, dataset_id, version),
    CONSTRAINT fk_evaluation_run_baseline FOREIGN KEY
        (tenant_id, baseline_run_id)
        REFERENCES evaluation_runs (tenant_id, run_id),
    CONSTRAINT chk_evaluation_subject_type CHECK
        (subject_type IN ('MODEL', 'EMBEDDING_MODEL', 'SEARCH_SERVICE',
                          'SEMANTIC_VIEW', 'INDEX', 'CAPABILITY')),
    CONSTRAINT chk_evaluation_run_state CHECK
        (state IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT chk_evaluation_run_terminal CHECK
        ((state = 'RUNNING' AND completed_at IS NULL AND failure_class IS NULL)
         OR (state = 'SUCCEEDED' AND completed_at IS NOT NULL AND failure_class IS NULL)
         OR (state = 'FAILED' AND completed_at IS NOT NULL AND failure_class IS NOT NULL)
         OR (state = 'CANCELLED' AND completed_at IS NOT NULL))
);

CREATE TABLE evaluation_metrics (
    tenant_id       VARCHAR(255) NOT NULL,
    run_id          VARCHAR(255) NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    metric_unit     VARCHAR(64) NOT NULL,
    passed          BOOLEAN NOT NULL,
    evidence_ref    VARCHAR(2048),
    PRIMARY KEY (tenant_id, run_id, metric_name),
    CONSTRAINT fk_evaluation_metric_run FOREIGN KEY (tenant_id, run_id)
        REFERENCES evaluation_runs (tenant_id, run_id) ON DELETE CASCADE,
    CONSTRAINT chk_evaluation_metric_finite CHECK
        (metric_value NOT IN ('NaN'::DOUBLE PRECISION,
                              'Infinity'::DOUBLE PRECISION,
                              '-Infinity'::DOUBLE PRECISION))
);

CREATE TABLE evaluation_release_decisions (
    tenant_id      VARCHAR(255) NOT NULL,
    run_id         VARCHAR(255) NOT NULL,
    approved       BOOLEAN NOT NULL,
    policy_version VARCHAR(255) NOT NULL,
    reason         VARCHAR(2048) NOT NULL,
    decided_by     VARCHAR(512) NOT NULL,
    decided_at     TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT fk_evaluation_release_run FOREIGN KEY (tenant_id, run_id)
        REFERENCES evaluation_runs (tenant_id, run_id) ON DELETE CASCADE
);

CREATE INDEX idx_evaluation_dataset_versions
    ON evaluation_dataset_versions (tenant_id, dataset_id, version DESC);

CREATE INDEX idx_evaluation_runs_subject
    ON evaluation_runs (tenant_id, subject_type, subject_id, subject_version, started_at DESC);
