-- Generic, durable control-plane state for immutable artifact construction.
-- Artifact bytes never pass through these tables; Workers write them directly
-- to the fenced staging URI and report only immutable object metadata.

CREATE TABLE IF NOT EXISTS artifact_build_jobs (
    tenant_id                    VARCHAR(255) NOT NULL,
    job_id                       VARCHAR(255) NOT NULL,
    artifact_id                  VARCHAR(255) NOT NULL,
    artifact_type                VARCHAR(128) NOT NULL,
    staging_generation           VARCHAR(255) NOT NULL,
    source_id                    VARCHAR(1024) NOT NULL,
    source_snapshot              VARCHAR(512) NOT NULL,
    staging_uri                  VARCHAR(2048) NOT NULL,
    format_id                    VARCHAR(255) NOT NULL,
    format_major                 INT NOT NULL,
    format_minor                 INT NOT NULL,
    schema_sha256                VARCHAR(64) NOT NULL,
    writer_build_id              VARCHAR(255) NOT NULL,
    encryption_key_ref           VARCHAR(1024) NOT NULL DEFAULT '',
    idempotency_key              VARCHAR(512) NOT NULL,
    expected_pages               INT NOT NULL,
    completed_pages              INT NOT NULL DEFAULT 0,
    state                        VARCHAR(32) NOT NULL,
    publication_owner            VARCHAR(255),
    publication_owner_epoch      BIGINT,
    publication_fencing_token    BIGINT NOT NULL DEFAULT 0,
    publication_lease_expires_at TIMESTAMPTZ(6),
    failure_reason               VARCHAR(2048),
    created_at                   TIMESTAMPTZ(6) NOT NULL,
    updated_at                   TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT uq_artifact_build_identity UNIQUE
        (tenant_id, artifact_id, staging_generation, idempotency_key),
    CONSTRAINT chk_artifact_build_pages CHECK
        (expected_pages >= 0 AND completed_pages >= 0
         AND completed_pages <= expected_pages),
    CONSTRAINT chk_artifact_build_format_version CHECK
        (format_major > 0 AND format_minor >= 0),
    CONSTRAINT chk_artifact_build_state CHECK
        (state IN ('QUEUED', 'RUNNING', 'READY_TO_PUBLISH',
                   'PUBLISHED', 'FAILED', 'CANCELLED'))
);

CREATE TABLE IF NOT EXISTS artifact_build_pages (
    tenant_id          VARCHAR(255) NOT NULL,
    job_id             VARCHAR(255) NOT NULL,
    page_id            VARCHAR(255) NOT NULL,
    stage_id           INT NOT NULL,
    partition_id       INT NOT NULL,
    page_ordinal       INT NOT NULL,
    page_kind          VARCHAR(32) NOT NULL,
    state              VARCHAR(32) NOT NULL,
    attempt_id         INT NOT NULL DEFAULT -1,
    fencing_token      BIGINT NOT NULL DEFAULT 0,
    lease_owner        VARCHAR(255),
    owner_epoch        BIGINT,
    lease_expires_at   TIMESTAMPTZ(6),
    next_available_at  TIMESTAMPTZ(6) NOT NULL,
    max_attempts       INT NOT NULL,
    report_bytes       BYTEA,
    report_sha256      VARCHAR(64),
    failure_reason     VARCHAR(2048),
    created_at         TIMESTAMPTZ(6) NOT NULL,
    updated_at         TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, page_id),
    CONSTRAINT fk_artifact_build_page_job FOREIGN KEY (tenant_id, job_id)
        REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE,
    CONSTRAINT uq_artifact_build_page_ordinal UNIQUE
        (tenant_id, job_id, page_ordinal),
    CONSTRAINT uq_artifact_build_page_partition UNIQUE
        (tenant_id, job_id, stage_id, partition_id),
    CONSTRAINT chk_artifact_build_page_state CHECK
        (state IN ('PENDING', 'LEASED', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT chk_artifact_build_page_kind CHECK
        (page_kind IN ('SOURCE_MAP', 'DEPENDENCY_REDUCE', 'GENERATION_FINALIZE')),
    CONSTRAINT chk_artifact_build_page_attempt CHECK
        (attempt_id >= -1 AND max_attempts > 0)
);

CREATE TABLE IF NOT EXISTS artifact_build_page_dependencies (
    tenant_id           VARCHAR(255) NOT NULL,
    job_id              VARCHAR(255) NOT NULL,
    page_id             VARCHAR(255) NOT NULL,
    dependency_ordinal  INT NOT NULL,
    dependency_page_id  VARCHAR(255) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, page_id, dependency_ordinal),
    CONSTRAINT fk_artifact_dependency_page FOREIGN KEY (tenant_id, job_id, page_id)
        REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE,
    CONSTRAINT fk_artifact_dependency_input
        FOREIGN KEY (tenant_id, job_id, dependency_page_id)
        REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE,
    CONSTRAINT uq_artifact_dependency UNIQUE
        (tenant_id, job_id, page_id, dependency_page_id),
    CONSTRAINT chk_artifact_dependency_not_self CHECK
        (page_id <> dependency_page_id)
);

CREATE TABLE IF NOT EXISTS artifact_build_page_sources (
    tenant_id          VARCHAR(255) NOT NULL,
    job_id             VARCHAR(255) NOT NULL,
    page_id            VARCHAR(255) NOT NULL,
    source_ordinal     INT NOT NULL,
    source_object_id   VARCHAR(2048) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, page_id, source_ordinal),
    CONSTRAINT fk_artifact_build_source_page FOREIGN KEY (tenant_id, job_id, page_id)
        REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE,
    CONSTRAINT uq_artifact_build_page_source UNIQUE
        (tenant_id, job_id, page_id, source_object_id)
);

CREATE TABLE IF NOT EXISTS artifact_build_outputs (
    tenant_id           VARCHAR(255) NOT NULL,
    job_id              VARCHAR(255) NOT NULL,
    page_id             VARCHAR(255) NOT NULL,
    object_id           VARCHAR(255) NOT NULL,
    opaque_uri          VARCHAR(2048) NOT NULL,
    object_role         VARCHAR(128) NOT NULL,
    source_object_id    VARCHAR(2048) NOT NULL,
    object_ordinal      INT NOT NULL,
    object_scope        VARCHAR(32) NOT NULL,
    size_bytes          BIGINT NOT NULL,
    row_count           BIGINT NOT NULL,
    checksum_sha256     VARCHAR(64) NOT NULL,
    encryption_key_ref  VARCHAR(1024) NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, job_id, page_id, object_id),
    CONSTRAINT fk_artifact_build_output_page FOREIGN KEY (tenant_id, job_id, page_id)
        REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE,
    CONSTRAINT uq_artifact_build_output_ordinal UNIQUE
        (tenant_id, job_id, page_id, object_ordinal),
    CONSTRAINT chk_artifact_build_output_size CHECK
        (size_bytes > 0 AND row_count > 0),
    CONSTRAINT chk_artifact_build_output_scope CHECK
        (object_scope IN ('SOURCE_OBJECT', 'WORK_UNIT', 'GENERATION'))
);

CREATE INDEX IF NOT EXISTS idx_artifact_build_page_claim
    ON artifact_build_pages
        (state, next_available_at, lease_expires_at, tenant_id, job_id, page_ordinal);

CREATE INDEX IF NOT EXISTS idx_artifact_build_job_publish
    ON artifact_build_jobs
        (state, publication_lease_expires_at, updated_at, tenant_id, job_id);
