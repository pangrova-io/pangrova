-- Generated-column values are immutable generic artifacts, not index descriptors.
-- This sidecar supplies an atomic active pointer and preserves older snapshot
-- generations for time-travel reads. Artifact bytes remain in object storage.

CREATE TABLE IF NOT EXISTS generated_value_artifacts (
    tenant_id              VARCHAR(255) NOT NULL,
    artifact_id             VARCHAR(255) NOT NULL,
    source_id               VARCHAR(1024) NOT NULL,
    catalog_name            VARCHAR(255) NOT NULL,
    schema_name             VARCHAR(255) NOT NULL,
    table_name              VARCHAR(255) NOT NULL,
    target_column           VARCHAR(255) NOT NULL,
    definition_sha256       VARCHAR(64) NOT NULL,
    target_generation       VARCHAR(255) NOT NULL,
    target_source_snapshot  VARCHAR(512) NOT NULL,
    active_generation       VARCHAR(255),
    active_source_snapshot  VARCHAR(512),
    publication_epoch       BIGINT NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ(6) NOT NULL,
    updated_at              TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, artifact_id),
    CONSTRAINT uq_generated_value_target UNIQUE
        (tenant_id, source_id, target_column)
);

CREATE TABLE IF NOT EXISTS generated_value_generation_manifests (
    tenant_id            VARCHAR(255) NOT NULL,
    artifact_id          VARCHAR(255) NOT NULL,
    generation           VARCHAR(255) NOT NULL,
    source_id            VARCHAR(1024) NOT NULL,
    source_snapshot      VARCHAR(512) NOT NULL,
    target_column        VARCHAR(255) NOT NULL,
    format_id            VARCHAR(255) NOT NULL,
    format_major         INT NOT NULL,
    format_minor         INT NOT NULL,
    schema_sha256        VARCHAR(64) NOT NULL,
    writer_build_id      VARCHAR(255) NOT NULL,
    object_count         BIGINT NOT NULL,
    row_count            BIGINT NOT NULL,
    total_bytes          BIGINT NOT NULL,
    manifest_sha256      VARCHAR(64) NOT NULL,
    publication_epoch    BIGINT NOT NULL,
    state                VARCHAR(16) NOT NULL,
    published_at         TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (tenant_id, artifact_id, generation),
    CONSTRAINT fk_generated_value_artifact FOREIGN KEY (tenant_id, artifact_id)
        REFERENCES generated_value_artifacts(tenant_id, artifact_id) ON DELETE CASCADE,
    CONSTRAINT uq_generated_value_publication_epoch
        UNIQUE (tenant_id, artifact_id, publication_epoch),
    CONSTRAINT chk_generated_value_generation_state
        CHECK (state IN ('ACTIVE', 'RETIRED'))
);

CREATE INDEX IF NOT EXISTS idx_generated_value_snapshot
    ON generated_value_generation_manifests
        (tenant_id, source_id, target_column, source_snapshot, state);
