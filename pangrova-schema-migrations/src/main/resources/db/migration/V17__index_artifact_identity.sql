-- Snapshot identifiers are connector-owned opaque tokens. Persist them as
-- strings and reject duplicate physical objects within one immutable index
-- generation so task binding is deterministic under coordinator failover.
ALTER TABLE index_file_mappings
    ALTER COLUMN snapshot_token TYPE VARCHAR(255)
    USING snapshot_token::text;

ALTER TABLE index_file_mappings
    ALTER COLUMN snapshot_token SET DEFAULT '';

UPDATE index_file_mappings
SET snapshot_token = ''
WHERE snapshot_token IS NULL;

ALTER TABLE index_file_mappings
    ALTER COLUMN snapshot_token SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_file_mappings_generation_data
    ON index_file_mappings (index_id, generation, data_file_path);

CREATE TABLE IF NOT EXISTS index_generation_manifests (
    index_id          VARCHAR(255) NOT NULL,
    generation        INT          NOT NULL,
    source_id         VARCHAR(1024) NOT NULL,
    source_snapshot   VARCHAR(255) NOT NULL DEFAULT '',
    artifact_type     VARCHAR(64)  NOT NULL,
    format_id         VARCHAR(255) NOT NULL,
    format_major      INT          NOT NULL,
    format_minor      INT          NOT NULL,
    schema_sha256     VARCHAR(64)  NOT NULL,
    writer_build_id   VARCHAR(255) NOT NULL,
    object_count      BIGINT       NOT NULL,
    row_count         BIGINT       NOT NULL,
    total_bytes       BIGINT       NOT NULL,
    manifest_sha256   VARCHAR(64)  NOT NULL,
    publication_epoch BIGINT       NOT NULL,
    state             VARCHAR(32)  NOT NULL,
    published_at      TIMESTAMPTZ(6) NOT NULL,
    PRIMARY KEY (index_id, generation),
    CONSTRAINT fk_index_generation_manifest
        FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_index_generation_active
    ON index_generation_manifests (index_id) WHERE state = 'ACTIVE';
