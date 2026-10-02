-- Persist the index-generation CAS timestamp with the durable work record so a
-- different maintenance leader can publish the exact generation after failover.
ALTER TABLE artifact_build_jobs
    ADD COLUMN IF NOT EXISTS publication_expected_updated_at TIMESTAMPTZ(6);

UPDATE artifact_build_jobs
SET publication_expected_updated_at = created_at
WHERE publication_expected_updated_at IS NULL;

ALTER TABLE artifact_build_jobs
    ALTER COLUMN publication_expected_updated_at SET NOT NULL;

ALTER TABLE artifact_build_jobs
    ADD COLUMN IF NOT EXISTS manifest_sha256 VARCHAR(64);
ALTER TABLE artifact_build_jobs
    ADD COLUMN IF NOT EXISTS published_object_count BIGINT;
ALTER TABLE artifact_build_jobs
    ADD COLUMN IF NOT EXISTS published_row_count BIGINT;
ALTER TABLE artifact_build_jobs
    ADD COLUMN IF NOT EXISTS published_total_bytes BIGINT;
ALTER TABLE artifact_build_jobs
    ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ(6);

ALTER TABLE index_generation_manifests
    ALTER COLUMN source_snapshot TYPE VARCHAR(512);

ALTER TABLE artifact_build_jobs
    DROP CONSTRAINT IF EXISTS chk_artifact_build_pages;
ALTER TABLE artifact_build_jobs
    ADD CONSTRAINT chk_artifact_build_pages CHECK
        (expected_pages >= 0 AND completed_pages >= 0
         AND completed_pages <= expected_pages);

CREATE INDEX IF NOT EXISTS idx_artifact_build_artifact_state
    ON artifact_build_jobs (tenant_id, artifact_id, state, updated_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_artifact_build_generation
    ON artifact_build_jobs (tenant_id, artifact_id, staging_generation);
