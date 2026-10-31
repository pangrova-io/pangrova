-- New executions require real backend ownership. Existing work has no such proof;
-- refuse transactionally rather than inventing owners or deleting recoverable jobs.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_build_jobs) THEN
        RAISE EXCEPTION 'Artifact execution fence migration requires explicit rebuild of existing artifact jobs';
    END IF;
END $$;

CREATE TABLE artifact_execution_routes (
    execution_id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    UNIQUE (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs (tenant_id, job_id) ON DELETE CASCADE
);

ALTER TABLE artifact_build_pages
    ADD COLUMN execution_owner_id VARCHAR(128),
    ADD COLUMN execution_owner_incarnation VARCHAR(64),
    ADD COLUMN execution_owner_token BIGINT,
    ADD CONSTRAINT chk_artifact_page_execution_owner CHECK (
        (execution_owner_id IS NULL AND execution_owner_incarnation IS NULL AND execution_owner_token IS NULL)
        OR (execution_owner_id IS NOT NULL AND execution_owner_incarnation IS NOT NULL AND execution_owner_token > 0)),
    ADD CONSTRAINT chk_artifact_leased_execution_owner CHECK (
        state NOT IN ('LEASED','SUCCEEDED') OR execution_owner_token IS NOT NULL);

ALTER TABLE artifact_build_jobs
    ADD COLUMN publication_execution_owner_id VARCHAR(128),
    ADD COLUMN publication_execution_owner_incarnation VARCHAR(64),
    ADD COLUMN publication_execution_owner_token BIGINT,
    ADD CONSTRAINT chk_artifact_publication_execution_owner CHECK (
        publication_owner IS NULL OR (publication_execution_owner_id IS NOT NULL
            AND publication_execution_owner_incarnation IS NOT NULL AND publication_execution_owner_token > 0));
