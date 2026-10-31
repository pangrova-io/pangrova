-- Replayable Native task payloads. Control-plane failover must reconstruct the
-- exact Substrait plan, complete OperatorBindingSet and canonical DataSplit bytes;
-- Java connector objects are deliberately never persisted here.

CREATE TABLE IF NOT EXISTS artifact_build_dispatch_specs (
    tenant_id                 VARCHAR(255) NOT NULL,
    job_id                    VARCHAR(255) NOT NULL,
    plan_bytes                BYTEA NOT NULL,
    plan_sha256               VARCHAR(64) NOT NULL,
    operator_bindings_bytes   BYTEA NOT NULL,
    operator_bindings_sha256  VARCHAR(64) NOT NULL,
    principal                 VARCHAR(512) NOT NULL DEFAULT '',
    max_memory_bytes          BIGINT NOT NULL,
    max_threads               INT NOT NULL,
    max_spill_bytes           BIGINT NOT NULL,
    task_timeout_millis       BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT fk_artifact_dispatch_job FOREIGN KEY (tenant_id, job_id)
        REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE,
    CONSTRAINT chk_artifact_dispatch_resources CHECK
        (max_memory_bytes > 0 AND max_threads > 0 AND max_spill_bytes > 0
         AND task_timeout_millis > 0)
);

ALTER TABLE artifact_build_page_sources
    ADD COLUMN IF NOT EXISTS split_bytes BYTEA;
ALTER TABLE artifact_build_page_sources
    ADD COLUMN IF NOT EXISTS split_sha256 VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_artifact_build_runnable_jobs
    ON artifact_build_jobs (state, updated_at, tenant_id, job_id);
