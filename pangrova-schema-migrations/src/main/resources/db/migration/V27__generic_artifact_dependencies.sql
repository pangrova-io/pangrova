-- Make generic artifact provenance and execution semantics durable. Existing
-- single-source jobs are normalized into a one-element dependency set.

ALTER TABLE artifact_build_jobs
    ADD COLUMN execution_kind VARCHAR(16) NOT NULL DEFAULT 'BUILD',
    ADD COLUMN empty_partition_policy VARCHAR(32) NOT NULL DEFAULT 'FORBID',
    ADD COLUMN dependency_set_sha256 VARCHAR(64);

ALTER TABLE artifact_build_jobs
    ADD CONSTRAINT chk_artifact_execution_kind
        CHECK (execution_kind IN ('BUILD', 'EXPORT')),
    ADD CONSTRAINT chk_artifact_empty_partition_policy
        CHECK (empty_partition_policy IN ('FORBID', 'ALLOW_ZERO_ROWS'));

CREATE TABLE artifact_build_dependencies (
    tenant_id           VARCHAR(255) NOT NULL,
    job_id              VARCHAR(255) NOT NULL,
    dependency_ordinal  INT NOT NULL,
    dependency_kind     VARCHAR(128) NOT NULL,
    dependency_id       VARCHAR(2048) NOT NULL,
    dependency_version  VARCHAR(1024) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, dependency_ordinal),
    CONSTRAINT fk_artifact_build_dependency_job FOREIGN KEY (tenant_id, job_id)
        REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE,
    CONSTRAINT uq_artifact_build_dependency_identity UNIQUE
        (tenant_id, job_id, dependency_kind, dependency_id),
    CONSTRAINT chk_artifact_build_dependency_ordinal CHECK
        (dependency_ordinal >= 0)
);

INSERT INTO artifact_build_dependencies
    (tenant_id, job_id, dependency_ordinal, dependency_kind,
     dependency_id, dependency_version)
SELECT tenant_id, job_id, 0, 'SOURCE_SNAPSHOT', source_id, source_snapshot
FROM artifact_build_jobs;

UPDATE artifact_build_jobs
SET dependency_set_sha256 = encode(sha256(
        int4send(octet_length(convert_to('SOURCE_SNAPSHOT', 'UTF8')))
            || convert_to('SOURCE_SNAPSHOT', 'UTF8')
            || int4send(octet_length(convert_to(source_id, 'UTF8')))
            || convert_to(source_id, 'UTF8')
            || int4send(octet_length(convert_to(source_snapshot, 'UTF8')))
            || convert_to(source_snapshot, 'UTF8')), 'hex');

ALTER TABLE artifact_build_jobs
    ALTER COLUMN dependency_set_sha256 SET NOT NULL,
    ADD CONSTRAINT chk_artifact_dependency_set_sha256
        CHECK (dependency_set_sha256 ~ '^[0-9a-f]{64}$');

ALTER TABLE artifact_build_outputs
    DROP CONSTRAINT chk_artifact_build_output_size;

ALTER TABLE artifact_build_outputs
    ADD CONSTRAINT chk_artifact_build_output_size
        CHECK (size_bytes > 0 AND row_count >= 0);
