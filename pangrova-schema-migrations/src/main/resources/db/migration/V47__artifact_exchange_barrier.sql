-- Existing tasks do not contain durable external allocation/finalize proof.
-- Preserve them and require explicit rebuild instead of manufacturing a barrier.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM artifact_build_jobs) THEN
        RAISE EXCEPTION 'Artifact exchange barrier migration requires explicit rebuild of existing artifact jobs';
    END IF;
END $$;

CREATE TABLE artifact_stage_exchanges (
    tenant_id VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    exchange_id VARCHAR(255) NOT NULL,
    producer_stage_id INTEGER NOT NULL CHECK (producer_stage_id >= 0),
    consumer_stage_id INTEGER NOT NULL CHECK (consumer_stage_id >= 0),
    graph_sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, exchange_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs (tenant_id, job_id) ON DELETE CASCADE
);

CREATE TABLE artifact_exchange_registrations (
    tenant_id VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    exchange_id VARCHAR(255) NOT NULL,
    registration_bytes BYTEA NOT NULL CHECK (octet_length(registration_bytes) BETWEEN 1 AND 16777216),
    registration_sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, exchange_id),
    FOREIGN KEY (tenant_id, job_id, exchange_id)
        REFERENCES artifact_stage_exchanges (tenant_id, job_id, exchange_id) ON DELETE CASCADE
);

CREATE TABLE artifact_exchange_finalizations (
    tenant_id VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    exchange_id VARCHAR(255) NOT NULL,
    finalization_bytes BYTEA NOT NULL CHECK (octet_length(finalization_bytes) BETWEEN 1 AND 16777216),
    finalization_sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (tenant_id, job_id, exchange_id),
    FOREIGN KEY (tenant_id, job_id, exchange_id)
        REFERENCES artifact_exchange_registrations (tenant_id, job_id, exchange_id) ON DELETE CASCADE
);

CREATE INDEX artifact_stage_consumer_barrier ON artifact_stage_exchanges (tenant_id, job_id, consumer_stage_id);
