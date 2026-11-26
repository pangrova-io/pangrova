-- Durable source snapshot intent precedes connector pinning and durable job creation.
-- The ledger, not a process-local reference count, owns retry and cleanup decisions.
CREATE TABLE artifact_snapshot_pins (
    tenant_id                 VARCHAR(255) NOT NULL,
    pin_owner                 VARCHAR(1024) NOT NULL,
    index_id                  VARCHAR(255) NOT NULL,
    generation                INT NOT NULL,
    attempt_id                VARCHAR(255) NOT NULL,
    source_id                 VARCHAR(1024) NOT NULL,
    source_snapshot           VARCHAR(512) NOT NULL,
    principal                 VARCHAR(512) NOT NULL,
    authorization_scope_bytes BYTEA NOT NULL,
    state                     VARCHAR(32) NOT NULL,
    job_id                    VARCHAR(255),
    created_at                TIMESTAMPTZ(6) NOT NULL,
    updated_at                TIMESTAMPTZ(6) NOT NULL,
    pinned_at                 TIMESTAMPTZ(6),
    released_at               TIMESTAMPTZ(6),
    PRIMARY KEY (tenant_id, pin_owner),
    CONSTRAINT uq_snapshot_pin_generation UNIQUE
        (tenant_id, index_id, generation, source_snapshot),
    CONSTRAINT fk_snapshot_pin_job FOREIGN KEY (tenant_id, job_id)
        REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE SET NULL,
    CONSTRAINT chk_snapshot_pin_generation CHECK (generation >= 0),
    CONSTRAINT chk_snapshot_pin_state CHECK
        (state IN ('INTENDED', 'PINNED', 'RELEASING', 'RELEASED')),
    CONSTRAINT chk_snapshot_pin_timestamps CHECK
        ((state = 'INTENDED' AND pinned_at IS NULL AND released_at IS NULL)
         OR (state = 'PINNED' AND pinned_at IS NOT NULL AND released_at IS NULL)
         OR (state = 'RELEASING' AND pinned_at IS NOT NULL AND released_at IS NULL)
         OR (state = 'RELEASED' AND pinned_at IS NOT NULL AND released_at IS NOT NULL))
);

-- Block old Coordinators from publishing Native jobs while the backfill and
-- authorization validation observe one stable job set during rolling upgrade.
LOCK TABLE artifact_build_jobs IN ACCESS EXCLUSIVE MODE;

-- Adopt already-published Native jobs that still carry their exact persisted
-- dispatch authority. They remain INTENDED until maintenance acquires and
-- validates the real connector pin; query retention fails closed meanwhile.
INSERT INTO artifact_snapshot_pins (
    tenant_id, pin_owner, index_id, generation, attempt_id, source_id,
    source_snapshot, principal, authorization_scope_bytes, state, job_id,
    created_at, updated_at, pinned_at, released_at
)
SELECT j.tenant_id,
       j.artifact_id || ':generation:' || j.staging_generation || ':attempt:legacy-' || md5(j.job_id),
       j.artifact_id,
       CAST(j.staging_generation AS INT),
       'legacy-' || md5(j.job_id),
       j.source_id,
       j.source_snapshot,
       s.principal,
       s.authorization_scope_bytes,
       'INTENDED',
       j.job_id,
       j.created_at,
       j.updated_at,
       NULL,
       NULL
FROM artifact_build_jobs j
JOIN artifact_build_dispatch_specs s
  ON s.tenant_id = j.tenant_id AND s.job_id = j.job_id
WHERE j.state = 'PUBLISHED'
  AND j.artifact_type IN ('VECTOR_INDEX', 'TEXT_INDEX')
  AND s.authorization_scope_bytes IS NOT NULL
  AND btrim(s.principal) <> ''
ON CONFLICT DO NOTHING;

-- Historical unscoped jobs cannot safely acquire source authority during a
-- query. Require explicit resubmission before the schema can advance.
DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM artifact_build_jobs j
        WHERE j.state = 'PUBLISHED'
          AND j.artifact_type IN ('VECTOR_INDEX', 'TEXT_INDEX')
          AND NOT EXISTS (
              SELECT 1
              FROM artifact_snapshot_pins p
              WHERE p.tenant_id = j.tenant_id AND p.job_id = j.job_id
          )
    ) THEN
        RAISE EXCEPTION 'Published Native index jobs without persisted authorization scope must be resubmitted before V121';
    END IF;
END
$migration$;

-- A published Native retrieval still hydrates physical candidate rows from its
-- source split. The execution anchor therefore owns a durable pin reference;
-- deleting the fenced anchor after reads drain releases this reference by FK.
CREATE TABLE artifact_snapshot_pin_references (
    tenant_id    VARCHAR(255) NOT NULL,
    pin_owner    VARCHAR(1024) NOT NULL,
    execution_id VARCHAR(128) NOT NULL,
    PRIMARY KEY (tenant_id, pin_owner, execution_id),
    CONSTRAINT fk_snapshot_pin_reference_pin FOREIGN KEY (tenant_id, pin_owner)
        REFERENCES artifact_snapshot_pins(tenant_id, pin_owner) ON DELETE CASCADE,
    CONSTRAINT fk_snapshot_pin_reference_execution FOREIGN KEY (execution_id)
        REFERENCES execution_anchor(execution_id) ON DELETE CASCADE
);

CREATE INDEX idx_snapshot_pin_cleanup
    ON artifact_snapshot_pins (state, created_at, tenant_id, index_id);
CREATE UNIQUE INDEX uq_snapshot_pin_job
    ON artifact_snapshot_pins (tenant_id, job_id) WHERE job_id IS NOT NULL;
CREATE INDEX idx_snapshot_pin_reference_execution
    ON artifact_snapshot_pin_references (execution_id);
