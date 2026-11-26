-- The logical generation may be retried after a terminal Native build. Each
-- admission has a distinct persisted submission identity, including its pin,
-- job and artifact namespace. Other artifact types retain their original
-- uniqueness; an index generation may still have only one live/published job.
LOCK TABLE index_descriptors, index_generation_builds, artifact_snapshot_pins,
    artifact_build_jobs IN ACCESS EXCLUSIVE MODE;

ALTER TABLE index_descriptors ADD COLUMN native_submission_attempt_id VARCHAR(64)
    NOT NULL DEFAULT gen_random_uuid()::text;
ALTER TABLE index_generation_builds ADD COLUMN native_submission_attempt_id VARCHAR(64)
    NOT NULL DEFAULT gen_random_uuid()::text;

-- Existing exact current reservations keep the historical "submission" owner.
-- Do not attach an older unbound pin to a subsequently restarted reservation.
UPDATE index_descriptors d SET native_submission_attempt_id='submission'
WHERE d.index_type IN ('VECTOR','INVERTED') AND EXISTS (
    SELECT 1 FROM artifact_snapshot_pins p
    LEFT JOIN artifact_build_jobs j ON j.tenant_id=p.tenant_id AND j.job_id=p.job_id
    WHERE p.tenant_id=d.tenant_id AND p.index_id=d.index_id AND p.generation=d.generation
      AND p.source_snapshot=d.build_snapshot_id AND p.attempt_id='submission'
      AND ((d.state='BUILDING' AND p.state IN ('INTENDED','PINNED')
            AND p.created_at>=d.updated_at
            AND (p.job_id IS NULL OR j.publication_expected_updated_at=d.updated_at))
           OR (d.state IN ('READY','STALE','DROPPED') AND p.job_id IS NOT NULL))
);

UPDATE index_generation_builds b SET native_submission_attempt_id='submission'
FROM index_descriptors d
WHERE b.index_id=d.index_id AND b.state='BUILDING' AND EXISTS (
    SELECT 1 FROM artifact_snapshot_pins p
    LEFT JOIN artifact_build_jobs j ON j.tenant_id=p.tenant_id AND j.job_id=p.job_id
    WHERE p.tenant_id=d.tenant_id AND p.index_id=b.index_id AND p.generation=b.generation
      AND p.source_snapshot=b.source_snapshot AND p.attempt_id='submission'
      AND p.state IN ('INTENDED','PINNED') AND p.created_at>=b.started_at
      AND (p.job_id IS NULL OR j.publication_expected_updated_at=b.updated_at)
);

-- Retain historical pin identities and references, including outstanding
-- connector releases. The exact owner is the attempt-level unique key.
ALTER TABLE artifact_snapshot_pins DROP CONSTRAINT uq_snapshot_pin_generation;
ALTER TABLE artifact_snapshot_pins ADD CONSTRAINT uq_snapshot_pin_attempt
    UNIQUE (tenant_id,index_id,generation,attempt_id);

DROP INDEX uq_artifact_build_generation;
CREATE UNIQUE INDEX uq_artifact_build_generation ON artifact_build_jobs
    (tenant_id,artifact_id,staging_generation)
    WHERE artifact_type NOT IN ('VECTOR_INDEX','TEXT_INDEX') OR state NOT IN ('FAILED','CANCELLED');
