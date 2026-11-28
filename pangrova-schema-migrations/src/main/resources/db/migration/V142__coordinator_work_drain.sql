ALTER TABLE execution_anchor
    DROP CONSTRAINT execution_anchor_coordinator_work_identity,
    ADD CONSTRAINT execution_anchor_coordinator_work_identity CHECK (
        (coordinator_work_domain IS NULL) = (coordinator_work_incarnation IS NULL)
        AND (coordinator_work_token IS NOT NULL OR (coordinator_work_domain IS NULL AND NOT coordinator_work_exited))
    );
