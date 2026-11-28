ALTER TABLE execution_anchor
    ADD COLUMN coordinator_work_domain TEXT,
    ADD COLUMN coordinator_work_incarnation TEXT,
    ADD COLUMN coordinator_work_exited BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT execution_anchor_coordinator_work_identity CHECK (
        (coordinator_work_domain IS NULL AND coordinator_work_incarnation IS NULL AND NOT coordinator_work_exited)
        OR (coordinator_work_token IS NOT NULL AND coordinator_work_domain IS NOT NULL
            AND coordinator_work_incarnation IS NOT NULL)
    );

DROP INDEX idx_execution_anchor_owner;
CREATE INDEX idx_execution_anchor_owner ON execution_anchor (owner_incarnation, owner_id, expires_at);

DROP INDEX idx_execution_anchor_expiry;
CREATE INDEX idx_execution_anchor_expiry ON execution_anchor (expires_at, execution_id)
    WHERE coordinator_work_token IS NULL OR coordinator_work_exited;
