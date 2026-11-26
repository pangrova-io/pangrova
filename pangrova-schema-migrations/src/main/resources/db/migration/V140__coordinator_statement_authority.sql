ALTER TABLE execution_anchor ADD COLUMN coordinator_work_token BIGINT;
ALTER TABLE execution_anchor ADD CONSTRAINT execution_anchor_coordinator_work_token
    CHECK (coordinator_work_token > 0 AND coordinator_work_token <= fencing_token);
