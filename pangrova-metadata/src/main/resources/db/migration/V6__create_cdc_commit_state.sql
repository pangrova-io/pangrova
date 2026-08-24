-- CDC two-phase commit state: persists in-flight 2PC transactions for crash recovery
-- and idempotent deduplication based on (source_id, partition_num, start_offset, end_offset).

CREATE TABLE IF NOT EXISTS cdc_commit_state (
    id              VARCHAR(255)  NOT NULL PRIMARY KEY,
    source_id       VARCHAR(255)  NOT NULL,
    partition_num   INT           NOT NULL,
    start_offset    BIGINT        NOT NULL,
    end_offset      BIGINT        NOT NULL,
    state           VARCHAR(32)   NOT NULL,
    deadline        TIMESTAMP     NOT NULL,
    created_at      TIMESTAMP     NOT NULL
);

-- Idempotency dedup key: ensures at most one active transaction per offset range.
CREATE UNIQUE INDEX idx_cdc_commit_state_dedup
    ON cdc_commit_state (source_id, partition_num, start_offset, end_offset);

-- Recovery scan: find all in-flight transactions for a given source.
CREATE INDEX idx_cdc_commit_state_source_state
    ON cdc_commit_state (source_id, state);
