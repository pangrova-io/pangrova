CREATE TABLE shuffle_query_scope (
    query_id VARCHAR(512) PRIMARY KEY,
    owner_id VARCHAR(512) NOT NULL,
    owner_incarnation VARCHAR(512) NOT NULL,
    owner_fencing_token BIGINT NOT NULL CHECK (owner_fencing_token > 0),
    cleanup_expires_at_ms BIGINT NOT NULL CHECK (cleanup_expires_at_ms > 0),
    state VARCHAR(16) NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'CLEANING')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE shuffle_allocation (
    shuffle_id VARCHAR(128) PRIMARY KEY,
    query_id VARCHAR(512) NOT NULL REFERENCES shuffle_query_scope(query_id),
    stage_id INTEGER NOT NULL CHECK (stage_id >= 0),
    plan_epoch BIGINT NOT NULL CHECK (plan_epoch > 0),
    partition_count INTEGER NOT NULL CHECK (partition_count > 0),
    expected_producer_count INTEGER NOT NULL CHECK (expected_producer_count > 0),
    producer_set_checksum BYTEA NOT NULL CHECK (octet_length(producer_set_checksum) = 32),
    capability_expires_at_ms BIGINT NOT NULL CHECK (capability_expires_at_ms > 0),
    metadata_version BIGINT NOT NULL DEFAULT 0 CHECK (metadata_version >= 0),
    state VARCHAR(16) NOT NULL CHECK (state IN ('REGISTERED', 'FINALIZED', 'CLEANING')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finalized_at TIMESTAMPTZ,
    UNIQUE (query_id, stage_id, plan_epoch)
);

CREATE TABLE shuffle_batch (
    shuffle_id VARCHAR(128) NOT NULL REFERENCES shuffle_allocation(shuffle_id) ON DELETE CASCADE,
    producer_partition_id INTEGER NOT NULL CHECK (producer_partition_id >= 0),
    producer_attempt_id INTEGER NOT NULL CHECK (producer_attempt_id >= 0),
    producer_fencing_token BIGINT NOT NULL CHECK (producer_fencing_token > 0),
    output_partition_id INTEGER NOT NULL CHECK (output_partition_id >= 0),
    batch_sequence BIGINT NOT NULL CHECK (batch_sequence >= 0),
    output_sequence BIGINT CHECK (output_sequence >= 0),
    object_key VARCHAR(2048) NOT NULL,
    batch_size_bytes BIGINT NOT NULL CHECK (batch_size_bytes > 0),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    batch_checksum BYTEA NOT NULL CHECK (octet_length(batch_checksum) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shuffle_id, producer_partition_id, producer_attempt_id,
                 output_partition_id, batch_sequence),
    UNIQUE (object_key)
);

CREATE TABLE shuffle_attempt_fence (
    shuffle_id VARCHAR(128) NOT NULL REFERENCES shuffle_allocation(shuffle_id) ON DELETE CASCADE,
    producer_partition_id INTEGER NOT NULL CHECK (producer_partition_id >= 0),
    producer_attempt_id INTEGER NOT NULL CHECK (producer_attempt_id >= 0),
    producer_fencing_token BIGINT NOT NULL CHECK (producer_fencing_token > 0),
    task_id VARCHAR(512) NOT NULL,
    control_epoch BIGINT NOT NULL CHECK (control_epoch > 0),
    PRIMARY KEY (shuffle_id, producer_partition_id, producer_attempt_id)
);

CREATE TABLE shuffle_producer_commit (
    shuffle_id VARCHAR(128) NOT NULL REFERENCES shuffle_allocation(shuffle_id) ON DELETE CASCADE,
    producer_partition_id INTEGER NOT NULL CHECK (producer_partition_id >= 0),
    producer_attempt_id INTEGER NOT NULL CHECK (producer_attempt_id >= 0),
    producer_fencing_token BIGINT NOT NULL CHECK (producer_fencing_token > 0),
    task_id VARCHAR(512) NOT NULL,
    control_epoch BIGINT NOT NULL CHECK (control_epoch > 0),
    committed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shuffle_id, producer_partition_id)
);

CREATE TABLE shuffle_finalized_partition (
    shuffle_id VARCHAR(128) NOT NULL REFERENCES shuffle_allocation(shuffle_id) ON DELETE CASCADE,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    total_rows BIGINT NOT NULL CHECK (total_rows >= 0),
    committed_last_sequence BIGINT NOT NULL CHECK (committed_last_sequence >= -1),
    output_checksum BYTEA NOT NULL CHECK (octet_length(output_checksum) = 32),
    PRIMARY KEY (shuffle_id, partition_id)
);

CREATE INDEX shuffle_batch_read_idx
    ON shuffle_batch (shuffle_id, output_partition_id,
                      producer_partition_id, batch_sequence);
CREATE UNIQUE INDEX shuffle_batch_output_sequence_idx
    ON shuffle_batch (shuffle_id, output_partition_id, output_sequence)
    WHERE output_sequence IS NOT NULL;
CREATE INDEX shuffle_allocation_query_idx ON shuffle_allocation (query_id);
