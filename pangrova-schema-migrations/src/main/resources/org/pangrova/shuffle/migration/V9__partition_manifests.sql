ALTER TABLE shuffle_finalized_partition
    ADD COLUMN manifest_root BYTEA NOT NULL,
    ADD CONSTRAINT shuffle_finalized_partition_manifest_root CHECK (
        (committed_last_sequence = -1 AND octet_length(manifest_root) = 0)
        OR (committed_last_sequence BETWEEN 0 AND 255
            AND octet_length(manifest_root) = (committed_last_sequence + 1) * 64)
        OR (committed_last_sequence >= 256
            AND octet_length(manifest_root) BETWEEN 32 AND 2048
            AND octet_length(manifest_root) % 32 = 0)
    );

ALTER TABLE shuffle_batch DROP COLUMN output_sequence;
