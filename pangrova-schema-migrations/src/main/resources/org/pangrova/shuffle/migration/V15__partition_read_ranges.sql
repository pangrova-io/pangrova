-- One fixed-width (start, end, bytes, rows, SHA-256) entry per published interval.
ALTER TABLE shuffle_finalized_partition
    ADD COLUMN read_ranges BYTEA NOT NULL DEFAULT ''::bytea,
    ADD CONSTRAINT shuffle_read_ranges_size CHECK (
        octet_length(read_ranges) <= 2048 AND octet_length(read_ranges) % 64 = 0
        AND octet_length(read_ranges) <> 64
    );
