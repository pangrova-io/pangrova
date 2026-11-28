-- Each nonempty output contributes (partition id, batches, bytes, rows), in that order.
-- The statistics belong to the exact winner row and change with it in one statement.
ALTER TABLE shuffle_producer_commit
    ADD COLUMN partition_totals BIGINT[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT shuffle_producer_partition_totals_shape CHECK (
        cardinality(partition_totals) % 4 = 0
        AND (cardinality(partition_totals) = 0 OR
            (array_ndims(partition_totals) = 1 AND array_lower(partition_totals, 1) = 1))
        AND array_position(partition_totals, NULL::bigint) IS NULL
    );

ALTER TABLE shuffle_producer_commit ALTER COLUMN partition_totals DROP DEFAULT;
