package org.pangrova.shuffle.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.pangrova.protocol.v2.TaskAttemptId;

import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** Publishes one bounded write window under the allocation row lock. */
final class PostgresShuffleWindowPublication {
    private static final String PUBLISH = """
            WITH producer(shuffle_id, producer_partition_id, producer_attempt_id, producer_fencing_token) AS (
                VALUES (?::varchar, ?::integer, ?::integer, ?::bigint)
            ), incoming AS MATERIALIZED (
                SELECT p.*, items.* FROM producer p
                CROSS JOIN unnest(?::integer[], ?::bigint[], ?::text[], ?::bigint[], ?::bigint[], ?::bytea[])
                    WITH ORDINALITY AS items(output_partition_id, batch_sequence, object_key,
                                             batch_size_bytes, row_count, batch_checksum, ordinal)
            ), matched AS MATERIALIZED (
                SELECT i.*, b.batch_sequence IS NOT NULL AS existing,
                    (b.producer_fencing_token, b.object_key, b.batch_size_bytes, b.row_count, b.batch_checksum)
                        IS DISTINCT FROM
                    (i.producer_fencing_token, i.object_key, i.batch_size_bytes, i.row_count, i.batch_checksum)
                        AS different_content
                FROM incoming i LEFT JOIN shuffle_batch b USING
                    (shuffle_id, producer_partition_id, producer_attempt_id, output_partition_id, batch_sequence)
            ), missing AS MATERIALIZED (
                SELECT m.*, row_number() OVER (PARTITION BY output_partition_id ORDER BY ordinal) AS relative_sequence
                FROM matched m WHERE NOT existing
            ), frontiers AS MATERIALIZED (
                SELECT partitions.output_partition_id, COALESCE((
                    SELECT b.batch_sequence FROM shuffle_batch b
                    WHERE (b.shuffle_id, b.producer_partition_id, b.producer_attempt_id, b.output_partition_id)
                        = (p.shuffle_id, p.producer_partition_id, p.producer_attempt_id, partitions.output_partition_id)
                    ORDER BY b.batch_sequence DESC LIMIT 1
                ), -1) AS last_sequence
                FROM (SELECT DISTINCT output_partition_id FROM missing) partitions CROSS JOIN producer p
            ), verdict AS MATERIALIZED (
                SELECT CASE
                    WHEN EXISTS (SELECT 1 FROM matched WHERE existing AND different_content) THEN 'CONFLICT'
                    WHEN EXISTS (SELECT 1 FROM missing) AND EXISTS (
                        SELECT 1 FROM shuffle_producer_commit w
                        JOIN producer p USING (shuffle_id, producer_partition_id)
                        WHERE p.producer_attempt_id <= w.producer_attempt_id
                           OR p.producer_fencing_token <= w.producer_fencing_token
                    ) THEN 'FENCED'
                    WHEN EXISTS (
                        SELECT 1 FROM missing m JOIN frontiers f USING (output_partition_id)
                        WHERE m.batch_sequence <> f.last_sequence + m.relative_sequence
                    ) THEN 'GAP'
                    ELSE 'PUBLISH'
                END AS decision
            ), published AS (
                INSERT INTO shuffle_batch (shuffle_id, producer_partition_id, producer_attempt_id,
                    producer_fencing_token, output_partition_id, batch_sequence, object_key, batch_size_bytes,
                    row_count, batch_checksum)
                SELECT shuffle_id, producer_partition_id, producer_attempt_id,
                    producer_fencing_token, output_partition_id, batch_sequence, object_key, batch_size_bytes,
                    row_count, batch_checksum
                FROM missing CROSS JOIN verdict WHERE decision = 'PUBLISH'
                RETURNING 1
            )
            SELECT decision, (SELECT COUNT(*) FROM missing), (SELECT COUNT(*) FROM published)
            FROM verdict
            """;

    private PostgresShuffleWindowPublication() {}

    static void publish(Connection connection, String shuffleId, TaskAttemptId attempt,
            List<StoredBatch> batches) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(PUBLISH)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            statement.setLong(4, attempt.getFencingToken());
            bindBatches(statement, batches);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                requirePublished(result);
            }
        }
    }

    private static void bindBatches(PreparedStatement statement, List<StoredBatch> batches) throws SQLException {
        int count = batches.size();
        int[] partitions = new int[count];
        long[] sequences = new long[count];
        String[] keys = new String[count];
        long[] bytes = new long[count];
        long[] rows = new long[count];
        byte[][] checksums = new byte[count][];
        for (int index = 0; index < count; index++) {
            StoredBatch batch = batches.get(index);
            partitions[index] = batch.identity().outputPartitionId();
            sequences[index] = batch.identity().batchSequence();
            keys[index] = batch.objectKey();
            bytes[index] = batch.sizeBytes();
            rows[index] = batch.rowCount();
            checksums[index] = batch.checksum();
        }
        // The PostgreSQL JDBC driver encodes the primitive arrays and bytea[] directly.
        statement.setObject(5, partitions);
        statement.setObject(6, sequences);
        statement.setObject(7, keys);
        statement.setObject(8, bytes);
        statement.setObject(9, rows);
        statement.setObject(10, checksums);
    }

    private static void requirePublished(ResultSet result) throws SQLException {
        switch (result.getString(1)) {
            case "PUBLISH" -> {
                if (result.getLong(2) != result.getLong(3)) {
                    throw new IllegalStateException("shuffle write window did not publish all new batches");
                }
            }
            case "CONFLICT" -> throw new IllegalStateException("batch identity conflicts with existing content");
            case "FENCED" -> throw new SecurityException("producer attempt is fenced by committed winner");
            case "GAP" -> throw new IllegalStateException("partition batch sequence is not contiguous");
            default -> throw new IllegalStateException("unknown shuffle write window publication decision");
        }
    }
}
