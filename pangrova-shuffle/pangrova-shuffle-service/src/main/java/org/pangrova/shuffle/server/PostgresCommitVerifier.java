package org.pangrova.shuffle.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionExpectation;

/** Verifies producer commit summaries with bounded PostgreSQL aggregate results. */
final class PostgresCommitVerifier {

    private PostgresCommitVerifier() {}

    static void verify(Connection connection, CommitCommand command) throws SQLException {
        Map<Integer, PartitionTotals> actual = readTotals(connection, command);
        if (actual.size() != command.partitions().size()) {
            throw new IllegalStateException("commit summary lacks exact durable partition coverage");
        }
        Set<Integer> seen = new HashSet<>();
        for (PartitionExpectation expected : command.partitions()) {
            if (!seen.add(expected.partitionId())) {
                throw new IllegalStateException("commit summary repeats a partition");
            }
            verify(expected, actual.get(expected.partitionId()));
        }
    }

    private static Map<Integer, PartitionTotals> readTotals(
            Connection connection,
            CommitCommand command) throws SQLException {
        String sql = "SELECT output_partition_id, COUNT(*), SUM(batch_size_bytes), "
                + "SUM(row_count), MAX(batch_sequence) FROM shuffle_batch "
                + "WHERE shuffle_id = ? AND producer_partition_id = ? "
                + "AND producer_attempt_id = ? GROUP BY output_partition_id";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, command.shuffleId());
            statement.setInt(2, command.attempt().getPartitionId());
            statement.setInt(3, command.attempt().getAttemptId());
            return readTotals(statement);
        }
    }

    private static Map<Integer, PartitionTotals> readTotals(PreparedStatement statement)
            throws SQLException {
        Map<Integer, PartitionTotals> totals = new HashMap<>();
        try (ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                totals.put(rows.getInt(1), new PartitionTotals(
                        rows.getLong(2), rows.getLong(3), rows.getLong(4), rows.getLong(5)));
            }
        }
        return totals;
    }

    private static void verify(PartitionExpectation expected, PartitionTotals actual) {
        if (actual == null || expected.totalBytes() != actual.bytes()
                || expected.totalRows() != actual.rows()
                || expected.totalBatches() != actual.batches()
                || expected.expectedLastSequence() != actual.lastSequence()) {
            throw new IllegalStateException("commit summary does not match durable batches");
        }
    }

    private record PartitionTotals(long batches, long bytes, long rows, long lastSequence) {}
}
