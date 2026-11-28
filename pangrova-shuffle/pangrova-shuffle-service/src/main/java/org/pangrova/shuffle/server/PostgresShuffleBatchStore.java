package org.pangrova.shuffle.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.server.authority.ExecutionAnchorFence;

import static org.pangrova.shuffle.server.persistence.JdbcTransactions.transaction;
import static org.pangrova.shuffle.server.persistence.JdbcTransactions.read;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.findAllocation;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.requireOwner;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.selectAttemptPage;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.selectCommittedPage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionExpectation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** PostgreSQL-backed batch inventory and speculative-attempt winner election. */
final class PostgresShuffleBatchStore implements ShuffleBatchStore {
    private final DataSource dataSource;

    PostgresShuffleBatchStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public WriteGrant openWriteWindow(String shuffleId, TaskAttemptId attempt, long recoveryDelayMs) {
        if (recoveryDelayMs <= 0) {
            throw new IllegalArgumentException("write recovery delay must be positive");
        }
        return transaction(dataSource, connection -> {
            int partitions = lockWritableAllocation(connection, shuffleId, attempt);
            requireAttemptFence(connection, shuffleId, attempt);
            return reserveWindow(connection, shuffleId, attempt, partitions, recoveryDelayMs);
        });
    }

    private static WriteGrant reserveWindow(Connection connection, String shuffleId, TaskAttemptId attempt,
            int partitions, long recoveryDelayMs) throws SQLException {
        String sql = """
                WITH admission AS MATERIALIZED (
                    SELECT clock_timestamp() AS started, e.expires_at
                    FROM public.execution_anchor e WHERE e.execution_id = ?
                      AND NOT EXISTS (SELECT 1 FROM shuffle_cleanup_intent WHERE query_id = e.execution_id)
                ), permit AS MATERIALIZED (
                    SELECT started, LEAST(expires_at, started + ? * INTERVAL '1 millisecond') AS allowed_until
                    FROM admission
                ), reserved AS (
                    INSERT INTO shuffle_write_intent (reservation_id, shuffle_id, object_key, recovery_after)
                    SELECT ?::uuid, ?, ?, allowed_until + ? * INTERVAL '1 millisecond'
                    FROM permit WHERE allowed_until > started
                    RETURNING reservation_id
                )
                SELECT FLOOR(EXTRACT(EPOCH FROM (allowed_until - started)) * 1000)::bigint
                FROM permit CROSS JOIN reserved
                """;
        String reservation = UUID.randomUUID().toString();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, attempt.getQueryOrJobId());
            statement.setLong(2, ShuffleWriteSession.WINDOW_MILLIS);
            statement.setString(3, reservation);
            statement.setString(4, shuffleId);
            statement.setString(5, reservationKey(shuffleId, attempt));
            statement.setLong(6, recoveryDelayMs);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new SecurityException("shuffle write admission is expired or terminally fenced");
                }
                return new WriteGrant(reservation, partitions, row.getLong(1));
            }
        }
    }

    @Override
    public void publishWriteWindow(WriteGrant grant, String shuffleId, TaskAttemptId attempt,
            List<StoredBatch> batches) {
        if (batches.isEmpty() || batches.size() > ShuffleWriteSession.MAX_BATCHES) {
            throw new IllegalArgumentException("invalid shuffle write window size");
        }
        transaction(dataSource, connection -> {
            int partitions = lockWritableAllocation(connection, shuffleId, attempt);
            for (StoredBatch batch : batches) {
                requireWindowBatch(shuffleId, attempt, partitions, batch);
            }
            consumeWriteIntent(connection, grant, shuffleId, attempt);
            PostgresShuffleWindowPublication.publish(connection, shuffleId, attempt, batches);
            return null;
        });
    }

    @Override
    public void settleWriteWindow(WriteGrant grant, String shuffleId, TaskAttemptId attempt) {
        transaction(dataSource, connection -> {
            String sql = """
                    DELETE FROM shuffle_write_intent
                    WHERE reservation_id = ?::uuid AND shuffle_id = ? AND object_key = ?
                    """;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, grant.reservation());
                statement.setString(2, shuffleId);
                statement.setString(3, reservationKey(shuffleId, attempt));
                statement.executeUpdate();
            }
            return null;
        });
    }

    private static void consumeWriteIntent(
            Connection connection, WriteGrant grant, String shuffleId, TaskAttemptId attempt) throws SQLException {
        // A failed publication rolls back the exact reservation deletion as well.
        String sql = """
                DELETE FROM shuffle_write_intent i USING shuffle_attempt_fence f
                WHERE i.reservation_id = ?::uuid AND i.shuffle_id = ? AND i.object_key = ?
                  AND i.recovery_after > clock_timestamp()
                  AND f.shuffle_id = i.shuffle_id AND f.producer_partition_id = ?
                  AND f.producer_attempt_id = ? AND f.canonical_attempt = ?
                  AND NOT EXISTS (SELECT 1 FROM shuffle_cleanup_intent WHERE query_id = ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, grant.reservation());
            statement.setString(2, shuffleId);
            statement.setString(3, reservationKey(shuffleId, attempt));
            statement.setInt(4, attempt.getPartitionId());
            statement.setInt(5, attempt.getAttemptId());
            statement.setBytes(6, attempt.toByteArray());
            statement.setString(7, attempt.getQueryOrJobId());
            if (statement.executeUpdate() != 1) {
                throw new SecurityException("shuffle write reservation or attempt fence is invalid");
            }
        }
    }

    private static String reservationKey(String shuffleId, TaskAttemptId attempt) {
        return BatchIdentity.producerPrefix(shuffleId, attempt.getPartitionId(), attempt.getAttemptId());
    }

    private static void requireWindowBatch(String shuffleId, TaskAttemptId attempt, int partitions, StoredBatch batch) {
        BatchIdentity identity = batch.identity();
        if (!attempt.equals(batch.producerAttempt()) || !shuffleId.equals(identity.shuffleId())
                || identity.producerPartitionId() != attempt.getPartitionId()
                || identity.producerAttemptId() != attempt.getAttemptId()
                || identity.outputPartitionId() < 0 || identity.outputPartitionId() >= partitions
                || !identity.objectKey().equals(batch.objectKey())) {
            throw new SecurityException("producer attempt does not match shuffle batch");
        }
    }

    @Override
    public List<StoredBatch> readAttemptPage(
            String shuffleId,
            TaskAttemptId attempt,
            int outputPartitionId,
            long batchAfter,
            int limit) {
        return transaction(dataSource, connection -> {
            requireReadableAllocation(connection, shuffleId, attempt.getExecutionOwner());
            return selectAttemptPage(connection, shuffleId,
                    attempt, outputPartitionId, batchAfter, limit);
        });
    }

    @Override
    public CommitResult commitAttempt(CommitCommand command) {
        return transaction(dataSource, connection -> {
            lockWritableAllocation(connection, command.shuffleId(), command.attempt());
            Winner winner = findWinner(connection, command.shuffleId(),
                    command.attempt().getPartitionId());
            requireAttemptFence(connection, command.shuffleId(), command.attempt());
            PostgresCommitVerifier.verify(connection, command);
            if (winner != null && winner.matches(command.attempt())) {
                return commitResult(command);
            }
            requireWinningFence(winner, command.attempt());
            upsertWinner(connection, command);
            incrementVersion(connection, command.shuffleId());
            return commitResult(command);
        });
    }

    @Override
    public List<StoredBatch> readCommittedPage(Allocation selected, BatchIdentity after, int limit) {
        if (limit <= 0 || (after != null && !after.shuffleId().equals(selected.identity().shuffleId()))) {
            throw new IllegalArgumentException("invalid committed batch page");
        }
        return read(dataSource, connection -> selectCommittedPage(connection, selected, after, limit));
    }

    private static int lockWritableAllocation(Connection connection, String shuffleId, TaskAttemptId attempt)
            throws SQLException {
        WorkerExecutionIdentity.from(attempt.getWorkerExecutionIdentity());
        if (attempt.getPartitionId() < 0 || attempt.getAttemptId() < 0
                || attempt.getFencingToken() <= 0 || attempt.getControlEpoch() <= 0
                || attempt.getTaskId().isBlank()) {
            throw new SecurityException("producer attempt has an incomplete fence");
        }
        ExecutionAnchorFence.requireLiveOwner(connection,
                attempt.getQueryOrJobId(), attempt.getExecutionOwner(), true);
        return PostgresShuffleRecords.lockWritableAllocation(connection, shuffleId, attempt);
    }

    private static Allocation requireReadableAllocation(
            Connection connection, String shuffleId, ExecutionOwnerIdentity expectedOwner)
            throws SQLException {
        Allocation allocation = findAllocation(connection, shuffleId, false)
                .orElseThrow(() -> new IllegalArgumentException("shuffle does not exist"));
        requireOwner(allocation, expectedOwner);
        ExecutionAnchorFence.requireLiveOwner(connection,
                allocation.identity().queryId(), expectedOwner, true);
        return allocation;
    }

    private static void requireAttemptFence(Connection connection, String shuffleId, TaskAttemptId attempt)
            throws SQLException {
        // The caller holds the allocation row. The existing fence is immutable.
        String sql = """
                WITH requested(shuffle_id, producer_partition_id, producer_attempt_id,
                               producer_fencing_token, task_id, control_epoch, canonical_attempt) AS (
                    VALUES (?::varchar, ?::integer, ?::integer, ?::bigint, ?::varchar, ?::bigint, ?::bytea)
                ), existing AS MATERIALIZED (
                    SELECT f.producer_fencing_token, f.task_id, f.control_epoch, f.canonical_attempt
                    FROM shuffle_attempt_fence f JOIN requested r
                        USING (shuffle_id, producer_partition_id, producer_attempt_id)
                ), created AS (
                    INSERT INTO shuffle_attempt_fence (
                        shuffle_id, producer_partition_id, producer_attempt_id,
                        producer_fencing_token, task_id, control_epoch, canonical_attempt)
                    SELECT * FROM requested WHERE NOT EXISTS (SELECT 1 FROM existing)
                    RETURNING 1
                )
                SELECT EXISTS (SELECT 1 FROM created)
                    OR EXISTS (SELECT 1 FROM existing e CROSS JOIN requested r
                               WHERE e.canonical_attempt = r.canonical_attempt
                                 AND e.producer_fencing_token = r.producer_fencing_token
                                 AND e.task_id = r.task_id AND e.control_epoch = r.control_epoch)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            statement.setLong(4, attempt.getFencingToken());
            statement.setString(5, attempt.getTaskId());
            statement.setLong(6, attempt.getControlEpoch());
            statement.setBytes(7, attempt.toByteArray());
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                if (!rows.getBoolean(1)) {
                    throw new SecurityException("producer attempt identity conflicts with fence");
                }
            }
        }
    }

    private static Winner findWinner(Connection connection, String shuffleId, int partition)
            throws SQLException {
        String sql = "SELECT producer_attempt_id, producer_fencing_token, task_id, control_epoch, canonical_attempt "
                + "FROM shuffle_producer_commit WHERE shuffle_id = ? "
                + "AND producer_partition_id = ? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, partition);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next()
                        ? new Winner(rows.getInt(1), rows.getLong(2),
                                rows.getString(3), rows.getLong(4), rows.getBytes(5)) : null;
            }
        }
    }

    private static void requireWinningFence(Winner winner, TaskAttemptId attempt) {
        if (winner != null && (attempt.getAttemptId() <= winner.attemptId()
                || attempt.getFencingToken() <= winner.fencingToken())) {
            throw new SecurityException("producer attempt is fenced by committed winner");
        }
    }

    private static void upsertWinner(Connection connection, CommitCommand command)
            throws SQLException {
        String sql = "INSERT INTO shuffle_producer_commit "
                + "(shuffle_id, producer_partition_id, producer_attempt_id, "
                + "producer_fencing_token, task_id, control_epoch, canonical_attempt, partition_totals) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (shuffle_id, producer_partition_id) DO UPDATE SET "
                + "producer_attempt_id = EXCLUDED.producer_attempt_id, "
                + "producer_fencing_token = EXCLUDED.producer_fencing_token, "
                + "task_id = EXCLUDED.task_id, control_epoch = EXCLUDED.control_epoch, "
                + "canonical_attempt = EXCLUDED.canonical_attempt, partition_totals = EXCLUDED.partition_totals, "
                + "committed_at = CURRENT_TIMESTAMP";
        Long[] totals = new Long[Math.multiplyExact(command.partitions().size(), 4)];
        int offset = 0;
        for (PartitionExpectation partition : command.partitions()) {
            totals[offset++] = (long) partition.partitionId();
            totals[offset++] = partition.totalBatches();
            totals[offset++] = partition.totalBytes();
            totals[offset++] = partition.totalRows();
        }
        var encodedTotals = connection.createArrayOf("int8", totals);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            TaskAttemptId attempt = command.attempt();
            statement.setString(1, command.shuffleId());
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            statement.setLong(4, attempt.getFencingToken());
            statement.setString(5, attempt.getTaskId());
            statement.setLong(6, attempt.getControlEpoch());
            statement.setBytes(7, attempt.toByteArray());
            statement.setArray(8, encodedTotals);
            statement.executeUpdate();
        } finally {
            encodedTotals.free();
        }
    }

    private static CommitResult commitResult(CommitCommand command) {
        long bytes = command.partitions().stream()
                .mapToLong(PartitionExpectation::totalBytes).sum();
        return new CommitResult(command.partitions().size(), bytes,
                command.attempt().getFencingToken());
    }

    private static void incrementVersion(Connection connection, String shuffleId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE shuffle_allocation SET metadata_version = metadata_version + 1 "
                        + "WHERE shuffle_id = ?")) {
            statement.setString(1, shuffleId);
            statement.executeUpdate();
        }
    }

    private record Winner(int attemptId, long fencingToken, String taskId, long controlEpoch, byte[] canonicalAttempt) {
        private boolean matches(TaskAttemptId attempt) {
            return attemptId == attempt.getAttemptId()
                    && fencingToken == attempt.getFencingToken()
                    && taskId.equals(attempt.getTaskId())
                    && controlEpoch == attempt.getControlEpoch()
                    && java.util.Arrays.equals(canonicalAttempt, attempt.toByteArray());
        }
    }
}
