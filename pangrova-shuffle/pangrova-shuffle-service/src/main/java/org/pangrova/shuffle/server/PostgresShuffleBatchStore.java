package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.server.authority.ExecutionAnchorFence;

import static org.pangrova.shuffle.server.persistence.JdbcTransactions.transaction;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.findAllocation;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.requireOwner;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.requireRegistered;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.BATCH_SELECT;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.readBatch;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.selectAttemptPage;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.selectCommittedPage;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.selectFinalizedPage;
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
    public String beginWriteIntent(StoredBatch batch, long recoveryDelayMs) {
        if (recoveryDelayMs <= 0) {
            throw new IllegalArgumentException("write recovery delay must be positive");
        }
        return transaction(dataSource, connection -> {
            Allocation allocation = lockWritableAllocation(connection,
                    batch.identity().shuffleId(), batch.producerAttempt().getExecutionOwner());
            requireAttempt(allocation, batch.producerAttempt(), batch.identity());
            requireAttemptFence(connection, batch);
            String reservationId = UUID.randomUUID().toString();
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO shuffle_write_intent "
                            + "(reservation_id, shuffle_id, object_key, state, recovery_after) "
                            + "VALUES (?::uuid, ?, ?, 'IN_PROGRESS', "
                            + "clock_timestamp() + (? * INTERVAL '1 millisecond'))")) {
                statement.setString(1, reservationId);
                statement.setString(2, batch.identity().shuffleId());
                statement.setString(3, batch.objectKey());
                statement.setLong(4, recoveryDelayMs);
                statement.executeUpdate();
            }
            return reservationId;
        });
    }

    @Override
    public void completeWriteIntent(String reservationId, StoredBatch batch) {
        transaction(dataSource, connection -> {
            Allocation allocation = lockWritableAllocation(connection,
                    batch.identity().shuffleId(), batch.producerAttempt().getExecutionOwner());
            requireWriteIntent(connection, reservationId, batch);
            requireAttempt(allocation, batch.producerAttempt(), batch.identity());
            requireAttemptFence(connection, batch);
            StoredBatch existing = findBatch(connection, batch.identity());
            if (existing != null) {
                requireSameBatch(existing, batch);
                deleteWriteIntent(connection, reservationId);
                return null;
            }
            requireWritableAttempt(connection, batch);
            requireNextSequence(connection, batch.identity());
            insertBatch(connection, batch);
            incrementVersion(connection, batch.identity().shuffleId());
            deleteWriteIntent(connection, reservationId);
            return null;
        });
    }

    @Override
    public void abortWriteIntent(String reservationId) {
        transaction(dataSource, connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE shuffle_write_intent SET state = 'ABORTED' "
                            + "WHERE reservation_id = ?::uuid AND state = 'IN_PROGRESS'")) {
                statement.setString(1, reservationId);
                statement.executeUpdate();
            }
            return null;
        });
    }

    private static void requireWriteIntent(
            Connection connection, String reservationId, StoredBatch batch) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT shuffle_id, object_key, state FROM shuffle_write_intent "
                        + "WHERE reservation_id = ?::uuid FOR UPDATE")) {
            statement.setString(1, reservationId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next() || !row.getString(1).equals(batch.identity().shuffleId())
                        || !row.getString(2).equals(batch.objectKey())
                        || !"IN_PROGRESS".equals(row.getString(3))) {
                    throw new SecurityException("shuffle write reservation is invalid");
                }
            }
        }
    }

    private static void deleteWriteIntent(Connection connection, String reservationId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM shuffle_write_intent WHERE reservation_id = ?::uuid")) {
            statement.setString(1, reservationId);
            statement.executeUpdate();
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
            Allocation allocation = lockWritableAllocation(connection,
                    command.shuffleId(), command.attempt().getExecutionOwner());
            requireAttempt(allocation, command.attempt(), null);
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
    public long committedMetadataVersion(String shuffleId) {
        return transaction(dataSource, connection ->
                requireReadableAllocation(connection, shuffleId, null).metadataVersion());
    }

    @Override
    public List<StoredBatch> readCommittedPage(
            String shuffleId,
            int partitionId,
            int producerAfter,
            long batchAfter,
            int limit) {
        return transaction(dataSource, connection -> {
            requireReadableAllocation(connection, shuffleId, null);
            if (producerAfter < -1 || batchAfter < -1 || limit <= 0) {
                throw new IllegalArgumentException("invalid committed batch page");
            }
            return selectCommittedPage(connection, shuffleId, partitionId,
                    producerAfter, batchAfter, limit);
        });
    }

    @Override
    public List<StoredBatch> readFinalizedPage(
            String shuffleId,
            int partitionId,
            long startSequence,
            int limit) {
        return transaction(dataSource, connection -> {
            Allocation allocation = requireReadableAllocation(connection, shuffleId, null);
            if (allocation.state() != ShuffleAllocationState.FINALIZED) {
                throw new IllegalStateException("shuffle is not finalized");
            }
            if (startSequence < 0 || limit <= 0) {
                throw new IllegalArgumentException("invalid finalized batch page");
            }
            return selectFinalizedPage(
                    connection, shuffleId, partitionId, startSequence, limit);
        });
    }

    private static Allocation lockWritableAllocation(
            Connection connection,
            String shuffleId,
            ExecutionOwnerIdentity owner)
            throws SQLException {
        String queryId = ExecutionAnchorFence.queryIdForShuffle(connection, shuffleId);
        ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_id, owner_incarnation, owner_fencing_token, state "
                        + "FROM shuffle_query_scope WHERE query_id = ? FOR UPDATE")) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next() || !"ACTIVE".equals(row.getString(4))
                        || !owner.getOwnerId().equals(row.getString(1))
                        || !owner.getOwnerIncarnation().equals(row.getString(2))
                        || owner.getFencingToken() != row.getLong(3)) {
                    throw new SecurityException("shuffle query scope is not writable");
                }
            }
        }
        Allocation allocation = findAllocation(connection, shuffleId, true)
                .orElseThrow(() -> new IllegalArgumentException("shuffle does not exist"));
        requireRegistered(allocation);
        return allocation;
    }

    private static Allocation requireReadableAllocation(
            Connection connection, String shuffleId, ExecutionOwnerIdentity expectedOwner)
            throws SQLException {
        Allocation allocation = findAllocation(connection, shuffleId, false)
                .orElseThrow(() -> new IllegalArgumentException("shuffle does not exist"));
        ExecutionOwnerIdentity owner = expectedOwner == null
                ? allocation.owner() : expectedOwner;
        requireOwner(allocation, owner);
        ExecutionAnchorFence.requireLiveOwner(connection,
                allocation.identity().queryId(), owner, true);
        return allocation;
    }

    private static void requireAttempt(
            Allocation allocation,
            TaskAttemptId attempt,
            BatchIdentity identity) {
        org.pangrova.protocol.execution.identity.WorkerExecutionIdentity.from(
                attempt.getWorkerExecutionIdentity());
        requireOwner(allocation, attempt.getExecutionOwner());
        if (!matchesAllocation(allocation, attempt)
                || (identity != null && !matchesBatchIdentity(allocation, attempt, identity))) {
            throw new SecurityException("producer attempt does not match shuffle allocation");
        }
    }

    private static boolean matchesAllocation(Allocation allocation, TaskAttemptId attempt) {
        boolean identity = attempt.getQueryOrJobId().equals(allocation.identity().queryId())
                && attempt.getStageId() == allocation.identity().stageId()
                && attempt.getPlanEpoch() == allocation.identity().planEpoch();
        boolean fence = attempt.getFencingToken() > 0 && attempt.getControlEpoch() > 0
                && !attempt.getTaskId().isBlank();
        boolean partition = attempt.getPartitionId() >= 0
                && attempt.getPartitionId() < allocation.shape().expectedProducerCount();
        return identity && fence && partition;
    }

    private static boolean matchesBatchIdentity(
            Allocation allocation,
            TaskAttemptId attempt,
            BatchIdentity identity) {
        return identity.producerPartitionId() == attempt.getPartitionId()
                && identity.producerAttemptId() == attempt.getAttemptId()
                && identity.outputPartitionId() >= 0
                && identity.outputPartitionId() < allocation.shape().partitionCount();
    }

    private static void requireAttemptFence(Connection connection, StoredBatch batch)
            throws SQLException {
        requireAttemptFence(connection, batch.identity().shuffleId(), batch.producerAttempt());
    }

    private static void requireAttemptFence(Connection connection, String shuffleId, TaskAttemptId attempt)
            throws SQLException {
        String insert = "INSERT INTO shuffle_attempt_fence "
                + "(shuffle_id, producer_partition_id, producer_attempt_id, "
                + "producer_fencing_token, task_id, control_epoch, canonical_attempt) VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT DO NOTHING";
        try (PreparedStatement statement = connection.prepareStatement(insert)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            statement.setLong(4, attempt.getFencingToken());
            statement.setString(5, attempt.getTaskId());
            statement.setLong(6, attempt.getControlEpoch());
            statement.setBytes(7, attempt.toByteArray());
            statement.executeUpdate();
        }
        String select = "SELECT producer_fencing_token, task_id, control_epoch, canonical_attempt "
                + "FROM shuffle_attempt_fence WHERE shuffle_id = ? "
                + "AND producer_partition_id = ? AND producer_attempt_id = ? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(select)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                if (rows.getLong(1) != attempt.getFencingToken()
                        || !rows.getString(2).equals(attempt.getTaskId())
                        || rows.getLong(3) != attempt.getControlEpoch()
                        || !java.util.Arrays.equals(rows.getBytes(4), attempt.toByteArray())) {
                    throw new SecurityException("producer attempt identity conflicts with fence");
                }
            }
        }
    }

    private static StoredBatch findBatch(Connection connection, BatchIdentity identity)
            throws SQLException {
        String sql = BATCH_SELECT + " WHERE b.shuffle_id = ? AND b.producer_partition_id = ? "
                + "AND b.producer_attempt_id = ? AND b.output_partition_id = ? "
                + "AND b.batch_sequence = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            setBatchKey(statement, identity);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? readBatch(rows) : null;
            }
        }
    }

    private static void requireSameBatch(StoredBatch existing, StoredBatch candidate) {
        if (!existing.sameContent(candidate)) {
            throw new IllegalStateException("batch identity conflicts with existing content");
        }
    }

    private static void requireWritableAttempt(Connection connection, StoredBatch batch)
            throws SQLException {
        Winner winner = findWinner(connection, batch.identity().shuffleId(),
                batch.identity().producerPartitionId());
        if (winner != null && (batch.identity().producerAttemptId() <= winner.attemptId()
                || batch.producerAttempt().getFencingToken() <= winner.fencingToken())) {
            throw new SecurityException("producer attempt is fenced by committed winner");
        }
    }

    private static void requireNextSequence(Connection connection, BatchIdentity identity)
            throws SQLException {
        String sql = "SELECT COALESCE(MAX(batch_sequence), -1) FROM shuffle_batch "
                + "WHERE shuffle_id = ? AND producer_partition_id = ? "
                + "AND producer_attempt_id = ? AND output_partition_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identity.shuffleId());
            statement.setInt(2, identity.producerPartitionId());
            statement.setInt(3, identity.producerAttemptId());
            statement.setInt(4, identity.outputPartitionId());
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                if (identity.batchSequence() != rows.getLong(1) + 1) {
                    throw new IllegalStateException("partition batch sequence is not contiguous");
                }
            }
        }
    }

    private static void insertBatch(Connection connection, StoredBatch batch) throws SQLException {
        String sql = "INSERT INTO shuffle_batch (shuffle_id, producer_partition_id, "
                + "producer_attempt_id, producer_fencing_token, output_partition_id, "
                + "batch_sequence, object_key, batch_size_bytes, row_count, batch_checksum) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            BatchIdentity identity = batch.identity();
            statement.setString(1, identity.shuffleId());
            statement.setInt(2, identity.producerPartitionId());
            statement.setInt(3, identity.producerAttemptId());
            statement.setLong(4, batch.producerAttempt().getFencingToken());
            statement.setInt(5, identity.outputPartitionId());
            statement.setLong(6, identity.batchSequence());
            statement.setString(7, batch.objectKey());
            statement.setLong(8, batch.sizeBytes());
            statement.setLong(9, batch.rowCount());
            statement.setBytes(10, batch.checksum());
            statement.executeUpdate();
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
                + "producer_fencing_token, task_id, control_epoch, canonical_attempt) VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (shuffle_id, producer_partition_id) DO UPDATE SET "
                + "producer_attempt_id = EXCLUDED.producer_attempt_id, "
                + "producer_fencing_token = EXCLUDED.producer_fencing_token, "
                + "task_id = EXCLUDED.task_id, control_epoch = EXCLUDED.control_epoch, "
                + "canonical_attempt = EXCLUDED.canonical_attempt, committed_at = CURRENT_TIMESTAMP";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            TaskAttemptId attempt = command.attempt();
            statement.setString(1, command.shuffleId());
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            statement.setLong(4, attempt.getFencingToken());
            statement.setString(5, attempt.getTaskId());
            statement.setLong(6, attempt.getControlEpoch());
            statement.setBytes(7, attempt.toByteArray());
            statement.executeUpdate();
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

    private static void setAttemptKey(
            PreparedStatement statement,
            int offset,
            BatchIdentity identity) throws SQLException {
        statement.setString(offset, identity.shuffleId());
        statement.setInt(offset + 1, identity.producerPartitionId());
        statement.setInt(offset + 2, identity.producerAttemptId());
    }

    private static void setBatchKey(PreparedStatement statement, BatchIdentity identity)
            throws SQLException {
        setAttemptKey(statement, 1, identity);
        statement.setInt(4, identity.outputPartitionId());
        statement.setLong(5, identity.batchSequence());
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
