package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;

import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationShape;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupShufflePage;

/** Shared PostgreSQL row decoding and allocation locking operations. */
final class PostgresShuffleRecords {

    private static final String ALLOCATION_COLUMNS = "a.shuffle_id AS shuffle_id, "
            + "a.query_id AS query_id, a.stage_id AS stage_id, a.plan_epoch AS plan_epoch, "
            + "a.partition_count AS partition_count, "
            + "a.expected_producer_count AS expected_producer_count, "
            + "a.producer_set_checksum AS producer_set_checksum, "
            + "a.capability_expires_at_ms AS capability_expires_at_ms, "
            + "q.cleanup_expires_at_ms AS cleanup_expires_at_ms, "
            + "a.metadata_version AS metadata_version, a.state AS state, "
            + "q.owner_id AS owner_id, q.owner_incarnation AS owner_incarnation, "
            + "q.owner_fencing_token AS owner_fencing_token";
    static final String BATCH_SELECT = "SELECT b.*, f.task_id, f.control_epoch, f.canonical_attempt, "
            + "a.query_id, a.stage_id, "
            + "a.plan_epoch, q.owner_id, q.owner_incarnation, q.owner_fencing_token "
            + "FROM shuffle_batch b JOIN shuffle_attempt_fence f USING "
            + "(shuffle_id, producer_partition_id, producer_attempt_id) "
            + "JOIN shuffle_allocation a USING (shuffle_id) "
            + "JOIN shuffle_query_scope q USING (query_id)";
    private static final String FIND_ALLOCATION_SQL = "SELECT " + ALLOCATION_COLUMNS
            + " FROM shuffle_allocation a JOIN shuffle_query_scope q USING (query_id)"
            + " WHERE shuffle_id = ?";
    private static final String FIND_ALLOCATION_FOR_UPDATE_SQL = FIND_ALLOCATION_SQL
            + " FOR UPDATE OF a";
    private static final String CLEANUP_OBJECTS_BASE_SQL =
            "SELECT b.object_key, b.shuffle_id, b.producer_partition_id, "
                    + "b.producer_attempt_id, b.output_partition_id, b.batch_sequence "
                    + "FROM shuffle_batch b JOIN shuffle_allocation a USING (shuffle_id) "
                    + "WHERE a.query_id = ? ";
    private static final String CLEANUP_OBJECTS_ORDER_SQL =
            "ORDER BY b.shuffle_id, b.producer_partition_id, b.producer_attempt_id, "
                    + "b.output_partition_id, b.batch_sequence LIMIT ?";
    private static final String CLEANUP_OBJECTS_FIRST_SQL =
            CLEANUP_OBJECTS_BASE_SQL + CLEANUP_OBJECTS_ORDER_SQL;
    private static final String CLEANUP_OBJECTS_AFTER_SQL = CLEANUP_OBJECTS_BASE_SQL
            + "AND (b.shuffle_id, b.producer_partition_id, b.producer_attempt_id, "
            + "b.output_partition_id, b.batch_sequence) > (?, ?, ?, ?, ?) "
            + CLEANUP_OBJECTS_ORDER_SQL;

    private PostgresShuffleRecords() {}

    static Optional<Allocation> findAllocation(
            Connection connection,
            String shuffleId,
            boolean forUpdate) throws SQLException {
        String sql = forUpdate ? FIND_ALLOCATION_FOR_UPDATE_SQL : FIND_ALLOCATION_SQL;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(readAllocation(rows)) : Optional.empty();
            }
        }
    }

    static Optional<Allocation> findAllocationByGeneration(
            Connection connection,
            String queryId,
            int stageId,
            long planEpoch) throws SQLException {
        String sql = "SELECT " + ALLOCATION_COLUMNS
                + " FROM shuffle_allocation a JOIN shuffle_query_scope q USING (query_id)"
                + " WHERE query_id = ? AND stage_id = ? AND plan_epoch = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, queryId);
            statement.setInt(2, stageId);
            statement.setLong(3, planEpoch);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(readAllocation(rows)) : Optional.empty();
            }
        }
    }

    static void requireOwner(
            Allocation allocation,
            ExecutionOwnerIdentity owner) {
        if (!allocation.owner().equals(owner)) {
            throw new SecurityException("shuffle execution owner fence does not match");
        }
    }

    static void requireRegistered(Allocation allocation) {
        if (allocation.state() != ShuffleAllocationState.REGISTERED) {
            throw new IllegalStateException("shuffle allocation is not writable");
        }
    }

    static void refreshQueryScopeExpiry(
            Connection connection,
            String queryId,
            long expiresAtMs) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE shuffle_query_scope SET cleanup_expires_at_ms = "
                        + "GREATEST(cleanup_expires_at_ms, ?) WHERE query_id = ?")) {
            statement.setLong(1, expiresAtMs);
            statement.setString(2, queryId);
            statement.executeUpdate();
        }
    }

    static void refreshAllocationExpiry(
            Connection connection,
            String shuffleId,
            long expiresAtMs) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE shuffle_allocation SET capability_expires_at_ms = "
                        + "GREATEST(capability_expires_at_ms, ?) WHERE shuffle_id = ?")) {
            statement.setLong(1, expiresAtMs);
            statement.setString(2, shuffleId);
            statement.executeUpdate();
        }
    }

    static void requireCleanupPageLimit(int limit) {
        if (limit <= 0 || limit > 1_000) {
            throw new IllegalArgumentException("cleanup page limit must be between 1 and 1000");
        }
    }

    static String findExpiredCleanup(Connection connection, long nowMs) throws SQLException {
        String sql = "SELECT query_id FROM shuffle_query_scope "
                + "WHERE state = 'CLEANING' "
                + "AND (cleanup_lease_expires_at_ms IS NULL "
                + "OR cleanup_lease_expires_at_ms <= ?) "
                + "ORDER BY cleanup_lease_expires_at_ms NULLS FIRST, query_id "
                + "FOR UPDATE SKIP LOCKED LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, nowMs);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    static void claimCleanup(
            Connection connection,
            String queryId,
            String state,
            Long currentLeaseExpiry,
            CleanupLease lease,
            boolean expiredRecovery) throws SQLException {
        boolean active = "ACTIVE".equals(state) && !expiredRecovery;
        boolean expired = "CLEANING".equals(state)
                && (currentLeaseExpiry == null || currentLeaseExpiry <= lease.nowMs());
        if (!active && !expired) {
            throw new IllegalStateException("shuffle cleanup already has an active lease");
        }
        try (PreparedStatement query = connection.prepareStatement(
                "UPDATE shuffle_query_scope SET state = 'CLEANING', "
                        + "cleanup_lease_owner = ?, cleanup_lease_expires_at_ms = ? "
                        + "WHERE query_id = ?")) {
            query.setString(1, lease.owner());
            query.setLong(2, lease.expiresAtMs());
            query.setString(3, queryId);
            query.executeUpdate();
        }
        try (PreparedStatement allocations = connection.prepareStatement(
                "UPDATE shuffle_allocation SET state = 'CLEANING' WHERE query_id = ?")) {
            allocations.setString(1, queryId);
            allocations.executeUpdate();
        }
    }

    static void assignOutputSequences(Connection connection, String shuffleId)
            throws SQLException {
        String sql = "WITH ordered AS (SELECT b.shuffle_id, b.producer_partition_id, "
                + "b.producer_attempt_id, b.output_partition_id, b.batch_sequence, "
                + "ROW_NUMBER() OVER (PARTITION BY b.output_partition_id ORDER BY "
                + "b.producer_partition_id, b.batch_sequence) - 1 AS output_sequence "
                + "FROM shuffle_batch b JOIN shuffle_producer_commit c "
                + "ON c.shuffle_id = b.shuffle_id "
                + "AND c.producer_partition_id = b.producer_partition_id "
                + "AND c.producer_attempt_id = b.producer_attempt_id "
                + "WHERE b.shuffle_id = ?) UPDATE shuffle_batch b SET output_sequence = "
                + "ordered.output_sequence FROM ordered WHERE b.shuffle_id = ordered.shuffle_id "
                + "AND b.producer_partition_id = ordered.producer_partition_id "
                + "AND b.producer_attempt_id = ordered.producer_attempt_id "
                + "AND b.output_partition_id = ordered.output_partition_id "
                + "AND b.batch_sequence = ordered.batch_sequence";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            statement.executeUpdate();
        }
    }

    static CleanupPlan cleanupPlan(
            Connection connection,
            String queryId,
            String leaseOwner) throws SQLException {
        String sql = "SELECT COUNT(DISTINCT (b.shuffle_id, b.output_partition_id)), "
                + "COALESCE(SUM(b.batch_size_bytes), 0) FROM shuffle_allocation a "
                + "LEFT JOIN shuffle_batch b USING (shuffle_id) WHERE a.query_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return new CleanupPlan(queryId, leaseOwner, rows.getLong(1), rows.getLong(2));
            }
        }
    }

    static CleanupObjectPage cleanupObjects(
            Connection connection,
            CleanupPlan plan,
            CleanupCursor after,
            int limit) throws SQLException {
        String sql = after == null ? CLEANUP_OBJECTS_FIRST_SQL : CLEANUP_OBJECTS_AFTER_SQL;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, plan.queryId());
            if (after != null) {
                statement.setString(parameter++, after.shuffleId());
                statement.setInt(parameter++, after.producerPartitionId());
                statement.setInt(parameter++, after.producerAttemptId());
                statement.setInt(parameter++, after.outputPartitionId());
                statement.setLong(parameter++, after.batchSequence());
            }
            statement.setInt(parameter, limit);
            return readCleanupObjects(statement);
        }
    }

    private static CleanupObjectPage readCleanupObjects(PreparedStatement statement)
            throws SQLException {
        List<String> keys = new ArrayList<>();
        CleanupCursor cursor = null;
        try (ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                keys.add(rows.getString(1));
                cursor = new CleanupCursor(rows.getString(2), rows.getInt(3), rows.getInt(4),
                        rows.getInt(5), rows.getLong(6));
            }
        }
        return new CleanupObjectPage(keys, cursor);
    }

    static CleanupShufflePage cleanupShuffles(
            Connection connection,
            CleanupPlan plan,
            String after,
            int limit) throws SQLException {
        String sql = "SELECT shuffle_id FROM shuffle_allocation WHERE query_id = ? "
                + "AND shuffle_id > ? ORDER BY shuffle_id LIMIT ?";
        List<String> shuffleIds = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, plan.queryId());
            statement.setString(2, after == null ? "" : after);
            statement.setInt(3, limit);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    shuffleIds.add(rows.getString(1));
                }
            }
        }
        String cursor = shuffleIds.isEmpty() ? null : shuffleIds.get(shuffleIds.size() - 1);
        return new CleanupShufflePage(shuffleIds, cursor);
    }

    static void renewCleanup(
            Connection connection,
            CleanupPlan plan,
            CleanupLease lease) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE shuffle_query_scope SET cleanup_lease_expires_at_ms = ? "
                        + "WHERE query_id = ? AND state = 'CLEANING' "
                        + "AND cleanup_lease_owner = ?")) {
            statement.setLong(1, lease.expiresAtMs());
            statement.setString(2, plan.queryId());
            statement.setString(3, plan.leaseOwner());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("shuffle cleanup lease has been lost");
            }
        }
    }

    static void completeCleanup(Connection connection, CleanupPlan plan)
            throws SQLException {
        renewCleanup(connection, plan,
                new CleanupLease(plan.leaseOwner(), 1, Long.MAX_VALUE));
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM shuffle_allocation WHERE query_id = ?")) {
            statement.setString(1, plan.queryId());
            statement.executeUpdate();
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM shuffle_query_scope WHERE query_id = ? AND state = 'CLEANING' "
                        + "AND cleanup_lease_owner = ?")) {
            statement.setString(1, plan.queryId());
            statement.setString(2, plan.leaseOwner());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("shuffle cleanup lease has been lost");
            }
        }
    }

    static List<StoredBatch> selectAttemptPage(
            Connection connection,
            String shuffleId,
            TaskAttemptId attempt,
            int outputPartitionId,
            long batchAfter,
            int limit) throws SQLException {
        String sql = BATCH_SELECT + " WHERE b.shuffle_id = ? AND b.producer_partition_id = ? "
                + "AND b.producer_attempt_id = ? AND b.output_partition_id = ? "
                + "AND b.batch_sequence > ? ORDER BY b.batch_sequence LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, attempt.getPartitionId());
            statement.setInt(3, attempt.getAttemptId());
            statement.setInt(4, outputPartitionId);
            statement.setLong(5, batchAfter);
            statement.setInt(6, limit);
            statement.setFetchSize(1_000);
            var batches = readBatches(statement, attempt.getExecutionOwner(), shuffleId);
            if (batches.stream().anyMatch(batch -> !batch.producerAttempt().equals(attempt))) {
                throw new SecurityException("Requested shuffle attempt conflicts with durable identity");
            }
            return batches;
        }
    }

    static List<StoredBatch> selectCommittedPage(
            Connection connection,
            String shuffleId,
            int outputPartitionId,
            int producerAfter,
            long batchAfter,
            int limit) throws SQLException {
        String sql = BATCH_SELECT + " JOIN shuffle_producer_commit c "
                + "ON c.shuffle_id = b.shuffle_id "
                + "AND c.producer_partition_id = b.producer_partition_id "
                + "AND c.producer_attempt_id = b.producer_attempt_id "
                + "WHERE b.shuffle_id = ? AND b.output_partition_id = ? "
                + "AND (b.producer_partition_id, b.batch_sequence) > (?, ?) "
                + "ORDER BY b.producer_partition_id, b.batch_sequence LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, outputPartitionId);
            statement.setInt(3, producerAfter);
            statement.setLong(4, batchAfter);
            statement.setInt(5, limit);
            statement.setFetchSize(1_000);
            return readBatches(statement, null, shuffleId);
        }
    }

    static List<StoredBatch> selectFinalizedPage(
            Connection connection,
            String shuffleId,
            int outputPartitionId,
            long startSequence,
            int limit) throws SQLException {
        String sql = BATCH_SELECT + " WHERE b.shuffle_id = ? "
                + "AND b.output_partition_id = ? AND b.output_sequence >= ? "
                + "ORDER BY b.output_sequence LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, shuffleId);
            statement.setInt(2, outputPartitionId);
            statement.setLong(3, startSequence);
            statement.setInt(4, limit);
            statement.setFetchSize(1_000);
            return readBatches(statement, null, shuffleId);
        }
    }

    private static List<StoredBatch> readBatches(
            PreparedStatement statement,
            ExecutionOwnerIdentity owner,
            String shuffleId) throws SQLException {
        List<StoredBatch> batches = new ArrayList<>();
        try (ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                batches.add(readBatch(rows, owner, shuffleId));
            }
        }
        return List.copyOf(batches);
    }

    static StoredBatch readBatch(ResultSet rows) throws SQLException {
        return readBatch(rows, null, rows.getString("shuffle_id"));
    }

    private static StoredBatch readBatch(
            ResultSet rows,
            ExecutionOwnerIdentity owner,
            String shuffleId) throws SQLException {
        byte[] canonical = rows.getBytes("canonical_attempt");
        if (canonical == null) {
            throw new IllegalStateException(
                    "Legacy shuffle attempt has no durable Worker identity; cleanup is required");
        }
        TaskAttemptId attempt;
        try {
            attempt = TaskAttemptId.parseFrom(canonical);
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity.from(attempt.getWorkerExecutionIdentity());
        } catch (com.google.protobuf.InvalidProtocolBufferException | IllegalArgumentException failure) {
            throw new IllegalStateException("Durable shuffle attempt identity is invalid", failure);
        }
        if (!attempt.getQueryOrJobId().equals(rows.getString("query_id"))
                || attempt.getStageId() != rows.getInt("stage_id")
                || attempt.getPlanEpoch() != rows.getLong("plan_epoch")
                || attempt.getPartitionId() != rows.getInt("producer_partition_id")
                || attempt.getAttemptId() != rows.getInt("producer_attempt_id")
                || attempt.getFencingToken() != rows.getLong("producer_fencing_token")
                || !attempt.getTaskId().equals(rows.getString("task_id"))
                || attempt.getControlEpoch() != rows.getLong("control_epoch")) {
            throw new IllegalStateException("Durable shuffle attempt conflicts with indexed identity");
        }
        BatchIdentity identity = new BatchIdentity(shuffleId, attempt.getPartitionId(),
                attempt.getAttemptId(), rows.getInt("output_partition_id"),
                rows.getLong("batch_sequence"));
        return new StoredBatch(identity, attempt, rows.getString("object_key"),
                rows.getLong("batch_size_bytes"), rows.getLong("row_count"),
                rows.getBytes("batch_checksum"));
    }

    private static Allocation readAllocation(ResultSet rows) throws SQLException {
        return new Allocation(
                new AllocationIdentity(
                        rows.getString("shuffle_id"), rows.getString("query_id"),
                        rows.getInt("stage_id"), rows.getLong("plan_epoch")),
                ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(rows.getString("owner_id"))
                        .setOwnerIncarnation(rows.getString("owner_incarnation"))
                        .setFencingToken(rows.getLong("owner_fencing_token"))
                        .build(),
                new AllocationShape(
                        rows.getInt("partition_count"),
                        rows.getInt("expected_producer_count"),
                        rows.getBytes("producer_set_checksum")),
                rows.getLong("capability_expires_at_ms"),
                rows.getLong("cleanup_expires_at_ms"),
                rows.getLong("metadata_version"),
                ShuffleAllocationState.valueOf(rows.getString("state")));
    }
}
