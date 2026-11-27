package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationShape;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupReceipt;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupShufflePage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PublishedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionPublication;

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
    private static final String BATCH_SELECT = "SELECT b.*, f.task_id, f.control_epoch, f.canonical_attempt, "
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
    private static final String FIND_WRITABLE_ALLOCATION_SQL = """
            WITH writable_scope AS MATERIALIZED (
                SELECT query_id
                FROM shuffle_query_scope
                WHERE query_id = ? AND owner_id = ? AND owner_incarnation = ?
                  AND owner_fencing_token = ? AND state = 'ACTIVE'
                FOR SHARE
            )
            SELECT a.state, a.stage_id, a.plan_epoch, a.expected_producer_count, a.partition_count
            FROM shuffle_allocation a JOIN writable_scope q USING (query_id)
            WHERE a.shuffle_id = ?
            FOR UPDATE OF a
            """;
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

    static int lockWritableAllocation(Connection connection, String shuffleId, TaskAttemptId attempt)
            throws SQLException {
        ExecutionOwnerIdentity owner = attempt.getExecutionOwner();
        try (PreparedStatement statement = connection.prepareStatement(FIND_WRITABLE_ALLOCATION_SQL)) {
            statement.setString(1, attempt.getQueryOrJobId());
            statement.setString(2, owner.getOwnerId());
            statement.setString(3, owner.getOwnerIncarnation());
            statement.setLong(4, owner.getFencingToken());
            statement.setString(5, shuffleId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new SecurityException("shuffle query scope or allocation is not writable");
                }
                if (!ShuffleAllocationState.REGISTERED.name().equals(row.getString("state"))) {
                    throw new IllegalStateException("shuffle allocation is not writable");
                }
                if (row.getInt("stage_id") != attempt.getStageId()
                        || row.getLong("plan_epoch") != attempt.getPlanEpoch()
                        || attempt.getPartitionId() >= row.getInt("expected_producer_count")) {
                    throw new SecurityException("producer attempt does not match shuffle allocation");
                }
                return row.getInt("partition_count");
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

    static PublishedPartition readPublishedPartition(
            Connection connection, String queryId, String shuffleId, int partitionId) throws SQLException {
        String sql = """
                SELECT a.state, a.partition_count, a.plan_epoch, a.expected_producer_count,
                       a.producer_set_checksum, p.committed_last_sequence,
                       p.total_bytes, p.total_rows, p.output_checksum, p.manifest_root
                FROM shuffle_allocation a
                LEFT JOIN shuffle_finalized_partition p ON p.shuffle_id = a.shuffle_id AND p.partition_id = ?
                WHERE a.shuffle_id = ? AND a.query_id = ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, partitionId);
            statement.setString(2, shuffleId);
            statement.setString(3, queryId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new ShuffleServiceException(FailureKind.NOT_FOUND, "shuffle does not exist");
                }
                if (partitionId < 0 || partitionId >= row.getInt("partition_count")) {
                    throw new IllegalArgumentException("shuffle read partition is outside the allocation");
                }
                if (!ShuffleAllocationState.FINALIZED.name().equals(row.getString("state"))) {
                    throw new IllegalStateException("shuffle is not finalized");
                }
                long lastSequence = row.getLong("committed_last_sequence");
                if (row.wasNull()) {
                    throw new IllegalStateException("finalized partition missing");
                }
                var partition = new FinalizedPartition(partitionId, row.getLong("total_bytes"),
                        row.getLong("total_rows"), lastSequence, row.getBytes("output_checksum"));
                return new PublishedPartition(row.getLong("plan_epoch"), row.getInt("expected_producer_count"),
                        row.getBytes("producer_set_checksum"),
                        new PartitionPublication(partition, row.getBytes("manifest_root")));
            }
        }
    }

    static String reserveWriteIntent(Connection connection, String queryId, String shuffleId,
            String objectKey, long recoveryDelayMs) throws SQLException {
        if (recoveryDelayMs <= 0) {
            throw new IllegalArgumentException("write recovery delay must be positive");
        }
        String reservationId = UUID.randomUUID().toString();
        String sql = """
                INSERT INTO shuffle_write_intent (reservation_id, shuffle_id, object_key, recovery_after)
                SELECT ?::uuid, ?, ?, clock_timestamp() + (? * INTERVAL '1 millisecond')
                WHERE NOT EXISTS (SELECT 1 FROM shuffle_cleanup_intent WHERE query_id = ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, reservationId);
            statement.setString(2, shuffleId);
            statement.setString(3, objectKey);
            statement.setLong(4, recoveryDelayMs);
            statement.setString(5, queryId);
            if (statement.executeUpdate() != 1) {
                throw new SecurityException("shuffle write admission is terminally fenced");
            }
        }
        return reservationId;
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

    record ExpiredCleanupScope(String queryId, Long leaseExpiresAtMs) {}

    static ExpiredCleanupScope lockExpiredCleanupScope(Connection connection, long nowMs) throws SQLException {
        String sql = "SELECT query_id, cleanup_lease_expires_at_ms FROM shuffle_query_scope "
                + "WHERE state = 'CLEANING' "
                + "AND (cleanup_lease_expires_at_ms IS NULL "
                + "OR cleanup_lease_expires_at_ms <= ?) "
                + "AND NOT EXISTS (SELECT 1 FROM shuffle_read_pin p "
                + "WHERE p.query_id = shuffle_query_scope.query_id "
                + "AND p.protect_until_ms > EXTRACT(EPOCH FROM clock_timestamp()) * 1000) "
                + "AND NOT EXISTS (SELECT 1 FROM shuffle_write_intent i JOIN shuffle_allocation a USING (shuffle_id) "
                + "WHERE a.query_id = shuffle_query_scope.query_id "
                + "AND i.recovery_after > clock_timestamp()) "
                + "ORDER BY cleanup_lease_expires_at_ms NULLS FIRST, query_id "
                + "FOR UPDATE SKIP LOCKED LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, nowMs);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? new ExpiredCleanupScope(rows.getString(1), rows.getObject(2, Long.class)) : null;
            }
        }
    }

    static void sealCleanupAdmission(Connection connection, CleanupCommand command, CleanupLease lease)
            throws SQLException {
        String sql = """
                WITH sealed AS (
                    UPDATE shuffle_query_scope SET state = 'CLEANING',
                        cleanup_lease_owner = ?, cleanup_lease_expires_at_ms = ?
                    WHERE query_id = ? AND state = 'ACTIVE' RETURNING query_id
                ), allocations AS (
                    UPDATE shuffle_allocation a SET state = 'CLEANING'
                    FROM sealed s WHERE a.query_id = s.query_id
                )
                INSERT INTO shuffle_cleanup_intent (query_id, revocation_generation)
                SELECT query_id, ? FROM sealed ON CONFLICT (query_id) DO NOTHING
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, lease == null ? null : lease.owner());
            statement.setObject(2, lease == null ? null : lease.expiresAtMs(), Types.BIGINT);
            statement.setString(3, command.queryId());
            statement.setLong(4, command.owner().getFencingToken());
            statement.executeUpdate();
        }
    }

    static void claimSealedCleanup(
            Connection connection,
            String queryId,
            Long currentLeaseExpiry,
            CleanupLease lease) throws SQLException {
        if (currentLeaseExpiry != null && currentLeaseExpiry > lease.nowMs()) {
            throw new IllegalStateException("shuffle cleanup already has an active lease");
        }
        try (PreparedStatement query = connection.prepareStatement(
                "UPDATE shuffle_query_scope SET cleanup_lease_owner = ?, cleanup_lease_expires_at_ms = ? "
                        + "WHERE query_id = ?")) {
            query.setString(1, lease.owner());
            query.setLong(2, lease.expiresAtMs());
            query.setString(3, queryId);
            query.executeUpdate();
        }
    }

    static CleanupReceipt recordEmptyScopeConvergence(Connection connection, CleanupCommand command)
            throws SQLException {
        String sql = """
                WITH target(query_id, revocation_generation) AS (VALUES (CAST(? AS text), CAST(? AS bigint))),
                recorded AS (
                    INSERT INTO shuffle_cleanup_intent (query_id, revocation_generation, converged_at)
                    SELECT query_id, revocation_generation, clock_timestamp() FROM target
                    ON CONFLICT (query_id) DO UPDATE SET converged_at = EXCLUDED.converged_at
                    WHERE shuffle_cleanup_intent.converged_at IS NULL
                    RETURNING revocation_generation, converged_at, uncertain_write_handoff
                )
                SELECT revocation_generation, converged_at, uncertain_write_handoff FROM recorded
                UNION ALL
                SELECT i.revocation_generation, i.converged_at, i.uncertain_write_handoff
                FROM shuffle_cleanup_intent i JOIN target t USING (query_id)
                WHERE i.converged_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM recorded)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, command.queryId());
            statement.setLong(2, command.owner().getFencingToken());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("empty Shuffle scope returned no convergence receipt");
                return readCleanupReceipt(command.queryId(), rows);
            }
        }
    }

    private static CleanupReceipt readCleanupReceipt(String queryId, ResultSet row) throws SQLException {
        return new CleanupReceipt(queryId, row.getLong("revocation_generation"),
                row.getObject("converged_at", OffsetDateTime.class).toInstant().toEpochMilli(),
                row.getBoolean("uncertain_write_handoff"));
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

    static CleanupReceipt completeCleanup(Connection connection, CleanupPlan plan) throws SQLException {
        String removeAllocations = """
                WITH locked AS MATERIALIZED (
                    SELECT query_id FROM shuffle_query_scope
                    WHERE query_id = ? AND state = 'CLEANING' AND cleanup_lease_owner = ? FOR UPDATE
                ), removed AS (
                    DELETE FROM shuffle_allocation a USING locked l WHERE a.query_id = l.query_id RETURNING a.shuffle_id
                )
                SELECT EXISTS (SELECT 1 FROM removed JOIN shuffle_uncertain_write_tombstone USING (shuffle_id))
                """;
        boolean uncertainWriteHandoff;
        try (PreparedStatement statement = connection.prepareStatement(removeAllocations)) {
            statement.setString(1, plan.queryId());
            statement.setString(2, plan.leaseOwner());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Shuffle cleanup returned no handoff status");
                uncertainWriteHandoff = rows.getBoolean(1);
            }
        }
        String converge = """
                WITH retired AS (
                    DELETE FROM shuffle_query_scope
                    WHERE query_id = ? AND state = 'CLEANING' AND cleanup_lease_owner = ?
                    RETURNING query_id
                )
                UPDATE shuffle_cleanup_intent i
                SET converged_at = clock_timestamp(), uncertain_write_handoff = ?
                FROM retired r WHERE i.query_id = r.query_id AND i.converged_at IS NULL
                RETURNING i.revocation_generation, i.converged_at, i.uncertain_write_handoff
                """;
        try (PreparedStatement statement = connection.prepareStatement(converge)) {
            statement.setString(1, plan.queryId());
            statement.setString(2, plan.leaseOwner());
            statement.setBoolean(3, uncertainWriteHandoff);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("shuffle cleanup lease or terminal seal has been lost");
                }
                return readCleanupReceipt(plan.queryId(), rows);
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
            var batches = readBatches(statement, shuffleId);
            if (batches.stream().anyMatch(batch -> !batch.producerAttempt().equals(attempt))) {
                throw new SecurityException("Requested shuffle attempt conflicts with durable identity");
            }
            return batches;
        }
    }

    static List<StoredBatch> selectCommittedPage(
            Connection connection, Allocation selected, BatchIdentity after, int limit) throws SQLException {
        String sql = BATCH_SELECT + """
                 JOIN shuffle_producer_commit c
                   ON (c.shuffle_id, c.producer_partition_id, c.producer_attempt_id)
                    = (b.shuffle_id, b.producer_partition_id, b.producer_attempt_id)
                 JOIN public.execution_anchor owner ON owner.execution_id = a.query_id
                 WHERE b.shuffle_id = ? AND a.query_id = ? AND a.stage_id = ? AND a.plan_epoch = ?
                   AND a.metadata_version = ? AND a.state = 'REGISTERED' AND q.state = 'ACTIVE'
                   AND (q.owner_id, q.owner_incarnation, q.owner_fencing_token) = (?, ?, ?)
                   AND (owner.owner_id, owner.owner_incarnation, owner.fencing_token)
                     = (q.owner_id, q.owner_incarnation, q.owner_fencing_token)
                   AND owner.expires_at > clock_timestamp()
                   AND (b.output_partition_id, b.producer_partition_id, b.batch_sequence) > (?, ?, ?)
                 ORDER BY b.output_partition_id, b.producer_partition_id, b.batch_sequence LIMIT ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, selected.identity().shuffleId());
            statement.setString(parameter++, selected.identity().queryId());
            statement.setInt(parameter++, selected.identity().stageId());
            statement.setLong(parameter++, selected.identity().planEpoch());
            statement.setLong(parameter++, selected.metadataVersion());
            statement.setString(parameter++, selected.owner().getOwnerId());
            statement.setString(parameter++, selected.owner().getOwnerIncarnation());
            statement.setLong(parameter++, selected.owner().getFencingToken());
            statement.setInt(parameter++, after == null ? -1 : after.outputPartitionId());
            statement.setInt(parameter++, after == null ? -1 : after.producerPartitionId());
            statement.setLong(parameter++, after == null ? -1 : after.batchSequence());
            statement.setInt(parameter, limit);
            return readBatches(statement, selected.identity().shuffleId());
        }
    }

    private static List<StoredBatch> readBatches(
            PreparedStatement statement,
            String shuffleId) throws SQLException {
        List<StoredBatch> batches = new ArrayList<>();
        try (ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                batches.add(readBatch(rows, shuffleId));
            }
        }
        return List.copyOf(batches);
    }

    private static StoredBatch readBatch(
            ResultSet rows,
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
