package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.shuffle.server.authority.ExecutionAnchorFence;

import static org.pangrova.shuffle.server.PostgresShuffleRecords.claimCleanup;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.findExpiredCleanup;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.requireCleanupPageLimit;
import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupShufflePage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionPublication;
import static org.pangrova.shuffle.server.persistence.JdbcTransactions.transaction;

/** Durable finalization and cleanup transitions sharing the shuffle transaction authority. */
final class PostgresShuffleLifecycleTransitions {
    private static final long MAX_SWEEP_INTERVAL_MILLIS = 3_600_000L;
    private static final String UNCERTAIN_SWEEP_RENEW_SQL =
            "UPDATE shuffle_uncertain_write_tombstone SET "
                    + "lease_expires_at = clock_timestamp() + (? * INTERVAL '1 millisecond') "
                    + "WHERE shuffle_id = ? AND lease_owner = ?::uuid "
                    + "AND lease_expires_at > clock_timestamp()";
    private static final String UNCERTAIN_SWEEP_COMPLETE_SQL =
            "UPDATE shuffle_uncertain_write_tombstone SET "
                    + "next_sweep_at = clock_timestamp() + (? * INTERVAL '1 millisecond'), "
                    + "lease_owner = NULL, lease_expires_at = NULL "
                    + "WHERE shuffle_id = ? AND lease_owner = ?::uuid "
                    + "AND lease_expires_at > clock_timestamp()";
    private final DataSource dataSource;

    PostgresShuffleLifecycleTransitions(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    FinalizeResult finalizeShuffle(FinalizeCommand command) {
        return transaction(dataSource, connection -> {
            String queryId = ExecutionAnchorFence.queryIdForShuffle(
                    connection, command.shuffleId());
            PostgresShuffleStore.requireActiveOwner(connection, queryId, command.fence().owner());
            Allocation allocation = PostgresShuffleRecords.findAllocation(
                    connection, command.shuffleId(), true)
                    .orElseThrow(() -> new IllegalArgumentException("shuffle does not exist"));
            requireFinalizeFence(allocation, command);
            if (allocation.state() == ShuffleAllocationState.FINALIZED) {
                return readFinalization(connection, allocation);
            }
            PostgresShuffleRecords.requireRegistered(allocation);
            PostgresShuffleStore.requireNoCleanupIntent(connection, queryId);
            requireCompleteProducerSet(connection, allocation);
            requireFinalizedPartitions(connection, allocation, command.partitions());
            storeFinalization(connection, command);
            markFinalized(connection, allocation);
            return result(allocation, command.partitions().stream().map(PartitionPublication::partition).toList());
        });
    }

    String beginManifestWrite(Allocation selected, String key, long recoveryDelayMs) {
        return transaction(dataSource, connection -> {
            String queryId = selected.identity().queryId();
            String shuffleId = selected.identity().shuffleId();
            PostgresShuffleStore.requireActiveOwner(connection, queryId, selected.owner());
            Allocation current = PostgresShuffleRecords.findAllocation(connection, shuffleId, true)
                    .orElseThrow(() -> new IllegalStateException("shuffle does not exist"));
            if (!current.identity().equals(selected.identity())) {
                throw new SecurityException("manifest upload allocation has changed");
            }
            long winnerVersion = current.metadataVersion();
            if (current.state() == ShuffleAllocationState.FINALIZED) {
                // A concurrent publication of exactly this version produced the same content-addressed nodes.
                winnerVersion--;
            } else {
                PostgresShuffleRecords.requireRegistered(current);
            }
            if (winnerVersion != selected.metadataVersion()) {
                throw new IllegalStateException("shuffle winner set changed during manifest upload");
            }
            return PostgresShuffleRecords.reserveWriteIntent(connection, queryId, shuffleId, key, recoveryDelayMs);
        });
    }

    void settleManifestWrite(String reservation, String shuffleId, String key) {
        transaction(dataSource, connection -> {
            String sql = """
                    DELETE FROM shuffle_write_intent
                    WHERE reservation_id = ?::uuid AND shuffle_id = ? AND object_key = ?
                      AND recovery_after > clock_timestamp()
                    """;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, reservation);
                statement.setString(2, shuffleId);
                statement.setString(3, key);
                if (statement.executeUpdate() != 1) {
                    throw new SecurityException("shuffle manifest write reservation expired or was lost");
                }
            }
            return null;
        });
    }

    Optional<CleanupPlan> beginCleanup(CleanupCommand command, CleanupLease lease) {
        String queryId = command.queryId();
        CleanupAdmission admission = transaction(dataSource, connection -> {
            ExecutionAnchorFence.requireLiveOwner(connection, queryId, command.owner(), true);
            PostgresShuffleStore.QueryScope scope = PostgresShuffleStore.lockExistingQueryScope(connection, queryId);
            if (scope == null) {
                deleteCleanupIntent(connection, queryId);
                return new CleanupAdmission(null, null);
            }
            PostgresShuffleStore.adoptOwnerIfFenced(connection, queryId, scope.owner(), command.owner());
            if (hasActiveReads(connection, queryId)) {
                PostgresShuffleRecords.sealCleanupAdmission(connection, queryId);
                return new CleanupAdmission(null, "shuffle reads are still in progress");
            }
            if (protectOutstandingWrites(connection, queryId)) {
                PostgresShuffleRecords.sealCleanupAdmission(connection, queryId);
                return new CleanupAdmission(null, "shuffle writes are still in progress");
            }
            claimCleanup(connection, queryId, scope.state(), scope.cleanupLeaseExpiresAtMs(), lease, false);
            return new CleanupAdmission(PostgresShuffleRecords.cleanupPlan(connection, queryId, lease.owner()), null);
        });
        // Preserve the committed admission seal even when existing I/O delays deletion.
        if (admission.pendingReason() != null) throw new IllegalStateException(admission.pendingReason());
        return Optional.ofNullable(admission.plan());
    }

    private record CleanupAdmission(CleanupPlan plan, String pendingReason) {}

    Optional<CleanupPlan> beginExpiredCleanup(CleanupLease lease) {
        return transaction(dataSource, connection -> {
            String queryId = findExpiredCleanup(connection, lease.nowMs());
            if (queryId == null) return Optional.empty();
            PostgresShuffleStore.QueryScope scope = PostgresShuffleStore.lockExistingQueryScope(connection, queryId);
            if (hasActiveReads(connection, queryId)) return Optional.empty();
            if (protectOutstandingWrites(connection, queryId)) return Optional.empty();
            claimCleanup(connection, queryId, scope.state(), scope.cleanupLeaseExpiresAtMs(), lease, true);
            return Optional.of(PostgresShuffleRecords.cleanupPlan(connection, queryId, lease.owner()));
        });
    }

    Optional<String> claimUncertainSweep(String owner, long leaseMillis) {
        requireSweepInterval(leaseMillis);
        return transaction(dataSource, connection -> {
            String shuffleId;
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT shuffle_id FROM shuffle_uncertain_write_tombstone "
                            + "WHERE next_sweep_at <= clock_timestamp() "
                            + "AND (lease_expires_at IS NULL "
                            + "OR lease_expires_at <= clock_timestamp()) "
                            + "ORDER BY next_sweep_at, shuffle_id "
                            + "FOR UPDATE SKIP LOCKED LIMIT 1")) {
                try (ResultSet row = select.executeQuery()) {
                    if (!row.next()) {
                        return Optional.empty();
                    }
                    shuffleId = row.getString(1);
                }
            }
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE shuffle_uncertain_write_tombstone "
                            + "SET lease_owner = ?::uuid, "
                            + "lease_expires_at = clock_timestamp() "
                            + "+ (? * INTERVAL '1 millisecond') "
                            + "WHERE shuffle_id = ?")) {
                update.setString(1, owner);
                update.setLong(2, leaseMillis);
                update.setString(3, shuffleId);
                update.executeUpdate();
            }
            return Optional.of(shuffleId);
        });
    }

    void renewUncertainSweep(String shuffleId, String owner, long leaseMillis) {
        requireSweepInterval(leaseMillis);
        transaction(dataSource, connection -> {
            updateUncertainSweep(connection, shuffleId, owner, false, leaseMillis);
            return null;
        });
    }

    void completeUncertainSweep(String shuffleId, String owner, long intervalMillis) {
        requireSweepInterval(intervalMillis);
        transaction(dataSource, connection -> {
            updateUncertainSweep(connection, shuffleId, owner, true, intervalMillis);
            return null;
        });
    }

    CleanupObjectPage cleanupObjects(
            CleanupPlan plan, CleanupCursor after, int limit, CleanupLease lease) {
        return transaction(dataSource, connection -> {
            requireCleanupPageLimit(limit);
            PostgresShuffleRecords.renewCleanup(connection, plan, lease);
            return PostgresShuffleRecords.cleanupObjects(connection, plan, after, limit);
        });
    }

    CleanupShufflePage cleanupShuffles(
            CleanupPlan plan, String after, int limit, CleanupLease lease) {
        return transaction(dataSource, connection -> {
            requireCleanupPageLimit(limit);
            PostgresShuffleRecords.renewCleanup(connection, plan, lease);
            return PostgresShuffleRecords.cleanupShuffles(connection, plan, after, limit);
        });
    }

    void renewCleanup(CleanupPlan plan, CleanupLease lease) {
        transaction(dataSource, connection -> {
            PostgresShuffleRecords.renewCleanup(connection, plan, lease);
            return null;
        });
    }

    void completeCleanup(CleanupPlan plan) {
        transaction(dataSource, connection -> {
            PostgresShuffleRecords.completeCleanup(connection, plan);
            deleteCleanupIntent(connection, plan.queryId());
            return null;
        });
    }

    private static boolean hasActiveReads(Connection connection, String queryId) throws SQLException {
        // pinRead and cleanup serialize on the same query scope row.
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM shuffle_read_pin WHERE query_id = ? "
                        + "AND protect_until_ms > EXTRACT(EPOCH FROM clock_timestamp()) * 1000 LIMIT 1")) {
            statement.setString(1, queryId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static boolean protectOutstandingWrites(Connection connection, String queryId) throws SQLException {
        // Use one cutoff for both outcomes. An expiring reservation must either hold
        // cleanup or produce a durable tombstone for late object writes.
        String sql = """
                WITH cutoff AS MATERIALIZED (
                    SELECT clock_timestamp() AS now
                ), reservations AS MATERIALIZED (
                    SELECT i.shuffle_id, i.recovery_after > cutoff.now AS protected
                    FROM shuffle_write_intent i JOIN shuffle_allocation a USING (shuffle_id)
                    CROSS JOIN cutoff WHERE a.query_id = ?
                ), fenced AS (
                    INSERT INTO shuffle_uncertain_write_tombstone (shuffle_id)
                    SELECT DISTINCT shuffle_id FROM reservations WHERE NOT protected
                      AND NOT EXISTS (SELECT 1 FROM reservations WHERE protected)
                    ON CONFLICT (shuffle_id) DO NOTHING
                )
                SELECT EXISTS (SELECT 1 FROM reservations WHERE protected)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                row.next();
                return row.getBoolean(1);
            }
        }
    }

    private static void deleteCleanupIntent(Connection connection, String queryId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM shuffle_cleanup_intent WHERE query_id = ?")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        }
    }

    private static void updateUncertainSweep(
            Connection connection, String shuffleId, String owner,
            boolean complete, long intervalMillis) throws SQLException {
        String sql = complete ? UNCERTAIN_SWEEP_COMPLETE_SQL : UNCERTAIN_SWEEP_RENEW_SQL;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, intervalMillis);
            statement.setString(2, shuffleId);
            statement.setString(3, owner);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("uncertain write sweep lease was lost");
            }
        }
    }

    private static void requireSweepInterval(long millis) {
        if (millis <= 0 || millis > MAX_SWEEP_INTERVAL_MILLIS) {
            throw new IllegalArgumentException("uncertain sweep interval is invalid");
        }
    }

    private static void requireFinalizeFence(Allocation allocation, FinalizeCommand command) {
        var fence = command.fence();
        fence.requireBinding(allocation);
        if (allocation.state() != ShuffleAllocationState.FINALIZED
                && allocation.metadataVersion() != fence.metadataVersion()) {
            throw new IllegalStateException("shuffle changed during finalization");
        }
    }

    private static void requireCompleteProducerSet(Connection connection, Allocation allocation)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COUNT(*), MIN(producer_partition_id), MAX(producer_partition_id) "
                        + "FROM shuffle_producer_commit WHERE shuffle_id = ?")) {
            statement.setString(1, allocation.identity().shuffleId());
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                int expected = allocation.shape().expectedProducerCount();
                if (rows.getLong(1) != expected) {
                    throw new IllegalStateException("shuffle producer set is incomplete");
                }
                // The primary key makes each producer unique, so count and bounds prove coverage.
                if (rows.getInt(2) != 0 || rows.getInt(3) != expected - 1) {
                    throw new IllegalStateException("shuffle producer set is not contiguous");
                }
            }
        }
    }

    private static void requireFinalizedPartitions(
            Connection connection, Allocation allocation, List<PartitionPublication> partitions)
            throws SQLException {
        Map<Integer, PartitionTotals> actual = partitionTotals(connection, allocation);
        if (partitions.size() != allocation.shape().partitionCount()) {
            throw new IllegalStateException("shuffle finalization lacks partition coverage");
        }
        HashSet<Integer> seen = new HashSet<>();
        for (PartitionPublication publication : partitions) {
            FinalizedPartition partition = publication.partition();
            if (partition.partitionId() < 0
                    || partition.partitionId() >= allocation.shape().partitionCount()
                    || !seen.add(partition.partitionId())) {
                throw new IllegalStateException("shuffle finalization partition is invalid");
            }
            PartitionTotals totals = actual.getOrDefault(
                    partition.partitionId(), PartitionTotals.EMPTY);
            if (partition.totalBytes() != totals.bytes()
                    || partition.totalRows() != totals.rows()
                    || partition.committedLastSequence() != totals.batchCount() - 1) {
                throw new IllegalStateException(
                        "shuffle finalization does not match committed batch metadata");
            }
        }
    }

    private static Map<Integer, PartitionTotals> partitionTotals(
            Connection connection, Allocation allocation) throws SQLException {
        String sql = "SELECT b.output_partition_id, COALESCE(SUM(b.batch_size_bytes), 0), "
                + "COALESCE(SUM(b.row_count), 0), COUNT(*) FROM shuffle_batch b "
                + "JOIN shuffle_producer_commit c ON c.shuffle_id = b.shuffle_id "
                + "AND c.producer_partition_id = b.producer_partition_id "
                + "AND c.producer_attempt_id = b.producer_attempt_id "
                + "WHERE b.shuffle_id = ? GROUP BY b.output_partition_id";
        Map<Integer, PartitionTotals> result = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, allocation.identity().shuffleId());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.put(rows.getInt(1), new PartitionTotals(
                            rows.getLong(2), rows.getLong(3), rows.getLong(4)));
                }
            }
        }
        return result;
    }

    private static void storeFinalization(Connection connection, FinalizeCommand command)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO shuffle_finalized_partition "
                        + "(shuffle_id, partition_id, total_bytes, total_rows, "
                        + "committed_last_sequence, output_checksum, manifest_root) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (PartitionPublication publication : command.partitions()) {
                FinalizedPartition partition = publication.partition();
                statement.setString(1, command.shuffleId());
                statement.setInt(2, partition.partitionId());
                statement.setLong(3, partition.totalBytes());
                statement.setLong(4, partition.totalRows());
                statement.setLong(5, partition.committedLastSequence());
                statement.setBytes(6, partition.checksum());
                statement.setBytes(7, publication.manifestRoot());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static void markFinalized(Connection connection, Allocation allocation)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE shuffle_allocation SET state = 'FINALIZED', "
                        + "finalized_at = CURRENT_TIMESTAMP, metadata_version = metadata_version + 1 "
                        + "WHERE shuffle_id = ?")) {
            statement.setString(1, allocation.identity().shuffleId());
            statement.executeUpdate();
        }
    }

    private static FinalizeResult readFinalization(Connection connection, Allocation allocation)
            throws SQLException {
        List<FinalizedPartition> partitions = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT partition_id, total_bytes, total_rows, committed_last_sequence, "
                        + "output_checksum FROM shuffle_finalized_partition "
                        + "WHERE shuffle_id = ? ORDER BY partition_id")) {
            statement.setString(1, allocation.identity().shuffleId());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    partitions.add(new FinalizedPartition(
                            rows.getInt(1), rows.getLong(2), rows.getLong(3),
                            rows.getLong(4), rows.getBytes(5)));
                }
            }
        }
        return result(allocation, partitions);
    }

    private static FinalizeResult result(
            Allocation allocation, List<FinalizedPartition> partitions) {
        long totalBytes = partitions.stream().mapToLong(FinalizedPartition::totalBytes).sum();
        long totalRows = partitions.stream().mapToLong(FinalizedPartition::totalRows).sum();
        return new FinalizeResult(allocation.shape().expectedProducerCount(),
                allocation.shape().producerSetChecksum(), partitions, totalBytes, totalRows);
    }

    private record PartitionTotals(long bytes, long rows, long batchCount) {
        private static final PartitionTotals EMPTY = new PartitionTotals(0, 0, 0);
    }
}
