package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.server.authority.ExecutionAnchorFence;

import static org.pangrova.shuffle.server.persistence.JdbcTransactions.read;
import static org.pangrova.shuffle.server.persistence.JdbcTransactions.transaction;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.findAllocationByGeneration;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.refreshAllocationExpiry;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.refreshQueryScopeExpiry;
import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCursor;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupObjectPage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupShufflePage;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.RegisterCommand;

/** PostgreSQL-backed allocation and finalization state machine. */
final class PostgresShuffleStore implements ShuffleLifecycleStore {
    private static final String SHUFFLE_SCHEMA = "shuffle";
    private final HikariDataSource dataSource;
    private final PostgresShuffleBatchStore batchStore;
    private final PostgresShuffleLifecycleTransitions transitions;

    private PostgresShuffleStore(HikariDataSource dataSource) {
        this.dataSource = dataSource;
        this.batchStore = new PostgresShuffleBatchStore(dataSource);
        this.transitions = new PostgresShuffleLifecycleTransitions(dataSource);
    }

    static PostgresShuffleStore create(ShuffleServiceConfig.Database config)
            throws IOException {
        String password = Files.readString(config.passwordFile()).strip();
        if (password.isEmpty()) {
            throw new IllegalArgumentException("PostgreSQL password file must not be empty");
        }
        HikariConfig pool = new HikariConfig();
        pool.setJdbcUrl(config.jdbcUrl());
        pool.setUsername(config.username());
        pool.setPassword(password);
        pool.setMaximumPoolSize(config.maximumPoolSize());
        pool.setPoolName("pangrova-shuffle-metadata");
        pool.setSchema(SHUFFLE_SCHEMA);
        pool.setAutoCommit(true);
        HikariDataSource dataSource = new HikariDataSource(pool);
        try {
            Flyway.configure()
                    .dataSource(dataSource)
                    .schemas(SHUFFLE_SCHEMA)
                    .defaultSchema(SHUFFLE_SCHEMA)
                    .locations("classpath:org/pangrova/shuffle/migration")
                    .load()
                    .migrate();
            read(dataSource, connection -> {
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT execution_id FROM public.execution_anchor WHERE FALSE")) {
                    statement.executeQuery().close();
                }
                return null;
            });
            return new PostgresShuffleStore(dataSource);
        } catch (RuntimeException | Error failure) {
            try {
                dataSource.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }
    PostgresShuffleBatchStore batchStore() {
        return batchStore;
    }

    void requireObjectNamespace(ShuffleServiceConfig.ObjectStorage storage) {
        String endpoint = storage.endpoint() == null ? "" : storage.endpoint().toString();
        transaction(dataSource, connection -> {
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO shuffle_storage_namespace "
                            + "(singleton, bucket, prefix, region, endpoint) "
                            + "VALUES (1, ?, ?, ?, ?) ON CONFLICT (singleton) DO NOTHING")) {
                insert.setString(1, storage.bucket());
                insert.setString(2, storage.prefix());
                insert.setString(3, storage.region());
                insert.setString(4, endpoint);
                insert.executeUpdate();
            }
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT bucket, prefix, region, endpoint "
                            + "FROM shuffle_storage_namespace WHERE singleton = 1 FOR SHARE");
                    ResultSet row = select.executeQuery()) {
                if (!row.next() || !row.getString(1).equals(storage.bucket())
                        || !row.getString(2).equals(storage.prefix())
                        || !row.getString(3).equals(storage.region())
                        || !row.getString(4).equals(endpoint)) {
                    throw new SecurityException(
                            "Shuffle object namespace differs from durable metadata authority");
                }
            }
            return null;
        });
    }

    @Override
    public void requireLiveOwner(String queryId, ExecutionOwnerIdentity owner) {
        transaction(dataSource, connection -> {
            requireActiveOwner(connection, queryId, owner);
            return null;
        });
    }

    private static void requireActiveOwner(Connection connection, String queryId, ExecutionOwnerIdentity owner)
            throws SQLException {
        ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
        QueryScope scope = lockExistingQueryScope(connection, queryId);
        if (scope == null) {
            throw new org.pangrova.shuffle.server.exception.ShuffleServiceException(
                    org.pangrova.shuffle.server.enums.FailureKind.NOT_FOUND,
                    "shuffle query scope does not exist");
        }
        if (!"ACTIVE".equals(scope.state())) {
            throw new SecurityException("shuffle query scope is not active");
        }
        requireSameOwner(scope.owner(), owner);
    }

    @Override
    public UUID pinRead(String queryId, ExecutionOwnerIdentity owner, long readUntilMs, long drainMillis) {
        if (drainMillis <= 0) throw new IllegalArgumentException("read drain timeout must be positive");
        return transaction(dataSource, connection -> {
            requireActiveOwner(connection, queryId, owner);
            requireFutureExpiry(connection, readUntilMs);
            UUID id = UUID.randomUUID();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO shuffle_read_pin (pin_id, query_id, read_until_ms, protect_until_ms) "
                            + "VALUES (?, ?, ?, ?)")) {
                insert.setObject(1, id);
                insert.setString(2, queryId);
                insert.setLong(3, readUntilMs);
                insert.setLong(4, Math.addExact(readUntilMs, drainMillis));
                insert.executeUpdate();
            }
            return id;
        });
    }

    @Override
    public void requirePinnedRead(String queryId, ExecutionOwnerIdentity owner, UUID id) {
        transaction(dataSource, connection -> {
            requireActiveOwner(connection, queryId, owner);
            try (PreparedStatement check = connection.prepareStatement(
                    "SELECT 1 FROM shuffle_read_pin WHERE pin_id = ? AND query_id = ? "
                            + "AND read_until_ms > EXTRACT(EPOCH FROM clock_timestamp()) * 1000")) {
                check.setObject(1, id);
                check.setString(2, queryId);
                try (ResultSet row = check.executeQuery()) {
                    if (!row.next()) throw new SecurityException("shuffle read pin is closed or expired");
                }
            }
            return null;
        });
    }

    @Override
    public void releaseRead(UUID id) {
        transaction(dataSource, connection -> {
            try (PreparedStatement delete = connection.prepareStatement(
                    "DELETE FROM shuffle_read_pin WHERE pin_id = ?")) {
                delete.setObject(1, id);
                delete.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public void recoverCleanupAccess(
            String queryId, ExecutionOwnerIdentity owner, long expiresAtMs) {
        transaction(dataSource, connection -> {
            requireFutureExpiry(connection, expiresAtMs);
            ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
            QueryScope scope = lockExistingQueryScope(connection, queryId);
            if (scope == null) {
                return null;
            }
            if (!"ACTIVE".equals(scope.state())) {
                throw new SecurityException("shuffle query scope is not active");
            }
            adoptOwnerIfFenced(connection, queryId, scope.owner(), owner);
            refreshQueryScopeExpiry(connection, queryId, expiresAtMs);
            return null;
        });
    }

    @Override
    public void recoverFinalizeAccess(
            String shuffleId, ExecutionOwnerIdentity owner, long expiresAtMs) {
        transaction(dataSource, connection -> {
            requireFutureExpiry(connection, expiresAtMs);
            String queryId = ExecutionAnchorFence.queryIdForShuffle(connection, shuffleId);
            ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
            QueryScope scope = lockExistingQueryScope(connection, queryId);
            if (scope == null || !"ACTIVE".equals(scope.state())) {
                throw new SecurityException("shuffle query scope is not active");
            }
            adoptOwnerIfFenced(connection, queryId, scope.owner(), owner);
            Allocation allocation = PostgresShuffleRecords.findAllocation(
                    connection, shuffleId, true).orElseThrow();
            if (allocation.state() != ShuffleAllocationState.REGISTERED
                    && allocation.state() != ShuffleAllocationState.FINALIZED) {
                throw new SecurityException("shuffle allocation cannot be finalized");
            }
            refreshAllocationExpiry(connection, shuffleId, expiresAtMs);
            refreshQueryScopeExpiry(connection, queryId, expiresAtMs);
            return null;
        });
    }

    @Override
    public void renewAccess(
            String queryId,
            String shuffleId,
            ExecutionOwnerIdentity owner,
            long expiresAtMs) {
        transaction(dataSource, connection -> {
            requireFutureExpiry(connection, expiresAtMs);
            ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
            QueryScope scope = lockExistingQueryScope(connection, queryId);
            if (scope == null || !"ACTIVE".equals(scope.state())) {
                throw new SecurityException("shuffle query scope is not active");
            }
            requireSameOwner(scope.owner(), owner);
            if (!shuffleId.isBlank()) {
                Allocation allocation = PostgresShuffleRecords.findAllocation(
                        connection, shuffleId, true).orElseThrow(
                                () -> new IllegalArgumentException("shuffle does not exist"));
                if (!allocation.identity().queryId().equals(queryId)
                        || allocation.state() == ShuffleAllocationState.CLEANING) {
                    throw new SecurityException("shuffle allocation does not belong to query");
                }
                refreshAllocationExpiry(connection, shuffleId, expiresAtMs);
            }
            refreshQueryScopeExpiry(connection, queryId, expiresAtMs);
            return null;
        });
    }
    @Override
    public Allocation register(RegisterCommand command) {
        return transaction(dataSource, connection -> {
            ExecutionAnchorFence.requireLiveOwner(
                    connection, command.queryId(), command.owner(), true);
            rejectTerminalIntent(connection, command.queryId());
            QueryScope queryScope = lockQueryScope(connection, command);
            Optional<Allocation> existing = findAllocationByGeneration(
                    connection, command.queryId(), command.stageId(), command.planEpoch());
            if (existing.isPresent()) {
                requireSameRegistration(existing.orElseThrow(), command);
                refreshCapabilityExpiry(connection, existing.orElseThrow(), command);
                return PostgresShuffleRecords.findAllocation(
                        connection, existing.orElseThrow().identity().shuffleId(), false)
                        .orElseThrow();
            }
            String shuffleId = command.requestedShuffleId().isBlank()
                    ? UUID.randomUUID().toString() : command.requestedShuffleId();
            rejectTombstonedShuffleId(connection, shuffleId);
            insertAllocation(connection, shuffleId, command);
            return new Allocation(
                    new AllocationIdentity(
                            shuffleId, command.queryId(), command.stageId(), command.planEpoch()),
                    command.owner(), command.shape(), command.capabilityExpiresAtMs(),
                    queryScope.cleanupExpiresAtMs(), 0, ShuffleAllocationState.REGISTERED);
        });
    }
    @Override
    public Optional<Allocation> findAllocation(String shuffleId) {
        return transaction(dataSource, connection -> {
            Optional<Allocation> allocation = PostgresShuffleRecords.findAllocation(
                    connection, shuffleId, false);
            if (allocation.isPresent()) {
                Allocation found = allocation.orElseThrow();
                ExecutionAnchorFence.requireLiveOwner(connection,
                        found.identity().queryId(), found.owner(), true);
            }
            return allocation;
        });
    }

    @Override
    public FinalizeResult finalizeShuffle(FinalizeCommand command) {
        return transitions.finalizeShuffle(command);
    }

    @Override
    public Optional<FinalizeResult> findFinalization(String shuffleId) {
        return transitions.findFinalization(shuffleId);
    }

    @Override
    public Optional<CleanupPlan> beginCleanup(CleanupCommand command, CleanupLease lease) {
        return transitions.beginCleanup(command, lease);
    }

    @Override
    public Optional<CleanupPlan> beginExpiredCleanup(CleanupLease lease) {
        return transitions.beginExpiredCleanup(lease);
    }

    @Override
    public Optional<String> claimUncertainSweep(String owner, long leaseMillis) {
        return transitions.claimUncertainSweep(owner, leaseMillis);
    }

    @Override
    public void renewUncertainSweep(String shuffleId, String owner, long leaseMillis) {
        transitions.renewUncertainSweep(shuffleId, owner, leaseMillis);
    }

    @Override
    public void completeUncertainSweep(String shuffleId, String owner, long intervalMillis) {
        transitions.completeUncertainSweep(shuffleId, owner, intervalMillis);
    }

    @Override
    public CleanupObjectPage cleanupObjects(
            CleanupPlan plan, CleanupCursor after, int limit, CleanupLease lease) {
        return transitions.cleanupObjects(plan, after, limit, lease);
    }

    @Override
    public CleanupShufflePage cleanupShuffles(
            CleanupPlan plan, String after, int limit, CleanupLease lease) {
        return transitions.cleanupShuffles(plan, after, limit, lease);
    }

    @Override
    public void renewCleanup(CleanupPlan plan, CleanupLease lease) {
        transitions.renewCleanup(plan, lease);
    }

    @Override
    public void completeCleanup(CleanupPlan plan) {
        transitions.completeCleanup(plan);
    }

    @Override
    public void close() {
        dataSource.close();
    }

    private static void rejectTerminalIntent(Connection connection, String queryId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM shuffle_cleanup_intent WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                if (row.next()) {
                    throw new SecurityException("shuffle registration is terminally fenced");
                }
            }
        }
    }

    private static void rejectTombstonedShuffleId(Connection connection, String shuffleId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM shuffle_uncertain_write_tombstone WHERE shuffle_id = ?")) {
            statement.setString(1, shuffleId);
            try (ResultSet row = statement.executeQuery()) {
                if (row.next()) {
                    throw new SecurityException(
                            "shuffle identifier remains fenced by uncertain write cleanup");
                }
            }
        }
    }

    private static QueryScope lockQueryScope(
            Connection connection,
            RegisterCommand command) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO shuffle_query_scope "
                        + "(query_id, owner_id, owner_incarnation, owner_fencing_token, "
                        + "cleanup_expires_at_ms) VALUES (?, ?, ?, ?, ?) "
                        + "ON CONFLICT (query_id) DO NOTHING")) {
            insert.setString(1, command.queryId());
            insert.setString(2, command.owner().getOwnerId());
            insert.setString(3, command.owner().getOwnerIncarnation());
            insert.setLong(4, command.owner().getFencingToken());
            insert.setLong(5, command.capabilityExpiresAtMs());
            insert.executeUpdate();
        }
        QueryScope scope = lockExistingQueryScope(connection, command.queryId());
        adoptOwnerIfFenced(connection, command.queryId(), scope.owner(), command.owner());
        if (!"ACTIVE".equals(scope.state())) {
            throw new IllegalStateException("shuffle query is being cleaned up");
        }
        refreshQueryScopeExpiry(connection, command.queryId(), command.capabilityExpiresAtMs());
        return new QueryScope(command.owner(),
                Math.max(scope.cleanupExpiresAtMs(), command.capabilityExpiresAtMs()),
                scope.state(), scope.cleanupLeaseOwner(), scope.cleanupLeaseExpiresAtMs());
    }

    static void adoptOwnerIfFenced(
            Connection connection,
            String queryId,
            ExecutionOwnerIdentity current,
            ExecutionOwnerIdentity requested) throws SQLException {
        if (current.equals(requested)) {
            return;
        }
        if (requested.getFencingToken() <= current.getFencingToken()) {
            throw new SecurityException("shuffle owner takeover must increase fencing token");
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE shuffle_query_scope SET owner_id = ?, owner_incarnation = ?, "
                        + "owner_fencing_token = ? WHERE query_id = ? AND state = 'ACTIVE'")) {
            statement.setString(1, requested.getOwnerId());
            statement.setString(2, requested.getOwnerIncarnation());
            statement.setLong(3, requested.getFencingToken());
            statement.setString(4, queryId);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("shuffle scope is not active for takeover");
            }
        }
    }

    private static void requireFutureExpiry(Connection connection, long expiresAtMs)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT ? > (extract(epoch FROM clock_timestamp()) * 1000)::bigint")) {
            statement.setLong(1, expiresAtMs);
            try (ResultSet row = statement.executeQuery()) {
                row.next();
                if (!row.getBoolean(1)) {
                    throw new IllegalArgumentException(
                            "shuffle capability renewal expiry must be in the future");
                }
            }
        }
    }

    static QueryScope lockExistingQueryScope(
            Connection connection,
            String queryId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_id, owner_incarnation, owner_fencing_token, "
                        + "cleanup_expires_at_ms, state, cleanup_lease_owner, "
                        + "cleanup_lease_expires_at_ms FROM shuffle_query_scope "
                        + "WHERE query_id = ? FOR UPDATE")) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                return new QueryScope(
                        ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId(rows.getString(1))
                                .setOwnerIncarnation(rows.getString(2))
                                .setFencingToken(rows.getLong(3))
                                .build(),
                        rows.getLong(4), rows.getString(5), rows.getString(6),
                        rows.getObject(7, Long.class));
            }
        }
    }

    private static void insertAllocation(
            Connection connection,
            String shuffleId,
            RegisterCommand command) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO shuffle_allocation "
                        + "(shuffle_id, query_id, stage_id, plan_epoch, partition_count, "
                        + "expected_producer_count, producer_set_checksum, "
                        + "capability_expires_at_ms, state) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'REGISTERED')")) {
            statement.setString(1, shuffleId);
            statement.setString(2, command.queryId());
            statement.setInt(3, command.stageId());
            statement.setLong(4, command.planEpoch());
            statement.setInt(5, command.shape().partitionCount());
            statement.setInt(6, command.shape().expectedProducerCount());
            statement.setBytes(7, command.shape().producerSetChecksum());
            statement.setLong(8, command.capabilityExpiresAtMs());
            statement.executeUpdate();
        }
    }

    private static void refreshCapabilityExpiry(
            Connection connection,
            Allocation allocation,
            RegisterCommand command) throws SQLException {
        refreshAllocationExpiry(connection, allocation.identity().shuffleId(),
                command.capabilityExpiresAtMs());
    }

    private static void requireSameRegistration(
            Allocation allocation,
            RegisterCommand command) {
        boolean requestedIdMatches = command.requestedShuffleId().isBlank()
                || command.requestedShuffleId().equals(allocation.identity().shuffleId());
        if (!requestedIdMatches || !allocation.owner().equals(command.owner())
                || allocation.shape().partitionCount() != command.shape().partitionCount()
                || allocation.shape().expectedProducerCount()
                        != command.shape().expectedProducerCount()
                || !Arrays.equals(allocation.shape().producerSetChecksum(),
                        command.shape().producerSetChecksum())) {
            throw new IllegalStateException(
                    "shuffle registration conflicts with the existing generation");
        }
    }

    private static void requireSameOwner(
            ExecutionOwnerIdentity actual,
            ExecutionOwnerIdentity expected) {
        if (!actual.equals(expected)) {
            throw new SecurityException("shuffle execution owner fence does not match");
        }
    }

    record QueryScope(
            ExecutionOwnerIdentity owner,
            long cleanupExpiresAtMs,
            String state,
            String cleanupLeaseOwner,
            Long cleanupLeaseExpiresAtMs) {}
}
