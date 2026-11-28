package org.pangrova.shuffle.server;

import org.pangrova.shuffle.server.enums.ShuffleAllocationState;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.security.enums.CapabilityOperation;
import org.pangrova.shuffle.server.ShuffleObjectStorage.ManifestWriteReceipt;
import org.pangrova.shuffle.server.authority.ExecutionAnchorFence;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import static org.pangrova.shuffle.server.persistence.JdbcTransactions.read;
import static org.pangrova.shuffle.server.persistence.JdbcTransactions.transaction;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.findAllocationByGeneration;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.refreshAllocationExpiry;
import static org.pangrova.shuffle.server.PostgresShuffleRecords.refreshQueryScopeExpiry;
import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.AllocationIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupLease;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupPlan;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupReceipt;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CleanupStart;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizeResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.RegisterCommand;

/** PostgreSQL-backed allocation and finalization state machine. */
final class PostgresShuffleStore implements ShuffleLifecycleStore {
    private static final String SHUFFLE_SCHEMA = "shuffle";
    private static final String RECORD_READ_WINDOW = """
            , admission AS MATERIALIZED (
                SELECT query_id, granted_at,
                    LEAST(expires_at, granted_at + (? * INTERVAL '1 millisecond')) AS protected_until
                FROM candidate
            ), recorded AS (
                INSERT INTO public.execution_read_window AS existing (read_id, execution_id, protected_until)
                SELECT ?, query_id, protected_until FROM admission WHERE protected_until > granted_at
                ON CONFLICT (read_id) DO UPDATE SET
                    protected_until = GREATEST(existing.protected_until, EXCLUDED.protected_until)
                WHERE existing.execution_id = EXCLUDED.execution_id
                RETURNING read_id
            )
            SELECT FLOOR(EXTRACT(EPOCH FROM (w.protected_until - w.granted_at)) * 1000)::bigint
            FROM admission w CROSS JOIN recorded
            """;
    private static final String GRANT_READ_WINDOW = """
            WITH pinned AS MATERIALIZED (
                SELECT query_id, read_until_ms FROM shuffle_read_pin
                WHERE pin_id = ? AND query_id = ? FOR SHARE
            ), candidate AS MATERIALIZED (
                SELECT p.query_id, clock_timestamp() AS granted_at,
                    LEAST(a.expires_at, to_timestamp(p.read_until_ms / 1000.0)) AS expires_at
                FROM pinned p JOIN public.execution_anchor a ON a.execution_id = p.query_id
                WHERE NOT EXISTS (SELECT 1 FROM shuffle_cleanup_intent i WHERE i.query_id = p.query_id)
            )
            """ + RECORD_READ_WINDOW;
    private static final String RENEW_READ_WINDOW = """
            WITH owned_scope AS MATERIALIZED (
                SELECT query_id FROM shuffle_query_scope
                WHERE query_id = ? AND owner_id = ? AND owner_incarnation = ?
                    AND owner_fencing_token = ? AND state = 'ACTIVE'
                    AND NOT EXISTS (SELECT 1 FROM shuffle_cleanup_intent i
                                    WHERE i.query_id = shuffle_query_scope.query_id)
                FOR SHARE
            ), candidate AS MATERIALIZED (
                SELECT p.query_id, clock_timestamp() AS granted_at,
                    LEAST(CAST(? AS timestamptz), to_timestamp(p.read_until_ms / 1000.0)) AS expires_at
                FROM owned_scope s JOIN shuffle_read_pin p USING (query_id)
                WHERE p.pin_id = ? FOR SHARE OF p
            )
            """ + RECORD_READ_WINDOW;

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
                        "SELECT 1 FROM public.execution_anchor CROSS JOIN public.execution_read_window WHERE FALSE")) {
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

    static void requireActiveOwner(Connection connection, String queryId, ExecutionOwnerIdentity owner)
            throws SQLException {
        ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
        String sql = """
                SELECT owner_id, owner_incarnation, owner_fencing_token, state
                FROM shuffle_query_scope WHERE query_id = ? FOR SHARE
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new ShuffleServiceException(FailureKind.NOT_FOUND, "shuffle query scope does not exist");
                }
                if (!"ACTIVE".equals(row.getString(4))) {
                    throw new SecurityException("shuffle query scope is not active");
                }
                if (!owner.getOwnerId().equals(row.getString(1))
                        || !owner.getOwnerIncarnation().equals(row.getString(2))
                        || owner.getFencingToken() != row.getLong(3)) {
                    throw new SecurityException("shuffle execution owner fence does not match");
                }
            }
        }
    }

    @Override
    public ReadAdmission openRead(
            String queryId, ExecutionOwnerIdentity owner, String shuffleId, int partitionId,
            long startSequence, org.pangrova.shuffle.api.ShuffleCommitReceipt.ReadRange range,
            long readUntilMs, long drainMillis) {
        return transaction(dataSource, connection -> {
            requireActiveOwner(connection, queryId, owner);
            var published = PostgresShuffleRecords.readPublishedPartition(
                    connection, queryId, shuffleId, partitionId, range != null);
            if (range != null && (!published.partition().readRanges().contains(range)
                    || startSequence < range.startSequence() || startSequence > range.endSequenceExclusive())) {
                throw new IllegalArgumentException("shuffle read does not match a published range");
            }
            if (startSequence < 0 || startSequence - 1 > published.partition().committedLastSequence()) {
                throw new IllegalArgumentException("read start sequence exceeds partition range");
            }
            return new ReadAdmission(published, pinRead(connection, queryId, readUntilMs, drainMillis));
        });
    }

    private static ReadPin pinRead(Connection connection, String queryId, long readUntilMs, long drainMillis)
            throws SQLException {
        if (drainMillis <= 0) {
            throw new IllegalArgumentException("read drain timeout must be positive");
        }
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
        return new ReadPin(id, grantReadWindow(connection, queryId, id));
    }

    @Override
    public long grantReadWindow(String queryId, ExecutionOwnerIdentity owner, UUID id) {
        return transaction(dataSource, connection -> {
            // Sample the terminal seal in a new statement after acquiring the anchor lock.
            var ownerExpiresAt = ExecutionAnchorFence.lockLiveOwnerExpiry(connection, queryId, owner);
            try (PreparedStatement grant = connection.prepareStatement(RENEW_READ_WINDOW)) {
                grant.setString(1, queryId);
                grant.setString(2, owner.getOwnerId());
                grant.setString(3, owner.getOwnerIncarnation());
                grant.setLong(4, owner.getFencingToken());
                grant.setObject(5, ownerExpiresAt);
                grant.setObject(6, id);
                grant.setLong(7, ShuffleReadLease.WINDOW_MILLIS);
                grant.setObject(8, id);
                return executeReadGrant(grant);
            }
        });
    }

    private static long grantReadWindow(Connection connection, String queryId, UUID id) throws SQLException {
        try (PreparedStatement grant = connection.prepareStatement(GRANT_READ_WINDOW)) {
            grant.setObject(1, id);
            grant.setString(2, queryId);
            grant.setLong(3, ShuffleReadLease.WINDOW_MILLIS);
            grant.setObject(4, id);
            return executeReadGrant(grant);
        }
    }

    private static long executeReadGrant(PreparedStatement grant) throws SQLException {
        try (ResultSet row = grant.executeQuery()) {
            if (!row.next()) {
                throw new SecurityException("shuffle read pin is closed, expired or terminally fenced");
            }
            return row.getLong(1);
        }
    }

    @Override
    public void releaseRead(UUID id) {
        transaction(dataSource, connection -> {
            // Consume the pin deletion before locking the execution window, as renewal does.
            String sql = """
                    WITH target(read_id) AS (VALUES (CAST(? AS uuid))),
                    released_pin AS (
                        DELETE FROM shuffle_read_pin p USING target t
                        WHERE p.pin_id = t.read_id RETURNING p.pin_id
                    )
                    DELETE FROM public.execution_read_window
                    WHERE read_id = COALESCE((SELECT pin_id FROM released_pin), (SELECT read_id FROM target))
                    """;
            try (PreparedStatement release = connection.prepareStatement(sql)) {
                release.setObject(1, id);
                release.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public void recoverCleanupAccess(
            String queryId, ExecutionOwnerIdentity owner, long expiresAtMs) {
        transaction(dataSource, connection -> {
            requireFutureExpiry(connection, expiresAtMs);
            ExecutionAnchorFence.requireCleanupOwner(connection, queryId, owner);
            QueryScope scope = lockExistingQueryScope(connection, queryId);
            if (scope == null) {
                return null;
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
        renewAccess(queryId, shuffleId, owner, expiresAtMs, allocation -> {});
    }

    @Override
    public void renewTaskAccess(String shuffleId, ExecutionOwnerIdentity owner, long expiresAtMs,
            TaskAttemptId attempt, CapabilityOperation operation) {
        renewAccess(attempt.getQueryOrJobId(), shuffleId, owner, expiresAtMs, allocation -> {
            switch (operation) {
                case READ -> ShuffleAccessControl.requireConsumerAttempt(allocation, attempt);
                case WRITE -> ShuffleAccessControl.requireProducerAttempt(allocation, attempt, -1);
                default -> throw new IllegalArgumentException("task access requires READ or WRITE");
            }
        });
    }

    private void renewAccess(String queryId, String shuffleId, ExecutionOwnerIdentity owner, long expiresAtMs,
            Consumer<Allocation> validateAllocation) {
        transaction(dataSource, connection -> {
            requireFutureExpiry(connection, expiresAtMs);
            ExecutionAnchorFence.requireLiveOwner(connection, queryId, owner, true);
            QueryScope scope = lockExistingQueryScope(connection, queryId);
            if (scope == null || (!shuffleId.isBlank() && !"ACTIVE".equals(scope.state()))) {
                throw new SecurityException("shuffle query scope is not active");
            }
            requireSameOwner(scope.owner(), owner);
            if (!shuffleId.isBlank()) {
                Allocation allocation = PostgresShuffleRecords.findAllocation(
                        connection, shuffleId, true).orElseThrow(
                                () -> new ShuffleServiceException(FailureKind.NOT_FOUND, "shuffle does not exist"));
                if (!allocation.identity().queryId().equals(queryId)
                        || allocation.state() == ShuffleAllocationState.CLEANING) {
                    throw new SecurityException("shuffle allocation does not belong to query");
                }
                validateAllocation.accept(allocation);
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
            requireNoCleanupIntent(connection, command.queryId());
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
    public String beginManifestWrite(Allocation allocation, String objectKey, long recoveryDelayMs) {
        return transitions.beginManifestWrite(allocation, objectKey, recoveryDelayMs);
    }

    @Override
    public void settleManifestWrite(String reservation, String shuffleId, String objectKey) {
        transitions.settleManifestWrite(reservation, shuffleId, objectKey);
    }

    @Override
    public void settleRejectedManifestWrite(String reservation, String shuffleId, String objectKey) {
        transitions.settleRejectedManifestWrite(reservation, shuffleId, objectKey);
    }

    @Override
    public CleanupStart beginCleanup(CleanupCommand command, CleanupLease lease) {
        return transitions.beginCleanup(command, lease);
    }

    @Override
    public Optional<CleanupPlan> beginExpiredCleanup(CleanupLease lease) {
        return transitions.beginExpiredCleanup(lease);
    }

    @Override
    public void settleVisibleManifestWrites(String shuffleId, List<ManifestWriteReceipt> receipts) {
        transitions.settleVisibleManifestWrites(shuffleId, receipts);
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
    public List<String> cleanupShuffles(
            CleanupPlan plan, String after, int limit, CleanupLease lease) {
        return transitions.cleanupShuffles(plan, after, limit, lease);
    }

    @Override
    public void renewCleanup(CleanupPlan plan, CleanupLease lease) {
        transitions.renewCleanup(plan, lease);
    }

    @Override
    public CleanupReceipt completeCleanup(CleanupPlan plan) {
        return transitions.completeCleanup(plan);
    }

    @Override
    public void close() {
        dataSource.close();
    }

    static void requireNoCleanupIntent(Connection connection, String queryId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM shuffle_cleanup_intent WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                if (row.next()) {
                    throw new SecurityException("shuffle admission is terminally fenced");
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
        if (!"ACTIVE".equals(scope.state())) {
            throw new IllegalStateException("shuffle query is being cleaned up");
        }
        adoptOwnerIfFenced(connection, command.queryId(), scope.owner(), command.owner());
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
                        + "owner_fencing_token = ? WHERE query_id = ?")) {
            statement.setString(1, requested.getOwnerId());
            statement.setString(2, requested.getOwnerIncarnation());
            statement.setLong(3, requested.getFencingToken());
            statement.setString(4, queryId);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("shuffle scope disappeared during owner takeover");
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
