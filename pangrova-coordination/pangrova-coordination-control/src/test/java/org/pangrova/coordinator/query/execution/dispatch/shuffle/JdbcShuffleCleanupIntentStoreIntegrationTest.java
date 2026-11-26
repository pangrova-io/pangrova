package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.coordinator.query.execution.QueryOwnershipTracker;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;
import org.pangrova.persistence.query.postgresql.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.shuffle.api.ShuffleCleanupRecovery;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class JdbcShuffleCleanupIntentStoreIntegrationTest {
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements");
    private static PGSimpleDataSource dataSource;
    private static JdbcShuffleCleanupIntentStore store;
    private static final ShuffleExecutionOwner OWNER =
            new ShuffleExecutionOwner("coordinator", "boot-1", 7);

    @BeforeAll
    static void startDatabase() throws Exception {
        POSTGRES.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        SchemaMigrationRunner.migrate(dataSource);
        Flyway.configure().dataSource(dataSource).schemas("shuffle").defaultSchema("shuffle")
                .locations("classpath:org/pangrova/shuffle/migration").load().migrate();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        }
        store = new JdbcShuffleCleanupIntentStore(dataSource, ignored -> OWNER);
    }

    @AfterAll
    static void stopDatabase() {
        POSTGRES.stop();
    }

    @Test
    void persistsTerminalIntentWithoutARegisteredShuffleScope() throws Exception {
        String queryId = "before-register-" + UUID.randomUUID();
        seedAnchor(queryId);
        long before = cleanupStatements();
        store.markPending(queryId);
        assertThat(cleanupStatements() - before).isEqualTo(2);
        assertThat(store.isPending(queryId)).isTrue();
        assertThat(store.pendingForOwner(OWNER.ownerId(), 100)).contains(queryId);
    }

    @Test
    void validatesRegisteredScopeAndRecordsOrReplaysIntentInTwoStatements() throws Exception {
        String queryId = "registered-scope-" + UUID.randomUUID();
        seedAnchor(queryId);
        try (var connection = dataSource.getConnection()) {
            seedScope(connection, queryId, OWNER);
        }
        long before = cleanupStatements();
        store.markPending(queryId);
        assertThat(cleanupStatements() - before).isEqualTo(2);
        before = cleanupStatements();
        store.markPending(queryId);
        assertThat(cleanupStatements() - before).isEqualTo(2);
        assertThat(store.isPending(queryId)).isTrue();
    }

    @Test
    void rejectsDifferentScopeOwnerWhileTheExecutionAnchorIsLive() throws Exception {
        String queryId = "foreign-scope-" + UUID.randomUUID();
        seedAnchor(queryId);
        try (var connection = dataSource.getConnection()) {
            seedScope(connection, queryId, new ShuffleExecutionOwner(OWNER.ownerId(), "boot-foreign", 8));
        }
        assertThatThrownBy(() -> store.markPending(queryId)).isInstanceOf(SecurityException.class)
                .hasMessageContaining("scope owner mismatch");
        assertThat(store.isPending(queryId)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void waitsForInFlightRegistrationAndValidatesTheScopeItCreates(boolean matchingOwner) throws Exception {
        String queryId = "register-race-" + UUID.randomUUID();
        seedAnchor(queryId);
        CountDownLatch registrationLocked = new CountDownLatch(1);
        CountDownLatch finishRegistration = new CountDownLatch(1);
        var blockerPid = new AtomicInteger();
        var scopeOwner = matchingOwner ? OWNER : new ShuffleExecutionOwner(OWNER.ownerId(), "boot-foreign", 8);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var registration = executor.submit(() -> {
                try (Connection connection = dataSource.getConnection()) {
                    connection.setAutoCommit(false);
                    try (PreparedStatement lock = connection.prepareStatement(
                            "SELECT pg_backend_pid() FROM public.execution_anchor "
                                    + "WHERE execution_id = ? FOR SHARE")) {
                        lock.setString(1, queryId);
                        try (ResultSet rows = lock.executeQuery()) {
                            assertThat(rows.next()).isTrue();
                            blockerPid.set(rows.getInt(1));
                        }
                    }
                    registrationLocked.countDown();
                    assertThat(finishRegistration.await(5, TimeUnit.SECONDS)).isTrue();
                    seedScope(connection, queryId, scopeOwner);
                    connection.commit();
                }
                return null;
            });
            assertThat(registrationLocked.await(5, TimeUnit.SECONDS)).isTrue();
            var terminal = executor.submit(() -> store.markPending(queryId));
            await().atMost(Duration.ofSeconds(5)).until(() -> waitingForAnchor(blockerPid.get()));
            finishRegistration.countDown();
            registration.get(5, TimeUnit.SECONDS);
            if (matchingOwner) {
                terminal.get(5, TimeUnit.SECONDS);
            } else {
                assertThatThrownBy(() -> terminal.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(SecurityException.class).hasRootCauseMessage(
                                "shuffle cleanup intent scope owner mismatch");
            }
            assertThat(store.isPending(queryId)).isEqualTo(matchingOwner);
        } finally {
            finishRegistration.countDown();
            executor.shutdownNow();
        }
    }

    private static boolean waitingForAnchor(int blockerPid) throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("""
                        SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                                       WHERE query LIKE '%public.execution_anchor%FOR UPDATE'
                                         AND ? = ANY(pg_blocking_pids(pid)))
                        """)) {
            statement.setInt(1, blockerPid);
            try (var row = statement.executeQuery()) {
                return row.next() && row.getBoolean(1);
            }
        }
    }

    @Test
    void cleanupRetryDoesNotReinsertCompletedIntent() throws Exception {
        String queryId = "completed-cleanup-" + UUID.randomUUID();
        seedAnchor(queryId);
        store.markPending(queryId);
        AtomicInteger remoteCleanups = new AtomicInteger();
        ShuffleCleanupRecovery recovery = id -> {
            remoteCleanups.incrementAndGet();
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "DELETE FROM shuffle.shuffle_cleanup_intent WHERE query_id = ?")) {
                statement.setString(1, id);
                statement.executeUpdate();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
        CoordinatorTaskTracker tasks = new CoordinatorTaskTracker();
        tasks.openQueryIssuance(queryId);
        QueryOwnershipTracker tracker = new QueryOwnershipTracker(
                tasks, new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                null, new TaskCancelRetrier((request, worker) -> null, tasks));
        tracker.setShuffleCleanup(store, recovery);

        long before = cleanupStatements();
        assertThat(tracker.tryCleanupQuery(queryId)).isTrue();
        assertThat(cleanupStatements() - before).isEqualTo(2);
        before = cleanupStatements();
        assertThat(tracker.tryCleanupQuery(queryId)).isTrue();
        assertThat(cleanupStatements() - before).isEqualTo(1);

        assertThat(store.isPending(queryId)).isFalse();
        assertThat(remoteCleanups.get()).isEqualTo(1);
    }

    @Test
    void keysetScanAdvancesBeyondOneCleanupPage() throws Exception {
        String prefix = "zz-keyset-" + UUID.randomUUID();
        for (int i = 1; i <= 3; i++) {
            String queryId = prefix + "-" + i;
            seedAnchor(queryId);
            store.markPending(queryId);
        }

        var first = store.pendingForOwnerAfter(OWNER.ownerId(), prefix + "-0", 2);
        var second = store.pendingForOwnerAfter(OWNER.ownerId(), first.get(1), 2);

        assertThat(first).containsExactly(prefix + "-1", prefix + "-2");
        assertThat(second).containsExactly(prefix + "-3");
    }

    private static void seedAnchor(String queryId) throws Exception {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO public.execution_anchor "
                                + "(execution_id, owner_id, owner_incarnation, "
                                + "fencing_token, acquired_at, expires_at) "
                                + "VALUES (?, ?, ?, ?, clock_timestamp(), clock_timestamp() + INTERVAL '1 hour')")) {
            statement.setString(1, queryId);
            statement.setString(2, OWNER.ownerId());
            statement.setString(3, OWNER.ownerIncarnation());
            statement.setLong(4, OWNER.fencingToken());
            statement.executeUpdate();
        }
    }

    private static void seedScope(Connection connection, String queryId, ShuffleExecutionOwner owner)
            throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO shuffle.shuffle_query_scope "
                        + "(query_id, owner_id, owner_incarnation, "
                        + "owner_fencing_token, cleanup_expires_at_ms) VALUES (?, ?, ?, ?, ?)")) {
            insert.setString(1, queryId);
            insert.setString(2, owner.ownerId());
            insert.setString(3, owner.ownerIncarnation());
            insert.setLong(4, owner.fencingToken());
            insert.setLong(5, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
            insert.executeUpdate();
        }
    }

    private static long cleanupStatements() throws Exception {
        String sql = """
                SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements
                WHERE toplevel AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query NOT LIKE '%pg_stat_statements%'
                  AND (query LIKE '%public.execution_anchor%' OR query LIKE '%shuffle.shuffle_query_scope%'
                       OR query LIKE '%shuffle.shuffle_cleanup_intent%')
                """;
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery(sql)) {
            if (!row.next()) throw new IllegalStateException("No PostgreSQL cleanup statement count");
            return row.getLong(1);
        }
    }
}
