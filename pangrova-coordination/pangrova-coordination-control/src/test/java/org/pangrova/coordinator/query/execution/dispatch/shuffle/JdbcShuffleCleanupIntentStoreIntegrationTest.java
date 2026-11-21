package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

class JdbcShuffleCleanupIntentStoreIntegrationTest {
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine");
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
        store.markPending(queryId);
        assertThat(store.isPending(queryId)).isTrue();
        assertThat(store.pendingForOwner(OWNER.ownerId(), 100)).contains(queryId);
    }

    @Test
    void waitsForInFlightRegistrationBeforeRecordingTerminalIntent() throws Exception {
        String queryId = "register-race-" + UUID.randomUUID();
        seedAnchor(queryId);
        CountDownLatch registrationLocked = new CountDownLatch(1);
        CountDownLatch finishRegistration = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var registration = executor.submit(() -> {
                try (Connection connection = dataSource.getConnection()) {
                    connection.setAutoCommit(false);
                    try (PreparedStatement lock = connection.prepareStatement(
                            "SELECT 1 FROM public.execution_anchor "
                                    + "WHERE execution_id = ? FOR SHARE")) {
                        lock.setString(1, queryId);
                        try (ResultSet rows = lock.executeQuery()) {
                            assertThat(rows.next()).isTrue();
                        }
                    }
                    registrationLocked.countDown();
                    assertThat(finishRegistration.await(5, TimeUnit.SECONDS)).isTrue();
                    try (PreparedStatement insert = connection.prepareStatement(
                            "INSERT INTO shuffle.shuffle_query_scope "
                                    + "(query_id, owner_id, owner_incarnation, "
                                    + "owner_fencing_token, cleanup_expires_at_ms) VALUES (?, ?, ?, ?, ?)")) {
                        insert.setString(1, queryId);
                        insert.setString(2, OWNER.ownerId());
                        insert.setString(3, OWNER.ownerIncarnation());
                        insert.setLong(4, OWNER.fencingToken());
                        insert.setLong(5, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
                        insert.executeUpdate();
                    }
                    connection.commit();
                }
                return null;
            });
            assertThat(registrationLocked.await(5, TimeUnit.SECONDS)).isTrue();
            var terminal = executor.submit(() -> store.markPending(queryId));
            assertThat(terminal.isDone()).isFalse();
            finishRegistration.countDown();
            registration.get(5, TimeUnit.SECONDS);
            terminal.get(5, TimeUnit.SECONDS);
            assertThat(store.isPending(queryId)).isTrue();
        } finally {
            finishRegistration.countDown();
            executor.shutdownNow();
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
                null, null, new TaskCancelRetrier((request, worker) -> null, tasks));
        tracker.setShuffleCleanup(store, recovery);

        tracker.cleanupQuery(queryId);
        tracker.cleanupQuery(queryId);

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
}
