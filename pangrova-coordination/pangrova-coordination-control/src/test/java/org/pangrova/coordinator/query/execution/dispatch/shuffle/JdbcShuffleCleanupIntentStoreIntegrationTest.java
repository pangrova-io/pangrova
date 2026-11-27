package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.OffsetDateTime;
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
import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.persistence.query.postgresql.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import java.time.Instant;
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
        store = new JdbcShuffleCleanupIntentStore(dataSource);
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
        store.markPending(grant(queryId));
        assertThat(cleanupStatements() - before).isEqualTo(2);
        assertThat(hasIntent(queryId)).isTrue();
    }

    @Test
    void validatesRegisteredScopeAndRecordsOrReplaysIntentInTwoStatements() throws Exception {
        String queryId = "registered-scope-" + UUID.randomUUID();
        seedAnchor(queryId);
        try (var connection = dataSource.getConnection()) {
            seedScope(connection, queryId, OWNER);
        }
        long before = cleanupStatements();
        store.markPending(grant(queryId));
        assertThat(cleanupStatements() - before).isEqualTo(2);
        before = cleanupStatements();
        store.markPending(grant(queryId));
        assertThat(cleanupStatements() - before).isEqualTo(2);
        assertThat(hasIntent(queryId)).isTrue();
    }

    @Test
    void rejectsDifferentScopeOwnerWhileTheExecutionAnchorIsLive() throws Exception {
        String queryId = "foreign-scope-" + UUID.randomUUID();
        seedAnchor(queryId);
        try (var connection = dataSource.getConnection()) {
            seedScope(connection, queryId, new ShuffleExecutionOwner(OWNER.ownerId(), "boot-foreign", 8));
        }
        assertThatThrownBy(() -> store.markPending(grant(queryId))).isInstanceOf(SecurityException.class)
                .hasMessageContaining("scope owner mismatch");
        assertThat(hasIntent(queryId)).isFalse();
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
            var terminal = executor.submit(() -> store.markPending(grant(queryId)));
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
            assertThat(hasIntent(queryId)).isEqualTo(matchingOwner);
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
    void emptyCleanupPreservesItsSealAndReplaysTheOriginalConvergenceTime() throws Exception {
        String queryId = "empty-cleanup-" + UUID.randomUUID();
        seedAnchor(queryId);
        store.markPending(grant(queryId));
        long before = cleanupStatements();

        assertThat(store.completeWithoutScope(grant(queryId))).isTrue();
        assertThat(cleanupStatements() - before).isEqualTo(2);
        var converged = convergenceTime(queryId);
        assertThat(converged).isNotNull();
        assertThat(store.completeWithoutScope(grant(queryId))).isTrue();
        assertThat(convergenceTime(queryId)).isEqualTo(converged);
    }

    @Test
    void untrackedDurableScopeStillRequiresTheRemoteCleanupProtocol() throws Exception {
        String queryId = "untracked-cleanup-" + UUID.randomUUID();
        seedAnchor(queryId);
        try (var connection = dataSource.getConnection()) {
            seedScope(connection, queryId, OWNER);
        }
        store.markPending(grant(queryId));
        assertThat(store.completeWithoutScope(grant(queryId))).isFalse();
        assertThat(hasIntent(queryId)).isTrue();
    }

    @Test
    void staleTerminalAndCleanupCallsCannotMutateTheSuccessorIntent() throws Exception {
        String queryId = "stale-cleanup-" + UUID.randomUUID();
        seedAnchor(queryId);
        var stale = new ExecutionAnchor(queryId, OWNER.ownerId(), "predecessor", 6,
                Instant.EPOCH, Instant.EPOCH.plusSeconds(1));
        store.markPending(stale);
        assertThat(hasIntent(queryId)).isFalse();

        store.markPending(grant(queryId));
        setDrainState(queryId, 1);
        assertThat(store.completeWithoutScope(stale)).isTrue();
        assertThat(hasIntent(queryId)).isTrue();
        setDrainState(queryId, 2);
        assertThat(store.prepareProvenRecovery(grant(queryId))).isFalse();
        assertThat(convergenceTime(queryId)).isNotNull();
    }

    @Test
    void onlyTheProvenSuccessorCanAdoptAPredecessorScopeWithoutAnIntent() throws Exception {
        String queryId = "drained-recovery-" + UUID.randomUUID();
        seedAnchor(queryId);
        try (var connection = dataSource.getConnection()) {
            seedScope(connection, queryId, new ShuffleExecutionOwner("predecessor", "older-boot", 6));
        }
        setDrainState(queryId, 1);
        assertThatThrownBy(() -> store.prepareProvenRecovery(grant(queryId)))
                .isInstanceOf(SecurityException.class).hasMessageContaining("predecessor exit proof");
        assertThat(hasIntent(queryId)).isFalse();
        setDrainState(queryId, 2);
        assertThat(store.prepareProvenRecovery(grant(queryId))).isTrue();
        assertThat(hasIntent(queryId)).isTrue();
    }

    @Test
    void recoveredRetirementRequiresPositiveConvergenceAndReapsItWithTheAnchor() throws Exception {
        String queryId = "cleanup-retirement-" + UUID.randomUUID();
        seedAnchor(queryId);
        var owner = grant(queryId);
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        setDrainState(queryId, 2);
        assertThat(anchors.releaseDrainedQuery(owner, true)).isFalse();
        assertThat(anchors.current(queryId)).isPresent();
        assertThat(store.prepareProvenRecovery(owner)).isFalse();
        assertThat(convergenceTime(queryId)).isNotNull();
        assertThat(anchors.releaseDrainedQuery(owner, true)).isTrue();
        assertThat(anchors.current(queryId)).isEmpty();
        assertThat(hasIntent(queryId)).isFalse();
    }

    private static OffsetDateTime convergenceTime(String queryId) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT converged_at FROM shuffle.shuffle_cleanup_intent WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getObject(1, OffsetDateTime.class);
            }
        }
    }

    private static ExecutionAnchor grant(String queryId) {
        return new ExecutionAnchor(queryId, OWNER.ownerId(), OWNER.ownerIncarnation(), OWNER.fencingToken(),
                Instant.EPOCH, Instant.EPOCH.plusSeconds(1));
    }

    private static boolean hasIntent(String queryId) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT 1 FROM shuffle.shuffle_cleanup_intent WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static void setDrainState(String queryId, int state) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE public.execution_anchor SET recovery_drain_state = ? WHERE execution_id = ?")) {
            statement.setInt(1, state);
            statement.setString(2, queryId);
            assertThat(statement.executeUpdate()).isOne();
        }
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
