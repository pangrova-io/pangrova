package org.pangrova.indexing.store.jdbc;

import org.pangrova.generation.api.execution.GenerationExecutionOwnerLostException;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.spi.query.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PostgreSQL LockRows timing with actual leases and a blocker that never updates the anchor. */
class GenerationExecutionFenceLockIntegrationTest {
    @Test
    void lockOnlyWaitCannotReturnOwnerAfterExecutionDeadline() throws Exception {
        var fixture = GenerationPublicationFenceLockIntegrationTest.Fixture.create(
                GenerationPublicationFenceLockIntegrationTest.Kind.INDEX);
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = fixture.dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            lockAnchor(blocker, fixture.anchor);
            var fence = executor.submit(() -> requireCurrentOnTransaction(fixture));
            awaitBlockedBeforeExpiry(fixture.dataSource, backendPid(blocker), fixture.anchor);
            waitUntilExpired(blocker, fixture.anchor);
            blocker.commit();
            assertThatThrownBy(() -> fence.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(GenerationExecutionOwnerLostException.class);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void postgresTargetProjectionRetainsPreLockLiveValueWithoutTupleUpdate() throws Exception {
        var fixture = GenerationPublicationFenceLockIntegrationTest.Fixture.create(
                GenerationPublicationFenceLockIntegrationTest.Kind.INDEX);
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = fixture.dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            lockAnchor(blocker, fixture.anchor);
            var projected = executor.submit(() -> {
                try (var connection = fixture.dataSource.getConnection()) {
                    connection.setAutoCommit(false);
                    try (var statement = connection.prepareStatement(
                            "SELECT expires_at>clock_timestamp() AS live FROM execution_anchor "
                                    + "WHERE execution_id=? FOR SHARE")) {
                        statement.setString(1, fixture.anchor.executionId());
                        try (var row = statement.executeQuery()) {
                            assertThat(row.next()).isTrue();
                            boolean liveBeforeLock = row.getBoolean(1);
                            try (var clock = connection.createStatement();
                                 var now = clock.executeQuery("SELECT clock_timestamp()")) {
                                assertThat(now.next()).isTrue();
                                assertThat(now.getTimestamp(1).toInstant()).isAfter(fixture.anchor.expiresAt());
                            }
                            connection.rollback();
                            return liveBeforeLock;
                        }
                    }
                }
            });
            awaitBlockedBeforeExpiry(fixture.dataSource, backendPid(blocker), fixture.anchor);
            waitUntilExpired(blocker, fixture.anchor);
            blocker.commit();
            // Actual executor behavior is the independent oracle for the former production SQL bug.
            assertThat(projected.get(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void lockOnlyWaitReturnsExactOwnerWhenDeadlineHasNotPassed() throws Exception {
        var fixture = GenerationPublicationFenceLockIntegrationTest.Fixture.create(
                GenerationPublicationFenceLockIntegrationTest.Kind.INDEX);
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = fixture.dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            lockAnchor(blocker, fixture.anchor);
            var fence = executor.submit(() -> requireCurrentOnTransaction(fixture));
            awaitBlockedBeforeExpiry(fixture.dataSource, backendPid(blocker), fixture.anchor);
            blocker.commit();
            assertThat(fence.get(10, TimeUnit.SECONDS)).isEqualTo(fixture.anchor);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void currentOwnerStillRequiresExactIncarnationAndDurableTenantJobRoute() throws Exception {
        var fixture = GenerationPublicationFenceLockIntegrationTest.Fixture.create(
                GenerationPublicationFenceLockIntegrationTest.Kind.INDEX);
        try (var connection = fixture.dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> org.pangrova.metadata.assembly.generation.GenerationStores.executionFence().requireCurrent(connection, "tenant", "job",
                    new GenerationExecutionOwner(fixture.anchor.ownerId(), "previous-process", fixture.anchor.fencingToken())))
                    .isInstanceOf(GenerationExecutionOwnerLostException.class);
            assertThatThrownBy(() -> org.pangrova.metadata.assembly.generation.GenerationStores.executionFence().requireCurrent(connection, "other-tenant", "job",
                    GenerationExecutionOwner.from(fixture.anchor)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("exact durable routing");
            connection.rollback();
        }
    }

    private static ExecutionAnchor requireCurrentOnTransaction(GenerationPublicationFenceLockIntegrationTest.Fixture fixture)
            throws Exception {
        try (var connection = fixture.dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("SET LOCAL statement_timeout='10s'");
            }
            try {
                var actual = org.pangrova.metadata.assembly.generation.GenerationStores.executionFence().requireCurrent(connection, "tenant", "job",
                        GenerationExecutionOwner.from(fixture.anchor));
                connection.commit();
                return actual;
            } catch (Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    private static void lockAnchor(Connection connection, ExecutionAnchor anchor) throws Exception {
        try (var statement = connection.prepareStatement(
                "SELECT execution_id FROM execution_anchor WHERE execution_id=? FOR UPDATE")) {
            statement.setString(1, anchor.executionId());
            try (var row = statement.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.next()).isFalse();
            }
        }
    }

    static int backendPid(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var row = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(row.next()).isTrue();
            return row.getInt(1);
        }
    }

    static Instant dbNow(DataSource dataSource) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var row = statement.executeQuery("SELECT clock_timestamp()")) {
            assertThat(row.next()).isTrue();
            return row.getTimestamp(1).toInstant();
        }
    }

    static void awaitBlockedBeforeExpiry(DataSource dataSource, int blockerPid, ExecutionAnchor anchor) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            try (var observer = dataSource.getConnection(); var statement = observer.prepareStatement(
                    "SELECT xact_start,clock_timestamp() FROM pg_stat_activity "
                            + "WHERE ?=ANY(pg_blocking_pids(pid))")) {
                statement.setInt(1, blockerPid);
                try (var row = statement.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    assertThat(row.getTimestamp(1).toInstant()).isBefore(anchor.expiresAt());
                    assertThat(row.getTimestamp(2).toInstant()).isBefore(anchor.expiresAt());
                    assertThat(row.next()).isFalse();
                }
            }
        });
    }

    static void waitUntilExpired(Connection blocker, ExecutionAnchor anchor) throws Exception {
        try (var statement = blocker.prepareStatement(
                "SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (?::timestamptz-clock_timestamp())))+0.05)")) {
            statement.setObject(1, anchor.expiresAt().atOffset(java.time.ZoneOffset.UTC));
            statement.execute();
        }
        try (var statement = blocker.createStatement(); var row = statement.executeQuery("SELECT clock_timestamp()")) {
            assertThat(row.next()).isTrue();
            assertThat(row.getTimestamp(1).toInstant()).isAfter(anchor.expiresAt());
        }
    }
}
