package org.pangrova.query.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.query.testinfra.SharedPostgresContainer;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Real pooled PostgreSQL sessions cover cached ranges and absent-row acquisition races against
 * the final-shape query baseline. The historical V23→V78 migration-replay cases were removed
 * with the legacy migration chain (clean-cut); the baseline already carries the final form
 * (token sequence CACHE 1, first issued token 2).
 */
class JdbcExecutionAnchorOrderingTest {
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final long INSERT_GATE = 789123456L;

    @Test
    void pooledOwnersAdvanceTokensForTakeoverBatchClaimAndReusedIdentifiers() throws Exception {
        var source = (PGSimpleDataSource) SharedPostgresContainer.migratedDataSource("anchor_ordered_pool");
        try (var firstPool = pool(source); var secondPool = pool(source)) {
            var first = new JdbcExecutionAnchorStore(firstPool);
            var second = new JdbcExecutionAnchorStore(secondPool);
            var unrelated = first.tryAcquire("other-id", "a", "a-1", LEASE).orElseThrow();
            var previous = second.tryAcquire("same-id", "b", "b-1", LEASE).orElseThrow();
            assertThat(previous.fencingToken()).isGreaterThan(unrelated.fencingToken());
            assertThat(second.yield(previous)).isTrue();
            var takeover = first.tryAcquire("same-id", "a", "a-1", LEASE).orElseThrow();
            assertThat(takeover.fencingToken()).isGreaterThan(previous.fencingToken());
            assertThat(first.yield(takeover)).isTrue();
            var claimed = second.claimExpired("b", "b-2", LEASE, 1);
            assertThat(claimed).hasSize(1);
            var batch = claimed.get(0);
            assertThat(batch.executionId()).isEqualTo("same-id");
            assertThat(batch.fencingToken()).isGreaterThan(takeover.fencingToken());
            assertThat(second.release(batch)).isTrue();
            var reused = first.tryAcquire("same-id", "a", "a-2", LEASE).orElseThrow();
            assertThat(reused.fencingToken()).isGreaterThan(batch.fencingToken());
            assertThat(first.release(previous)).isFalse();
            assertThat(second.release(batch)).isFalse();
        }
    }

    @Test
    void absentIdentifierIsSerializedBeforeAnyTokenCanBeIssued() throws Exception {
        var source = (PGSimpleDataSource) SharedPostgresContainer.migratedDataSource("anchor_absent_order");
        execute(source, """
                CREATE FUNCTION hold_first_anchor_insert() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF current_setting('pangrova.test_anchor_gate',true)='paused'
                            AND NEW.execution_id='same-id' THEN
                        PERFORM pg_advisory_xact_lock(789123456::bigint);
                    END IF;
                    RETURN NEW;
                END;
                $$;
                CREATE TRIGGER hold_anchor_insert BEFORE INSERT ON execution_anchor
                    FOR EACH ROW EXECUTE FUNCTION hold_first_anchor_insert();
                """);
        var executor = Executors.newFixedThreadPool(2);
        try (var firstPool = pool(source); var secondPool = pool(source); var gate = source.getConnection()) {
            long firstPid;
            long secondPid;
            long relationOid;
            try (var connection = firstPool.getConnection()) {
                execute(connection, "SET pangrova.test_anchor_gate='paused'");
                firstPid = scalar(connection, "SELECT pg_backend_pid()");
                relationOid = scalar(connection, "SELECT 'execution_anchor'::regclass::oid::bigint");
                assertThat(scalar(connection, "SELECT (current_schema()='pg_catalog')::int")).isZero();
            }
            try (var connection = secondPool.getConnection()) {
                execute(connection, "SELECT set_config('search_path', 'pg_catalog,' || current_schema(), false)");
                assertThat(scalar(connection, "SELECT (current_schema()='pg_catalog')::int")).isEqualTo(1);
                assertThat(scalar(connection, "SELECT 'execution_anchor'::regclass::oid::bigint"))
                        .isEqualTo(relationOid);
                secondPid = scalar(connection, "SELECT pg_backend_pid()");
            }
            var first = new JdbcExecutionAnchorStore(firstPool);
            var second = new JdbcExecutionAnchorStore(secondPool);
            gate.setAutoCommit(false);
            execute(gate, "SELECT pg_advisory_xact_lock(" + INSERT_GATE + ")");
            var acquiring = executor.submit(() -> first.tryAcquire("same-id", "a", "a-1", LEASE));
            try {
                long gatePid = scalar(gate, "SELECT pg_backend_pid()");
                await().atMost(Duration.ofSeconds(10)).until(() -> blockedBy(source, gatePid));
                var competing = executor.submit(() -> second.tryAcquire("same-id", "b", "b-1", LEASE));
                await().atMost(Duration.ofSeconds(10)).until(() -> competing.isDone()
                        || blockedBy(source, secondPid, firstPid));
                assertThat(competing.isDone()).as("same-ID admission must wait before token allocation").isFalse();
                gate.commit();
                var admitted = acquiring.get(10, TimeUnit.SECONDS).orElseThrow();
                assertThat(competing.get(10, TimeUnit.SECONDS)).isEmpty();
                assertThat(first.release(admitted)).isTrue();
                var replacement = second.tryAcquire("same-id", "b", "b-2", LEASE).orElseThrow();
                assertThat(replacement.fencingToken()).isGreaterThan(admitted.fencingToken());
            } finally {
                gate.rollback();
            }
        } finally {
            ExecutorDrain.shutdownAndAwait(executor, 20_000);
        }
    }

    private static HikariDataSource pool(PGSimpleDataSource source) {
        var config = new com.zaxxer.hikari.HikariConfig();
        config.setJdbcUrl(source.getURL());
        config.setUsername(source.getUser());
        config.setPassword(source.getPassword());
        config.setPoolName("pangrova-query-anchor-test-pool");
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(0);
        return new HikariDataSource(config);
    }

    private static long scalar(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private static void execute(DataSource source, String sql) throws Exception {
        try (var connection = source.getConnection()) {
            execute(connection, sql);
        }
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static boolean blockedBy(DataSource source, long blocker) throws Exception {
        try (var connection = source.getConnection()) {
            return scalar(connection, "SELECT count(*) FROM pg_stat_activity WHERE "
                    + blocker + "=ANY(pg_blocking_pids(pid))") > 0;
        }
    }

    private static boolean blockedBy(DataSource source, long waiter, long blocker) throws Exception {
        try (var connection = source.getConnection()) {
            return scalar(connection, "SELECT count(*) FROM pg_stat_activity WHERE pid=" + waiter
                    + " AND " + blocker + "=ANY(pg_blocking_pids(pid))") == 1;
        }
    }
}
