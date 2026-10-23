package org.pangrova.metadata.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.metadata.store.PooledDataSourceFactory;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.postgresql.ds.PGSimpleDataSource;

/** Real pooled PostgreSQL sessions cover cached ranges, upgrades and absent-row acquisition races. */
class JdbcExecutionAnchorOrderingTest {
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final long INSERT_GATE = 789123456L;

    @TempDir
    private Path migrationDirectory;

    @Test
    void migrationInvalidatesBothOldSessionCachesWithoutReplacingSequenceIdentity() throws Exception {
        var source = legacySource();
        try (var first = source.getConnection(); var second = source.getConnection()) {
            long firstPid = scalar(first, "SELECT pg_backend_pid()");
            long secondPid = scalar(second, "SELECT pg_backend_pid()");
            long initial = next(first);
            long higher = next(second);
            assertThat(next(first)).isLessThan(higher).isGreaterThan(initial);
            long reserved = scalar(first, "SELECT last_value FROM execution_anchor_token_seq");
            long oid = scalar(first, "SELECT 'execution_anchor_token_seq'::regclass::oid::bigint");

            migrate(source, "78");

            assertThat(scalar(first, "SELECT pg_backend_pid()")).isEqualTo(firstPid);
            assertThat(scalar(second, "SELECT pg_backend_pid()")).isEqualTo(secondPid);
            assertThat(scalar(first, "SELECT 'execution_anchor_token_seq'::regclass::oid::bigint")).isEqualTo(oid);
            assertThat(scalar(first, "SELECT seqcache FROM pg_sequence "
                    + "WHERE seqrelid='execution_anchor_token_seq'::regclass")).isEqualTo(1);
            long afterFirst = next(first);
            long afterSecond = next(second);
            assertThat(afterFirst).isGreaterThan(reserved);
            assertThat(afterSecond).isGreaterThan(afterFirst);
            assertThat(next(first)).isGreaterThan(afterSecond);
        }
    }

    @Test
    void migrationWaitsForAnInFlightAllocationAndRestartsAboveItsReservedRange() throws Exception {
        var source = legacySource();
        var executor = Executors.newSingleThreadExecutor();
        try (var previous = source.getConnection()) {
            previous.setAutoCommit(false);
            long token = next(previous);
            long reserved = scalar(previous, "SELECT last_value FROM execution_anchor_token_seq");
            execute(previous, "INSERT INTO execution_anchor VALUES ('upgrade-owner','old','old-1',"
                    + token + ",clock_timestamp(),clock_timestamp()+INTERVAL '1 minute',1)");
            long previousPid = scalar(previous, "SELECT pg_backend_pid()");
            var migration = executor.submit(() -> migrate(source, "78"));
            try {
                await().atMost(Duration.ofSeconds(10)).until(() -> blockedBy(source, previousPid));
                assertThat(migration.isDone()).isFalse();
            } finally {
                previous.commit();
            }
            migration.get(15, TimeUnit.SECONDS);
            assertThat(next(previous)).isGreaterThan(reserved);
            assertThat(scalar(previous, "SELECT fencing_token FROM execution_anchor "
                    + "WHERE execution_id='upgrade-owner'")).isEqualTo(token);
        } finally {
            ExecutorDrain.shutdownAndAwait(executor, 20_000);
        }
    }

    @Test
    void exhaustedAllocatorCannotRestartAtAnOlderValue() throws Exception {
        var source = legacySource();
        execute(source, "SELECT setval('execution_anchor_token_seq',9223372036854775807,true)");

        assertThatThrownBy(() -> migrate(source, "78"))
                .hasStackTraceContaining("Execution fencing token space is exhausted");

        try (var connection = source.getConnection()) {
            assertThat(scalar(connection, "SELECT last_value FROM execution_anchor_token_seq"))
                    .isEqualTo(Long.MAX_VALUE);
            assertThat(scalar(connection, "SELECT seqcache FROM pg_sequence "
                    + "WHERE seqrelid='execution_anchor_token_seq'::regclass")).isEqualTo(1000);
        }
    }

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

    private PGSimpleDataSource legacySource() throws Exception {
        for (String resource : java.util.List.of("V23__durable_execution_anchors.sql",
                "V78__ordered_execution_fencing_tokens.sql")) {
            try (var input = getClass().getResourceAsStream("/db/migration/" + resource)) {
                assertThat(input).isNotNull();
                Files.copy(input, migrationDirectory.resolve(resource));
            }
        }
        var source = (PGSimpleDataSource) SharedPostgresContainer.isolatedDataSource("anchor_order_upgrade");
        migrate(source, "23");
        return source;
    }

    private void migrate(DataSource source, String version) {
        Flyway.configure().dataSource(source).locations("filesystem:" + migrationDirectory)
                .target(version).load().migrate();
    }

    private static HikariDataSource pool(PGSimpleDataSource source) {
        return PooledDataSourceFactory.create(source.getURL(), source.getUser(), source.getPassword(), 1);
    }

    private static long next(Connection connection) throws Exception {
        return scalar(connection, "SELECT nextval('execution_anchor_token_seq')");
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
