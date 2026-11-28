package org.pangrova.persistence.coordination.postgresql.membership;

import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgres;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import java.util.stream.Stream;
import static org.assertj.core.api.Assertions.catchThrowable;
import java.sql.SQLException;
import org.pangrova.coordination.api.membership.WorkerSnapshot;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcWorkerRegistryPostgresTest extends ContainerTestBase {

    private static final String CANONICAL_INVENTORY =
            "CjIIAhogRdVbleB2e7KyN0ENsyhXMAxZ2xaezSU0lUC4Od3Gd8giDGFpLW5hdGl2ZS1y"
                    + "MigBMhZtZXRhZGF0YS1yZWdpc3RyeS10ZXN0";

    @Test
    void preservesWorkerCapabilitiesAndResultDataPlaneEndpointAcrossRecovery() throws Exception {
        var dataSource = MigratedPostgres.create();
        JdbcWorkerRegistry registry = new JdbcWorkerRegistry(dataSource);
        String inventory = CANONICAL_INVENTORY;
        long now = System.currentTimeMillis();
        WorkerSnapshot worker = new WorkerSnapshot(
                "worker-1", "host-1", 8080, 9090, 2,
                "rack-a", List.of("s3://bucket"),
                "NONE", 0.1, 1, 1024L, now, now,
                9191, null, inventory, 9443,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1",
                new org.pangrova.coordination.api.membership.WorkerShuffleDeployment("REMOTE", "pangrova"), true);

        registry.register(worker);
        WorkerSnapshot recovered = registry.getWorker("worker-1").orElseThrow();

        assertThat(recovered.shuffleDeployment()).isEqualTo(worker.shuffleDeployment());
        assertThat(recovered.cdcIngestSupported()).isTrue();
        var heartbeat = new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot(
                "NONE", 0.2, 2, 1024L, null);
        assertThat(registry.heartbeat(worker.registrationIdentity(), heartbeat).orElseThrow().cdcIngestSupported())
                .isTrue();
        assertThat(recovered.resultServerPort()).isEqualTo(9191);
        assertThat(recovered.copyTaskTlsPort()).isEqualTo(9443);
        assertThat(recovered.capabilityInventory()).isEqualTo(inventory);
        assertThat(recovered.admissionDomainId()).isEqualTo(worker.admissionDomainId());
        assertThat(recovered.incarnationId()).isEqualTo(worker.incarnationId());

        try (var connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet columns = statement.executeQuery("""
                     SELECT column_name, is_nullable
                     FROM information_schema.columns
                     WHERE table_schema = current_schema()
                       AND table_name = 'worker_registry'
                       AND column_name IN ('capabilities', 'capability_inventory_proto')
                     ORDER BY column_name
                     """)) {
            assertThat(columns.next()).isTrue();
            assertThat(columns.getString("column_name"))
                    .isEqualTo("capability_inventory_proto");
            assertThat(columns.getString("is_nullable")).isEqualTo("NO");
            assertThat(columns.next()).isFalse();
        }
    }

    @Test
    void unidentifiedRegistrationStaysUndispatchableUntilTheWorkerRegistersItsIdentity() throws Exception {
        var dataSource = SharedPostgres.isolatedDataSource();
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target("78").load().migrate();
        long now = System.currentTimeMillis();
        try (var connection = dataSource.getConnection();
                var insert = connection.prepareStatement("""
                        INSERT INTO worker_registry (
                            worker_id, host, http_port, transport_port, protocol_version,
                            capability_inventory_proto, last_heartbeat_ms, registered_at_ms)
                        VALUES ('unidentified-worker', 'host-1', 8080, 9090, 2, ?, ?, ?)
                        """)) {
            insert.setString(1, CANONICAL_INVENTORY);
            insert.setLong(2, now);
            insert.setLong(3, now);
            insert.executeUpdate();
        }
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        var registry = new JdbcWorkerRegistry(dataSource);
        assertThat(registry.getWorker("unidentified-worker")).isEmpty();
        assertThat(registry.getActiveWorkers(java.time.Duration.ofMinutes(5))).isEmpty();
        registry.heartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity("unidentified-worker", "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1"), new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot(
                "NONE", 0.0, 0, 1024L, null));
        assertThat(registry.getWorker("unidentified-worker")).isEmpty();

        var identity = new WorkerSnapshot("unidentified-worker", "host-1", 8080, 9090, 2,
                null, List.of(), "NONE", 0.0, 0, 1024L, now, now, 9191, null, CANONICAL_INVENTORY,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
        registry.register(identity);
        assertThat(registry.getWorker("unidentified-worker")).contains(identity);
        assertThat(registry.getActiveWorkers(java.time.Duration.ofMinutes(5))).containsExactly(identity);
    }

    @Test
    void migrationEvictsLegacyRowsRatherThanInventingInventory() throws Exception {
        var dataSource = SharedPostgres.isolatedDataSource();
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .target("29")
                .load()
                .migrate();
        try (var connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    INSERT INTO worker_registry (
                        worker_id, host, http_port, transport_port,
                        protocol_version, capabilities,
                        last_heartbeat_ms, registered_at_ms)
                    VALUES ('legacy-worker', 'legacy-host', 8080, 9090,
                            1, '["legacy-string-capability"]', 1, 1)
                    """);
        }

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        assertThat(new JdbcWorkerRegistry(dataSource).getWorker("legacy-worker"))
                .isEmpty();
    }
    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedInsertLeavesNoDispatchableWorker(Throwable failure, SQLException rollbackFailure) throws Exception {
        assertFailedRegistration(failure, rollbackFailure, false);
    }

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedUpdatePreservesRegisteredWorkerIdentity(Throwable failure, SQLException rollbackFailure) throws Exception {
        assertFailedRegistration(failure, rollbackFailure, true);
    }

    private static void assertFailedRegistration(Throwable failure, SQLException rollbackFailure,
            boolean update) throws Exception {
        var dataSource = MigratedPostgres.create();
        var registry = new JdbcWorkerRegistry(dataSource);
        long now = System.currentTimeMillis();
        var original = registrationWorker("original", "00000000-0000-0000-0000-0000000000e1", now);
        if (update) registry.register(original);
        var before = registry.getWorker("transaction-worker");
        var replacement = registrationWorker("replacement", "00000000-0000-0000-0000-0000000000e2", now);
        var failingRegistry = new JdbcWorkerRegistry(JdbcMutationFault.afterMutation(dataSource,
                update ? "UPDATE worker_registry" : "INSERT INTO worker_registry", failure, rollbackFailure));
        Throwable caught = catchThrowable(() -> failingRegistry.register(replacement));
        assertThat(registry.getWorker("transaction-worker")).isEqualTo(before);
        assertFailure(caught, failure, rollbackFailure);
        registry.register(replacement);
        assertThat(registry.getWorker("transaction-worker")).contains(replacement);
    }

    private static WorkerSnapshot registrationWorker(String host, String incarnation, long now) {
        return new WorkerSnapshot("transaction-worker", host, 8080, 9090, 2,
                "rack-a", List.of("s3://bucket"), "NONE", 0.1, 1, 1024L, now, now,
                9191, null, CANONICAL_INVENTORY, 9443,
                "00000000-0000-0000-0000-0000000000d1", incarnation);
    }

    private static Stream<Arguments> transactionFailures() {
        return Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after mutation"), new IllegalStateException("after mutation"),
                new AssertionError("after mutation")).map(failure -> Arguments.of(failure,
                        failRollback ? new SQLException("rollback unavailable") : null)));
    }

    private static void assertFailure(Throwable caught, Throwable failure, SQLException rollbackFailure) {
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(RuntimeException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
    }


    @Test
    void staleWithdrawalCannotDeleteReplacementOrDifferentAdmissionDomain() throws Exception {
        var registry = new JdbcWorkerRegistry(MigratedPostgres.create());
        long now = System.currentTimeMillis();
        var original = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", now);
        registry.register(original);
        var replacement = removalWorker(original.admissionDomainId(),
                "00000000-0000-0000-0000-0000000000e2", now);
        registry.register(replacement);
        assertThat(registry.unregister(original.registrationIdentity())).isFalse();
        assertThat(registry.getWorker(original.workerId())).contains(replacement);
        var moved = removalWorker("00000000-0000-0000-0000-0000000000d2",
                replacement.incarnationId(), now);
        registry.register(moved);
        assertThat(registry.unregister(replacement.registrationIdentity())).isFalse();
        assertThat(registry.getWorker(original.workerId())).contains(moved);
        assertThat(registry.unregister(moved.registrationIdentity())).isTrue();
        assertThat(registry.unregister(moved.registrationIdentity())).isFalse();
    }

    @Test
    void heartbeatMetricsDoNotChangeRegistrationIdentity() throws Exception {
        var registry = new JdbcWorkerRegistry(MigratedPostgres.create());
        var original = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", System.currentTimeMillis());
        registry.register(original);
        registry.heartbeat(original.registrationIdentity(),
                new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot("HIGH", 0.9, 3, 123L, null));
        assertThat(registry.getWorker(original.workerId()).orElseThrow().inFlightTaskCount()).isEqualTo(3);
        assertThat(registry.unregister(original.registrationIdentity())).isTrue();
    }

    @Test
    void blockedDeleteRechecksIncarnationAfterConcurrentReplacementCommits() throws Exception {
        var dataSource = MigratedPostgres.create();
        var registry = new JdbcWorkerRegistry(dataSource);
        var original = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", System.currentTimeMillis());
        registry.register(original);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var replacing = dataSource.getConnection()) {
            replacing.setAutoCommit(false);
            int blocker;
            try (var statement = replacing.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                result.next();
                blocker = result.getInt(1);
            }
            String nextIncarnation = "00000000-0000-0000-0000-0000000000e2";
            try (var update = replacing.prepareStatement("UPDATE worker_registry SET incarnation_id=? WHERE worker_id=?")) {
                update.setString(1, nextIncarnation);
                update.setString(2, original.workerId());
                assertThat(update.executeUpdate()).isEqualTo(1);
            }
            var deletion = executor.submit(() -> registry.unregister(original.registrationIdentity()));
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(() -> {
                try (var connection = dataSource.getConnection();
                     var query = connection.prepareStatement("SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                             + "WHERE wait_event_type='Lock' AND ?=ANY(pg_blocking_pids(pid)))")) {
                    query.setInt(1, blocker);
                    try (var result = query.executeQuery()) {
                        result.next();
                        return result.getBoolean(1);
                    }
                }
            });
            replacing.commit();
            assertThat(deletion.get(10, java.util.concurrent.TimeUnit.SECONDS)).isFalse();
            assertThat(registry.getWorker(original.workerId()).orElseThrow().incarnationId()).isEqualTo(nextIncarnation);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private static WorkerSnapshot removalWorker(String domain, String incarnation, long now) {
        return new WorkerSnapshot("withdrawal-worker", "host", 8080, 9090, 2,
                null, java.util.List.of(), "NONE", 0.0, 0, 1024L, now, now,
                9191, null, CANONICAL_INVENTORY, domain, incarnation);
    }
    @Test
    void exactHeartbeatAndConditionalRepairCannotOverwriteAnotherProcess() throws Exception {
        var registry = new JdbcWorkerRegistry(MigratedPostgres.create());
        long now = System.currentTimeMillis();
        var old = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", now);
        var metrics = new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot("HIGH", 0.9, 7, 99L, null);
        assertThat(registry.heartbeat(old.registrationIdentity(), metrics)).isEmpty();
        assertThat(registry.refreshRegistration(old)).isTrue();
        assertThat(registry.refreshRegistration(old)).isTrue();
        assertThat(registry.heartbeat(old.registrationIdentity(), metrics).orElseThrow().inFlightTaskCount()).isEqualTo(7);
        var replacement = removalWorker(old.admissionDomainId(), "00000000-0000-0000-0000-0000000000e2", now);
        registry.register(replacement);
        assertThat(registry.heartbeat(old.registrationIdentity(), metrics)).isEmpty();
        assertThat(registry.refreshRegistration(old)).isFalse();
        assertThat(registry.getWorker(old.workerId())).contains(replacement);
        var moved = removalWorker("00000000-0000-0000-0000-0000000000d2", replacement.incarnationId(), now);
        registry.register(moved);
        assertThat(registry.heartbeat(replacement.registrationIdentity(), metrics)).isEmpty();
        assertThat(registry.refreshRegistration(replacement)).isFalse();
        assertThat(registry.getWorker(old.workerId())).contains(moved);
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void blockedHeartbeatOrRepairRechecksReplacement(boolean repair) throws Exception {
        var dataSource = MigratedPostgres.create();
        var registry = new JdbcWorkerRegistry(dataSource);
        var original = removalWorker("00000000-0000-0000-0000-0000000000d1",
                "00000000-0000-0000-0000-0000000000e1", System.currentTimeMillis());
        registry.register(original);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var replacing = dataSource.getConnection()) {
            replacing.setAutoCommit(false);
            int blocker;
            try (var statement = replacing.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                result.next();
                blocker = result.getInt(1);
            }
            String nextIncarnation = "00000000-0000-0000-0000-0000000000e2";
            try (var update = replacing.prepareStatement("UPDATE worker_registry SET incarnation_id=? WHERE worker_id=?")) {
                update.setString(1, nextIncarnation);
                update.setString(2, original.workerId());
                assertThat(update.executeUpdate()).isEqualTo(1);
            }
            var mutation = executor.submit(() -> repair ? registry.refreshRegistration(original)
                    : registry.heartbeat(original.registrationIdentity(),
                            new org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot("HIGH", 0.9, 99, 1L, null)).isPresent());
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(() -> {
                try (var connection = dataSource.getConnection();
                     var query = connection.prepareStatement("SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                             + "WHERE wait_event_type='Lock' AND ?=ANY(pg_blocking_pids(pid)))")) {
                    query.setInt(1, blocker);
                    try (var result = query.executeQuery()) {
                        result.next();
                        return result.getBoolean(1);
                    }
                }
            });
            replacing.commit();
            assertThat(mutation.get(10, java.util.concurrent.TimeUnit.SECONDS)).isFalse();
            assertThat(registry.getWorker(original.workerId()).orElseThrow().incarnationId()).isEqualTo(nextIncarnation);
            assertThat(registry.getWorker(original.workerId()).orElseThrow().inFlightTaskCount()).isZero();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }


}
