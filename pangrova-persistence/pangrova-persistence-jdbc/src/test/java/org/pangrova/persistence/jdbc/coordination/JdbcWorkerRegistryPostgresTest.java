package org.pangrova.persistence.jdbc.coordination;

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
import org.pangrova.spi.coordination.WorkerSnapshot;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcWorkerRegistryPostgresTest extends ContainerTestBase {

    private static final String CANONICAL_INVENTORY =
            "CjIIAhogRdVbleB2e7KyN0ENsyhXMAxZ2xaezSU0lUC4Od3Gd8giDGFpLW5hdGl2ZS1y"
                    + "MigBMhZtZXRhZGF0YS1yZWdpc3RyeS10ZXN0";

    @Test
    void preservesCanonicalInventoryAndResultDataPlaneEndpointAcrossRecovery() throws Exception {
        var dataSource = MigratedPostgres.create();
        JdbcWorkerRegistry registry = new JdbcWorkerRegistry(dataSource);
        String inventory = CANONICAL_INVENTORY;
        long now = System.currentTimeMillis();
        WorkerSnapshot worker = new WorkerSnapshot(
                "worker-1", "host-1", 8080, 9090, 2,
                "rack-a", List.of("s3://bucket"),
                "NONE", 0.1, 1, 1024L, now, now,
                9191, null, inventory, 9443,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");

        registry.register(worker);
        WorkerSnapshot recovered = registry.getWorker("worker-1").orElseThrow();

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
        registry.heartbeat("unidentified-worker", new org.pangrova.spi.coordination.HeartbeatSnapshot(
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

}
