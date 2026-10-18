package org.pangrova.metadata.coordination;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.WorkerSnapshot;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcWorkerRegistryPostgresTest {

    private static final String CANONICAL_INVENTORY =
            "CjIIAhogRdVbleB2e7KyN0ENsyhXMAxZ2xaezSU0lUC4Od3Gd8giDGFpLW5hdGl2ZS1y"
                    + "MigBMhZtZXRhZGF0YS1yZWdpc3RyeS10ZXN0";

    @Test
    void preservesCanonicalInventoryAndResultDataPlaneEndpointAcrossRecovery() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("worker_registry");
        JdbcWorkerRegistry registry = new JdbcWorkerRegistry(dataSource);
        String inventory = CANONICAL_INVENTORY;
        long now = System.currentTimeMillis();
        WorkerSnapshot worker = new WorkerSnapshot(
                "worker-1", "host-1", 8080, 9090, 2,
                "rack-a", List.of("s3://bucket"),
                "NONE", 0.1, 1, 1024L, now, now,
                9191, null, inventory, 9443);

        registry.register(worker);
        WorkerSnapshot recovered = registry.getWorker("worker-1").orElseThrow();

        assertThat(recovered.resultServerPort()).isEqualTo(9191);
        assertThat(recovered.copyTaskTlsPort()).isEqualTo(9443);
        assertThat(recovered.capabilityInventory()).isEqualTo(inventory);

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
    void migrationEvictsLegacyRowsRatherThanInventingInventory() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource(
                "worker_registry_legacy");
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
}
