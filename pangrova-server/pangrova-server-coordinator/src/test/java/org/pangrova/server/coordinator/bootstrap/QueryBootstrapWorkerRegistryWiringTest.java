package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordination.api.membership.WorkerShuffleDeployment;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.common.enums.MemoryPressure;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that QueryBootstrap wires the correct WorkerRegistry implementation
 * based on metadataDataSource availability (R2.1).
 *
 * <p>Uses real PostgreSQL via Testcontainers for JDBC tests (ON CONFLICT syntax)
 * and null DataSource for InMemory fallback test.</p>
 */
class QueryBootstrapWorkerRegistryWiringTest {

    @Test
    void startUsesJdbcWorkerRegistryWhenDataSourcePresent() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource(
                "worker_registry_wiring");

        try (var metadata = new QueryBootstrapMetadataFixture(dataSource)) {
            CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("wiring-jdbc-1", 0);
            QueryBootstrap bootstrap = new QueryBootstrap(
                    config, new NoOpTracingProvider(), null, null, metadata.bootstrap(),
                    new ModuleIntegrationBootstrap(), dataSource, ModelAuthorityFixtures.models());

            try {
                bootstrap.start();
                WorkerManager wm = bootstrap.workerManager();
                assertThat(wm).isNotNull();

                wm.registerWorker(testWorker("worker-jdbc-1", "host1", 8080));

                try (Connection conn = dataSource.getConnection();
                     Statement stmt = conn.createStatement();
                     var rs = stmt.executeQuery(
                             "SELECT worker_id FROM worker_registry WHERE worker_id='worker-jdbc-1'")) {
                    assertThat(rs.next())
                            .as("Worker should be persisted in JDBC worker_registry table")
                            .isTrue();
                    assertThat(rs.getString("worker_id")).isEqualTo("worker-jdbc-1");
                }
            } finally {
                bootstrap.shutdown();
            }
        }
    }

    @Test
    void startUsesInMemoryWorkerRegistryWhenDataSourceNull() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("wiring-mem-1", 0);
        QueryBootstrap bootstrap = new QueryBootstrap(
                config, new NoOpTracingProvider(), null, null, null,
                new ModuleIntegrationBootstrap(), null, ModelAuthorityFixtures.models());

        bootstrap.start();
        try {
            WorkerManager wm = bootstrap.workerManager();
            assertThat(wm).isNotNull();

            wm.registerWorker(testWorker("worker-mem-1", "host2", 8081));
            assertThat(wm.getWorker("worker-mem-1")).isNotNull();
            assertThat(wm.getActiveWorkers()).hasSize(1);
        } finally {
            bootstrap.shutdown();
        }
    }

    @Test
    void jdbcRegistrySurvivesWorkerManagerRecreation() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource(
                "worker_registry_recovery");

        try (var metadata = new QueryBootstrapMetadataFixture(dataSource)) {
            CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("wiring-persist-1", 0);

            QueryBootstrap bootstrap1 = new QueryBootstrap(
                    config, new NoOpTracingProvider(), null, null, metadata.bootstrap(),
                    new ModuleIntegrationBootstrap(), dataSource, ModelAuthorityFixtures.models());
            try {
                bootstrap1.start();
                bootstrap1.workerManager().registerWorker(testWorker("worker-persist-1", "host3", 8082));
            } finally {
                bootstrap1.shutdown();
            }

            QueryBootstrap bootstrap2 = new QueryBootstrap(
                    config, new NoOpTracingProvider(), null, null, metadata.bootstrap(),
                    new ModuleIntegrationBootstrap(), dataSource, ModelAuthorityFixtures.models());
            try {
                bootstrap2.start();
                bootstrap2.workerManager().loadFromRegistry();
                assertThat(bootstrap2.workerManager().getWorker("worker-persist-1"))
                        .as("Worker registered in first bootstrap should be recoverable from JDBC")
                        .isNotNull();
            } finally {
                bootstrap2.shutdown();
            }
        }
    }

    private static WorkerInfo testWorker(String id, String host, int httpPort) {
        return new WorkerInfo(
                id, host, httpPort, httpPort + 1000, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1024L * 1024 * 1024,
                Instant.now(), 0.0, null, 2,
                null, List.of(), null,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(), 0,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id),
                new WorkerShuffleDeployment("NATIVE", null));
    }

}
