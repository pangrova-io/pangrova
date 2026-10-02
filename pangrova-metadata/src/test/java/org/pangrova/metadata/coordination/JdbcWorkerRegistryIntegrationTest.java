package org.pangrova.metadata.coordination;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.spi.coordination.HeartbeatSnapshot;
import org.pangrova.spi.coordination.WorkerSnapshot;
import org.pangrova.testing.ContainerTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link JdbcWorkerRegistry} using TestContainers PostgreSQL.
 * Validates INSERT...ON CONFLICT upsert, heartbeat updates, stale expiration, and JSON round-trip.
 */
@Testcontainers
class JdbcWorkerRegistryIntegrationTest extends ContainerTestBase {

    private static final String CANONICAL_INVENTORY =
            "CjIIAhogRdVbleB2e7KyN0ENsyhXMAxZ2xaezSU0lUC4Od3Gd8giDGFpLW5hdGl2ZS1y"
                    + "MigBMhZtZXRhZGF0YS1yZWdpc3RyeS10ZXN0";
    private static final String UPDATED_CANONICAL_INVENTORY =
            "CjIIAhogRdVbleB2e7KyN0ENsyhXMAxZ2xaezSU0lUC4Od3Gd8giDGFpLW5hdGl2ZS1y"
                    + "MigBMhltZXRhZGF0YS1yZWdpc3RyeS11cGRhdGVk";

    @SuppressWarnings("resource")
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test");

    private DataSource dataSource;
    private JdbcWorkerRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        initSchema(dataSource);
        truncate(dataSource);
        registry = new JdbcWorkerRegistry(dataSource);
    }

    @Test
    void registerAndGetWorker() {
        WorkerSnapshot worker = newWorker("w-1", "host-1", 8080, 9090);
        registry.register(worker);

        Optional<WorkerSnapshot> result = registry.getWorker("w-1");
        assertThat(result).isPresent();
        WorkerSnapshot got = result.get();
        assertThat(got.workerId()).isEqualTo("w-1");
        assertThat(got.host()).isEqualTo("host-1");
        assertThat(got.httpPort()).isEqualTo(8080);
        assertThat(got.transportPort()).isEqualTo(9090);
        assertThat(got.protocolVersion()).isEqualTo(2);
        assertThat(got.capabilityInventory()).isEqualTo(CANONICAL_INVENTORY);
        assertThat(got.rackId()).isEqualTo("rack-a");
        assertThat(got.dataLocality()).containsExactly("/data/0");
        assertThat(got.memoryPressure()).isEqualTo("NONE");
        assertThat(got.cpuUtilization()).isEqualTo(0.1);
        assertThat(got.runningTaskCount()).isEqualTo(2);
        assertThat(got.availableMemoryBytes()).isEqualTo(1024L);
        assertThat(got.lastHeartbeatMs()).isEqualTo(worker.lastHeartbeatMs());
        assertThat(got.registeredAtMs()).isEqualTo(worker.registeredAtMs());
    }

    @Test
    void registerIsIdempotentUpsert() throws Exception {
        long now = System.currentTimeMillis();
        WorkerSnapshot first = new WorkerSnapshot(
                "w-dup", "host-old", 8080, 9090, 2,
                "rack-a", List.of("/data/0"),
                "NONE", 0.1, 1, 512L, now, now,
                0, null, CANONICAL_INVENTORY);
        registry.register(first);

        WorkerSnapshot second = new WorkerSnapshot(
                "w-dup", "host-new", 9999, 7777, 2,
                "rack-b", List.of("/data/1"),
                "HIGH", 0.9, 5, 2048L, now + 1000, now + 1000,
                0, null, UPDATED_CANONICAL_INVENTORY);
        registry.register(second);

        // Verify only one row exists
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM worker_registry WHERE worker_id='w-dup'")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }

        // Verify second registration overwrote the first
        Optional<WorkerSnapshot> result = registry.getWorker("w-dup");
        assertThat(result).isPresent();
        WorkerSnapshot got = result.get();
        assertThat(got.host()).isEqualTo("host-new");
        assertThat(got.httpPort()).isEqualTo(9999);
        assertThat(got.transportPort()).isEqualTo(7777);
        assertThat(got.protocolVersion()).isEqualTo(2);
        assertThat(got.capabilityInventory())
                .isEqualTo(UPDATED_CANONICAL_INVENTORY);
        assertThat(got.rackId()).isEqualTo("rack-b");
        assertThat(got.memoryPressure()).isEqualTo("HIGH");
    }

    @Test
    void heartbeatUpdatesDynamicFields() {
        WorkerSnapshot worker = newWorker("w-hb", "host-1", 8080, 9090);
        registry.register(worker);

        HeartbeatSnapshot hb = new HeartbeatSnapshot("HIGH", 0.95, 10, 256L);
        registry.heartbeat("w-hb", hb);

        WorkerSnapshot got = registry.getWorker("w-hb").orElseThrow();
        // Dynamic fields updated
        assertThat(got.memoryPressure()).isEqualTo("HIGH");
        assertThat(got.cpuUtilization()).isEqualTo(0.95);
        assertThat(got.runningTaskCount()).isEqualTo(10);
        assertThat(got.availableMemoryBytes()).isEqualTo(256L);
        // last_heartbeat_ms updated by heartbeat() to System.currentTimeMillis()
        assertThat(got.lastHeartbeatMs()).isGreaterThanOrEqualTo(worker.lastHeartbeatMs());

        // Static fields unchanged
        assertThat(got.host()).isEqualTo("host-1");
        assertThat(got.httpPort()).isEqualTo(8080);
        assertThat(got.transportPort()).isEqualTo(9090);
        assertThat(got.protocolVersion()).isEqualTo(2);
        assertThat(got.capabilityInventory()).isEqualTo(CANONICAL_INVENTORY);
        assertThat(got.rackId()).isEqualTo("rack-a");
        assertThat(got.dataLocality()).containsExactly("/data/0");
        assertThat(got.registeredAtMs()).isEqualTo(worker.registeredAtMs());
    }

    @Test
    void getActiveWorkersFiltersStale() {
        long now = System.currentTimeMillis();
        // Fresh worker: heartbeat just now
        WorkerSnapshot fresh = new WorkerSnapshot(
                "w-fresh", "host-1", 8080, 9090, 2,
                null, List.of(),
                "NONE", 0.0, 0, 0L, now, now,
                0, null, CANONICAL_INVENTORY);
        registry.register(fresh);

        // Stale worker: heartbeat 10 minutes ago
        WorkerSnapshot stale = new WorkerSnapshot(
                "w-stale", "host-2", 8081, 9091, 2,
                null, List.of(),
                "NONE", 0.0, 0, 0L, now - Duration.ofMinutes(10).toMillis(),
                now - Duration.ofMinutes(10).toMillis(), 0, null,
                CANONICAL_INVENTORY);
        registry.register(stale);

        List<WorkerSnapshot> active = registry.getActiveWorkers(Duration.ofMinutes(5));
        assertThat(active).hasSize(1);
        assertThat(active.get(0).workerId()).isEqualTo("w-fresh");
    }

    @Test
    void expireStaleRemovesOldEntries() {
        long now = System.currentTimeMillis();
        // Old worker
        WorkerSnapshot old = new WorkerSnapshot(
                "w-old", "host-1", 8080, 9090, 2,
                null, List.of(),
                "NONE", 0.0, 0, 0L, now - Duration.ofHours(1).toMillis(),
                now - Duration.ofHours(1).toMillis(), 0, null,
                CANONICAL_INVENTORY);
        registry.register(old);

        // Recent worker
        WorkerSnapshot recent = new WorkerSnapshot(
                "w-recent", "host-2", 8081, 9091, 2,
                null, List.of(),
                "NONE", 0.0, 0, 0L, now, now,
                0, null, CANONICAL_INVENTORY);
        registry.register(recent);

        int expired = registry.expireStale(Duration.ofMinutes(30));
        assertThat(expired).isEqualTo(1);
        assertThat(registry.getWorker("w-old")).isEmpty();
        assertThat(registry.getWorker("w-recent")).isPresent();
    }

    @Test
    void unregisterRemovesWorker() {
        registry.register(newWorker("w-rm", "host-1", 8080, 9090));
        assertThat(registry.getWorker("w-rm")).isPresent();

        registry.unregister("w-rm");
        assertThat(registry.getWorker("w-rm")).isEmpty();
    }

    @Test
    void canonicalInventoryAndDataLocalityRoundTrip() {
        long now = System.currentTimeMillis();
        WorkerSnapshot worker = new WorkerSnapshot(
                "w-json", "host-1", 8080, 9090, 2,
                null, List.of("/data/1", "/data/2"),
                "NONE", 0.0, 0, 0L, now, now,
                0, null, CANONICAL_INVENTORY);
        registry.register(worker);

        WorkerSnapshot got = registry.getWorker("w-json").orElseThrow();
        assertThat(got.capabilityInventory()).isEqualTo(CANONICAL_INVENTORY);
        assertThat(got.dataLocality()).containsExactly("/data/1", "/data/2");
    }

    // --- helpers ---

    private static WorkerSnapshot newWorker(String id, String host, int httpPort, int transportPort) {
        long now = System.currentTimeMillis();
        return new WorkerSnapshot(
                id, host, httpPort, transportPort, 2,
                "rack-a", List.of("/data/0"),
                "NONE", 0.1, 2, 1024L, now, now,
                0, null, CANONICAL_INVENTORY);
    }

    private static void initSchema(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS worker_registry (
                        worker_id VARCHAR(128) NOT NULL PRIMARY KEY,
                        host VARCHAR(256) NOT NULL,
                        http_port INT NOT NULL,
                        transport_port INT NOT NULL,
                        result_server_port INT NOT NULL DEFAULT 0,
                        protocol_version INT NOT NULL DEFAULT 2 CHECK (protocol_version = 2),
                        capability_inventory_proto TEXT NOT NULL,
                        rack_id VARCHAR(128) DEFAULT NULL,
                        data_locality TEXT,
                        memory_pressure VARCHAR(32) NOT NULL DEFAULT 'NONE',
                        cpu_utilization DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                        running_task_count INT NOT NULL DEFAULT 0,
                        available_memory_bytes BIGINT NOT NULL DEFAULT 0,
                        last_heartbeat_ms BIGINT NOT NULL,
                        registered_at_ms BIGINT NOT NULL
                    )""");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_wr_heartbeat ON worker_registry (last_heartbeat_ms)");
        }
    }

    private static void truncate(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM worker_registry");
        }
    }
}
