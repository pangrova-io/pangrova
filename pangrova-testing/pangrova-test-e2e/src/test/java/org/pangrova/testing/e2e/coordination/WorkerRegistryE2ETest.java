package org.pangrova.testing.e2e.coordination;

import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.TopologyListener;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.coordination.JdbcWorkerRegistry;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.WorkerSnapshot;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.e2e.CanonicalWorkerInventory;
import org.pangrova.testing.e2e.fixture.RegistryTestSchemaHelper;
import org.pangrova.testing.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for Worker Registry distributed scenarios.
 *
 * <p>Validates the full worker registration lifecycle through real PostgreSQL:
 * register → heartbeat → failover recovery → expiration → graceful shutdown.
 * Uses TestContainers PostgreSQL with JdbcWorkerRegistry sharing the same DataSource.</p>
 */
@DisplayName("Worker Registry E2E")
@Testcontainers
class WorkerRegistryE2ETest extends ContainerTestBase {

    private static final long SUSPECT_MS = 20_000;
    private static final long DEAD_MS = 60_000;

    private DataSource dataSource;
    private JdbcWorkerRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        var pg = RegistryTestSchemaHelper.requirePostgres();
        dataSource = new EmbeddedDataSource(
                pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        MetadataSchemaInitializer.createSchema(dataSource);
        RegistryTestSchemaHelper.truncateAll(dataSource, "worker_registry");
        registry = new JdbcWorkerRegistry(dataSource);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dataSource == null) {
            return;
        }
        RegistryTestSchemaHelper.truncateAll(dataSource, "worker_registry");
    }

    // -----------------------------------------------------------------------
    // Scenario 1 — Worker registration full chain
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Worker Registration Full Chain")
    class WorkerRegistrationFullChain {

        @Test
        @DisplayName("Register worker via WorkerManager → JdbcWorkerRegistry → DB → getActiveWorkers returns it")
        void registerWorker_persistedAndRetrievable() {
            WorkerManager manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            WorkerInfo worker = createWorkerInfo("worker-1", "host-1", 8080, 9090);

            manager.registerWorker(worker);

            List<WorkerInfo> active = manager.getActiveWorkers();
            assertThat(active).hasSize(1);
            assertThat(active.get(0).workerId()).isEqualTo("worker-1");

            // Verify DB persistence via registry
            List<WorkerSnapshot> dbWorkers = registry.getActiveWorkers(Duration.ofMinutes(5));
            assertThat(dbWorkers).hasSize(1);
            assertThat(dbWorkers.get(0).workerId()).isEqualTo("worker-1");
            assertThat(dbWorkers.get(0).host()).isEqualTo("host-1");
            assertThat(dbWorkers.get(0).httpPort()).isEqualTo(8080);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — Heartbeat persistence
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Heartbeat Persistence")
    class HeartbeatPersistence {

        @Test
        @DisplayName("Heartbeat updates DB fields: memory_pressure, cpu_utilization, running_task_count, available_memory_bytes")
        void heartbeat_updatesDbFields() throws Exception {
            WorkerManager manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            WorkerInfo worker = createWorkerInfo("worker-hb", "host-hb", 8080, 9090);
            manager.registerWorker(worker);

            // Send heartbeat with updated metrics
            manager.onHeartbeat("worker-hb", new WorkerManager.HeartbeatData(
                    MemoryPressure.HIGH, 0.85, 5, 1024L * 1024 * 512));

            // Verify DB fields updated
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT memory_pressure, cpu_utilization, running_task_count, available_memory_bytes FROM worker_registry WHERE worker_id = ?")) {
                ps.setString(1, "worker-hb");
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString("memory_pressure")).isEqualTo("HIGH");
                    assertThat(rs.getDouble("cpu_utilization")).isCloseTo(0.85, org.assertj.core.data.Offset.offset(0.01));
                    assertThat(rs.getInt("running_task_count")).isEqualTo(5);
                    assertThat(rs.getLong("available_memory_bytes")).isEqualTo(1024L * 1024 * 512);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3 — Heartbeat timeout cleanup
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Heartbeat Timeout Cleanup")
    class HeartbeatTimeoutCleanup {

        @Test
        @DisplayName("Worker with old timestamp is expired from DB by expireStaleFromRegistry")
        void staleWorker_expiredFromDb() throws Exception {
            // Register worker with old timestamp directly in DB
            long staleTimestamp = System.currentTimeMillis() - 120_000; // 2 minutes ago
            insertWorkerWithTimestamp(dataSource, "worker-stale", "host-stale", 8080, 9090, staleTimestamp);

            // Register a fresh worker via manager
            WorkerManager manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            manager.registerWorker(createWorkerInfo("worker-fresh", "host-fresh", 8081, 9091));

            // Expire stale workers (threshold = 60s)
            int expired = manager.expireStaleFromRegistry(Duration.ofMillis(DEAD_MS));

            assertThat(expired).isEqualTo(1);

            // Verify stale worker removed from DB
            Optional<WorkerSnapshot> stale = registry.getWorker("worker-stale");
            assertThat(stale).isEmpty();

            // Fresh worker still in DB
            Optional<WorkerSnapshot> fresh = registry.getWorker("worker-fresh");
            assertThat(fresh).isPresent();

            // Another coordinator loading from registry should not see stale worker
            WorkerManager anotherCoordinator = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            anotherCoordinator.loadFromRegistry();
            assertThat(anotherCoordinator.getActiveWorkers())
                    .extracting(WorkerInfo::workerId)
                    .containsExactly("worker-fresh");
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 5 — Graceful worker shutdown
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Graceful Worker Shutdown")
    class GracefulWorkerShutdown {

        @Test
        @DisplayName("removeWorker deletes DB row and other coordinators do not see it")
        void removeWorker_deletesFromDb() {
            WorkerManager manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            manager.registerWorker(createWorkerInfo("worker-shutdown", "host-sd", 8080, 9090));

            // Verify registered
            assertThat(registry.getWorker("worker-shutdown")).isPresent();

            // Graceful shutdown
            manager.removeWorker("worker-shutdown");

            // DB row deleted
            assertThat(registry.getWorker("worker-shutdown")).isEmpty();

            // Another coordinator loading from registry does not see it
            WorkerManager anotherCoordinator = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            anotherCoordinator.loadFromRegistry();
            assertThat(anotherCoordinator.getActiveWorkers()).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 6 — Concurrent registration idempotency
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Concurrent Registration Idempotency")
    class ConcurrentRegistrationIdempotency {

        @Test
        @DisplayName("Same workerId registered from 2 threads → only 1 DB row")
        void concurrentRegister_singleDbRow() throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> thread1 = executor.submit(() -> {
                    WorkerManager m = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
                    m.registerWorker(createWorkerInfo("worker-dup", "host-a", 8080, 9090));
                });
                Future<?> thread2 = executor.submit(() -> {
                    WorkerManager m = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
                    m.registerWorker(createWorkerInfo("worker-dup", "host-b", 8081, 9091));
                });

                thread1.get();
                thread2.get();
            } finally {
                executor.shutdown();
            }

            // Only 1 DB row
            int rowCount = countWorkerRows(dataSource, "worker-dup");
            assertThat(rowCount).isEqualTo(1);

            // getWorker returns the latest snapshot (one of the two)
            Optional<WorkerSnapshot> snapshot = registry.getWorker("worker-dup");
            assertThat(snapshot).isPresent();
            assertThat(snapshot.get().workerId()).isEqualTo("worker-dup");
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 7 — Topology change notification
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Topology Change Notification")
    class TopologyChangeNotification {

        @Test
        @DisplayName("TopologyListener called with correct active list on register and remove")
        void topologyListener_calledOnRegisterAndRemove() {
            WorkerManager manager = new WorkerManager(SUSPECT_MS, DEAD_MS, registry);
            CopyOnWriteArrayList<List<String>> notifications = new CopyOnWriteArrayList<>();

            TopologyListener listener = activeWorkers -> {
                List<String> ids = activeWorkers.stream()
                        .map(WorkerInfo::workerId)
                        .sorted()
                        .toList();
                notifications.add(ids);
            };
            manager.addTopologyListener(listener);

            // Register two workers
            manager.registerWorker(createWorkerInfo("worker-a", "host-a", 8080, 9090));
            manager.registerWorker(createWorkerInfo("worker-b", "host-b", 8081, 9091));

            // Remove one worker
            manager.removeWorker("worker-a");

            // Verify notifications
            assertThat(notifications).hasSize(3);
            // After first register: [worker-a]
            assertThat(notifications.get(0)).containsExactly("worker-a");
            // After second register: [worker-a, worker-b]
            assertThat(notifications.get(1)).containsExactlyInAnyOrder("worker-a", "worker-b");
            // After remove: [worker-b]
            assertThat(notifications.get(2)).containsExactly("worker-b");
        }
    }

    // -----------------------------------------------------------------------
    // Schema setup and data helpers
    // -----------------------------------------------------------------------

    private static void insertWorkerWithTimestamp(DataSource ds, String workerId, String host,
                                                  int httpPort, int transportPort,
                                                  long heartbeatMs) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO worker_registry (worker_id, host, http_port, transport_port, "
                     + "protocol_version, capability_inventory_proto, data_locality, memory_pressure, "
                     + "last_heartbeat_ms, registered_at_ms) "
                     + "VALUES (?, ?, ?, ?, 2, ?, '[]', 'NONE', ?, ?)")) {
            ps.setString(1, workerId);
            ps.setString(2, host);
            ps.setInt(3, httpPort);
            ps.setInt(4, transportPort);
            ps.setString(5, CanonicalWorkerInventory.encodedActual());
            ps.setLong(6, heartbeatMs);
            ps.setLong(7, heartbeatMs);
            ps.executeUpdate();
        }
    }

    private static int countWorkerRows(DataSource ds, String workerId) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM worker_registry WHERE worker_id = ?")) {
            ps.setString(1, workerId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private static WorkerInfo createWorkerInfo(String workerId, String host,
                                               int httpPort, int transportPort) {
        return new WorkerInfo(
                workerId, host, httpPort, transportPort, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.0, 0, 4L * 1024 * 1024 * 1024,
                Instant.now(), 0.0, null,
                2, null, List.of(), null, CanonicalWorkerInventory.actual(),
                new org.pangrova.protocol.rest.worker.WorkerExecutionIdentity(
                        "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1"));
    }
}
