package org.pangrova.persistence.coordination.postgresql.membership;

import org.pangrova.testing.testkit.ContainerTestBase;

import org.pangrova.coordination.api.heartbeat.HeartbeatSnapshot;
import org.pangrova.coordination.api.membership.WorkerSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcWorkerRegistryTest extends ContainerTestBase {

    private static final String CANONICAL_INVENTORY =
            "CjIIAhogRdVbleB2e7KyN0ENsyhXMAxZ2xaezSU0lUC4Od3Gd8giDGFpLW5hdGl2ZS1y"
                    + "MigBMhZtZXRhZGF0YS1yZWdpc3RyeS10ZXN0";

    private DataSource dataSource;
    private JdbcWorkerRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = org.pangrova.testing.testkit.postgres.MigratedPostgres.create();
        this.registry = new JdbcWorkerRegistry(dataSource);
    }

    @Test
    void registerAndGetWorker() {
        WorkerSnapshot worker = createWorker("w-1", "host1", 8080);
        registry.register(worker);

        Optional<WorkerSnapshot> found = registry.getWorker("w-1");
        assertThat(found).isPresent();
        assertThat(found.get().host()).isEqualTo("host1");
        assertThat(found.get().httpPort()).isEqualTo(8080);
    }

    @Test
    void registerUpsertUpdatesExistingWorker() {
        WorkerSnapshot worker1 = createWorker("w-1", "host1", 8080);
        registry.register(worker1);

        WorkerSnapshot worker2 = createWorker("w-1", "host2", 9090);
        registry.register(worker2);

        Optional<WorkerSnapshot> found = registry.getWorker("w-1");
        assertThat(found).isPresent();
        assertThat(found.get().host()).isEqualTo("host2");
        assertThat(found.get().httpPort()).isEqualTo(9090);
    }

    @Test
    void heartbeatUpdatesMetrics() {
        registry.register(createWorker("w-1", "host1", 8080));

        HeartbeatSnapshot snapshot = new HeartbeatSnapshot("HIGH", 0.9, 5, 512, null);
        registry.heartbeat(registry.getWorker("w-1").orElseThrow().registrationIdentity(), snapshot);

        Optional<WorkerSnapshot> found = registry.getWorker("w-1");
        assertThat(found).isPresent();
        assertThat(found.get().memoryPressure()).isEqualTo("HIGH");
        assertThat(found.get().inFlightTaskCount()).isEqualTo(5);
    }

    @Test
    void unregisterRemovesWorker() {
        registry.register(createWorker("w-1", "host1", 8080));
        registry.unregister(registry.getWorker("w-1").orElseThrow().registrationIdentity());

        assertThat(registry.getWorker("w-1")).isEmpty();
    }

    @Test
    void getActiveWorkersFiltersStale() {
        long now = System.currentTimeMillis();
        WorkerSnapshot fresh = new WorkerSnapshot("w-fresh", "host1", 8080, 9090, 2,
                null, List.of(), "NONE", 0.1, 0, 1024, now, now,
                0, null, CANONICAL_INVENTORY,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
        WorkerSnapshot stale = new WorkerSnapshot("w-stale", "host2", 8080, 9090, 2,
                null, List.of(), "NONE", 0.1, 0, 1024, now - 120_000, now - 120_000,
                0, null, CANONICAL_INVENTORY,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");

        registry.register(fresh);
        registry.register(stale);

        List<WorkerSnapshot> active = registry.getActiveWorkers(Duration.ofSeconds(60));
        assertThat(active).hasSize(1);
        assertThat(active.get(0).workerId()).isEqualTo("w-fresh");
    }

    @Test
    void expireStaleRemovesOldWorkers() {
        long now = System.currentTimeMillis();
        WorkerSnapshot stale = new WorkerSnapshot("w-stale", "host1", 8080, 9090, 2,
                null, List.of(), "NONE", 0.1, 0, 1024, now - 120_000, now - 120_000,
                0, null, CANONICAL_INVENTORY,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
        registry.register(stale);

        int expired = registry.expireStale(Duration.ofSeconds(60));
        assertThat(expired).isEqualTo(1);
        assertThat(registry.getWorker("w-stale")).isEmpty();
    }

    private static WorkerSnapshot createWorker(String id, String host, int httpPort) {
        long now = System.currentTimeMillis();
        return new WorkerSnapshot(id, host, httpPort, httpPort + 1000, 2,
                null, List.of(), "NONE", 0.1, 0, 1024, now, now,
                0, null, CANONICAL_INVENTORY,
                "00000000-0000-0000-0000-0000000000d1", "00000000-0000-0000-0000-0000000000e1");
    }
}
