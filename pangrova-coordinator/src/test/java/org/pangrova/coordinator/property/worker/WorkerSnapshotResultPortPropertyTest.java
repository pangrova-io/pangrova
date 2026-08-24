package org.pangrova.coordinator.property.worker;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 11: WorkerSnapshot resultServerPort persistence round-trip.
 *
 * <p>For any WorkerSnapshot, after persisting to the registry and recovering,
 * the resultServerPort value is preserved.</p>
 *
 * <p><b>Validates: Requirement 13.4</b></p>
 */
class WorkerSnapshotResultPortPropertyTest {

    @Property(tries = 100)
    void resultServerPortSurvivesRegistryRoundTrip(
            @ForAll("resultPorts") int resultPort) {

        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        WorkerManager wm = new WorkerManager(20_000, 60_000, registry);

        WorkerInfo original = new WorkerInfo(
                "worker-rt", "host-1", 8080, 9090, resultPort,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.2, 1,
                2L * 1024 * 1024 * 1024, Instant.now(), 1.0,
                "default", 1, List.of("NATIVE"), null, List.of());
        wm.registerWorker(original);

        WorkerManager recovered = new WorkerManager(20_000, 60_000, registry);
        recovered.loadFromRegistry();

        WorkerInfo loaded = recovered.getWorker("worker-rt");
        assertThat(loaded).isNotNull();
        assertThat(loaded.resultServerPort())
                .as("resultServerPort must survive registry round-trip")
                .isEqualTo(resultPort);
    }

    @Provide
    Arbitrary<Integer> resultPorts() {
        return Arbitraries.integers().between(0, 65535);
    }
}
