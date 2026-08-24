package org.pangrova.coordinator.property.dispatch;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.WorkerSnapshot;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 10: workerId precise routing.
 *
 * <p>For any cluster with multiple workers (including same-host multi-instance),
 * looking up by workerId returns the exact matching worker, never a same-host
 * sibling.</p>
 *
 * <p><b>Validates: Requirement 13.2</b></p>
 */
class WorkerIdRoutingPropertyTest {

    @Property(tries = 50)
    void workerIdLookupReturnsExactWorker(
            @ForAll("workerPairs") WorkerPair pair) {

        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        wm.registerWorker(pair.worker1());
        wm.registerWorker(pair.worker2());

        WorkerInfo found1 = wm.getWorker(pair.worker1().workerId());
        WorkerInfo found2 = wm.getWorker(pair.worker2().workerId());

        assertThat(found1).isNotNull();
        assertThat(found1.workerId()).isEqualTo(pair.worker1().workerId());
        assertThat(found1.resultServerPort()).isEqualTo(pair.worker1().resultServerPort());

        assertThat(found2).isNotNull();
        assertThat(found2.workerId()).isEqualTo(pair.worker2().workerId());
        assertThat(found2.resultServerPort()).isEqualTo(pair.worker2().resultServerPort());
    }

    @Provide
    Arbitrary<WorkerPair> workerPairs() {
        Arbitrary<String> hosts = Arbitraries.of("host-a", "host-b", "host-c");
        Arbitrary<Integer> ports = Arbitraries.integers().between(1024, 65535);
        return Combinators.combine(hosts, ports, ports).as((host, port1, port2) -> {
            WorkerInfo w1 = buildWorker("worker-1", host, port1);
            WorkerInfo w2 = buildWorker("worker-2", host, port2);
            return new WorkerPair(w1, w2);
        });
    }

    private static WorkerInfo buildWorker(String workerId, String host, int resultPort) {
        return new WorkerInfo(
                workerId, host, 8080, 9090, resultPort,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                4L * 1024 * 1024 * 1024, Instant.now(), 1.0,
                "default", 1, List.of(), null, List.of());
    }

    record WorkerPair(WorkerInfo worker1, WorkerInfo worker2) {}
}
