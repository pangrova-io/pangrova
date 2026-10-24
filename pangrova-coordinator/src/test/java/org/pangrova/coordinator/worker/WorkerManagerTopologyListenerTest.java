package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link WorkerManager#fireTopologyChanged()} handles
 * per-listener exceptions: logs error, increments metrics counter,
 * and continues notifying remaining listeners.
 */
class WorkerManagerTopologyListenerTest {

    @Test
    void failingListenerDoesNotBlockSubsequentListeners() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        AtomicLong counterValue = new AtomicLong();
        LongCounter counter = counterValue::addAndGet;
        wm.setListenerFailureCounter(counter);

        List<String> callOrder = new ArrayList<>();

        wm.addTopologyListener(active -> callOrder.add("first"));
        wm.addTopologyListener(active -> { throw new RuntimeException("boom"); });
        wm.addTopologyListener(active -> callOrder.add("third"));

        // registerWorker triggers fireTopologyChanged
        wm.registerWorker(testWorker("w-1"));

        assertThat(callOrder).containsExactly("first", "third");
        assertThat(counterValue.get()).isEqualTo(1);
    }

    @Test
    void noFailureDoesNotIncrementCounter() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        AtomicLong counterValue = new AtomicLong();
        wm.setListenerFailureCounter(counterValue::addAndGet);

        List<String> callOrder = new ArrayList<>();
        wm.addTopologyListener(active -> callOrder.add("ok"));

        wm.registerWorker(testWorker("w-1"));

        assertThat(callOrder).containsExactly("ok");
        assertThat(counterValue.get()).isZero();
    }

    @Test
    void multipleFailuresIncrementCounterForEach() {
        WorkerManager wm = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        AtomicLong counterValue = new AtomicLong();
        wm.setListenerFailureCounter(counterValue::addAndGet);

        wm.addTopologyListener(active -> { throw new RuntimeException("fail-1"); });
        wm.addTopologyListener(active -> { throw new RuntimeException("fail-2"); });

        wm.registerWorker(testWorker("w-1"));

        assertThat(counterValue.get()).isEqualTo(2);
    }

    private static WorkerInfo testWorker(String id) {
        return new WorkerInfo(
                id, "localhost", 8080, 9090, 0,
                HealthState.ACTIVE, MemoryPressure.NONE,
                0.5, 0, 1024L * 1024 * 1024, Instant.now(),
                1.0, null, 2, null, List.of(), null,
                TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
}
