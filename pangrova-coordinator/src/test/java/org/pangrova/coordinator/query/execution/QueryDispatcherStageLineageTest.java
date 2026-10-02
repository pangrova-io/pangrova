package org.pangrova.coordinator.query.execution;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.governance.lineage.LineageBackend;
import org.pangrova.governance.lineage.LineageEvent;
import org.pangrova.governance.lineage.LineageEventEmitter;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Verifies LineageEventEmitter is wirable into QueryDispatcherContext
 * and that stage lineage events are emitted with correct structure.
 */
class QueryDispatcherStageLineageTest {

    private LineageEventEmitter emitter;

    @AfterEach
    void cleanup() {
        if (emitter != null) {
            emitter.close();
        }
    }

    @Test
    void lineageEmitterIsNullableInDependencies() {
        QueryDispatcherContext deps = QueryDispatcherContext.withDefaults(
                new org.pangrova.planner.SqlCompiler(),
                null, null, null,
                new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                        org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                        new org.pangrova.observability.noop.NoOpTracingProvider()),
                new org.pangrova.coordinator.scheduler.TaskScheduler(
                        new org.pangrova.coordinator.worker.WorkerPerformanceScorer()),
                new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                null, null, null, 0L);

        assertThat(deps.lineageEmitter()).isNull();
    }

    @Test
    void lineageEmitterAcceptsStageEvents() throws Exception {
        var captured = new CopyOnWriteArrayList<LineageEvent>();
        emitter = new LineageEventEmitter(new CapturingLineageBackend(captured));

        emitter.emitStartEvent("q-1/stage-0", null, List.of(), List.of());
        emitter.emitCompleteEvent("q-1/stage-0");
        awaitEvents(captured, 2, 5);

        assertThat(captured).hasSizeGreaterThanOrEqualTo(2);
        assertThat(captured.get(0).eventType()).isEqualTo(LineageEvent.EventType.START);
        assertThat(captured.get(0).runId()).isEqualTo("q-1/stage-0");
        assertThat(captured.get(1).eventType()).isEqualTo(LineageEvent.EventType.COMPLETE);
    }

    @Test
    void lineageEmitterWiredIntoDependencies() {
        var captured = new CopyOnWriteArrayList<LineageEvent>();
        emitter = new LineageEventEmitter(new CapturingLineageBackend(captured));

        QueryDispatcherContext deps = QueryDispatcherContext.withDefaults(
                new org.pangrova.planner.SqlCompiler(),
                null, null, null,
                new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                        org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                        new org.pangrova.observability.noop.NoOpTracingProvider()),
                new org.pangrova.coordinator.scheduler.TaskScheduler(
                        new org.pangrova.coordinator.worker.WorkerPerformanceScorer()),
                new org.pangrova.coordinator.worker.WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry()),
                new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                null, null, emitter, 0L);

        assertThat(deps.lineageEmitter()).isSameAs(emitter);
    }

    private static void awaitEvents(List<LineageEvent> events, int expected, int timeoutSec)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSec);
        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    /** Real LineageBackend that captures events for assertion. */
    static class CapturingLineageBackend implements LineageBackend {
        private final List<LineageEvent> captured;

        CapturingLineageBackend(List<LineageEvent> captured) {
            this.captured = captured;
        }

        @Override
        public void send(LineageEvent event) {
            captured.add(event);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }
}
