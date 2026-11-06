package org.pangrova.coordinator.worker;

import java.time.Instant;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.scheduler.api.placement.HealthState;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerMembershipFencingTest {
    @Test
    void heartbeatHealthEvaluationAndSameProcessRegistrationCannotUndoDecommission() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        var worker = TestWorkerInfoBuilder.active("worker", "localhost", 8080, 9090);
        manager.registerWorker(worker);
        assertThat(manager.decommission(worker.workerId())).isTrue();
        manager.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity(worker.workerId(), manager.getWorker(worker.workerId()).workerExecutionIdentity().admissionDomainId(), manager.getWorker(worker.workerId()).workerExecutionIdentity().incarnationId()), new WorkerManager.HeartbeatData(MemoryPressure.NONE, 0.2, 1, 1024));
        assertThat(manager.evaluateHealth(worker.workerId(), Instant.now())).isEqualTo(HealthState.DECOMMISSIONING);
        manager.registerWorker(worker);
        manager.loadFromRegistry();
        assertThat(manager.getWorker(worker.workerId()).runningTaskCount()).isEqualTo(1);
        assertThat(manager.getWorker(worker.workerId()).healthState()).isEqualTo(HealthState.DECOMMISSIONING);
        assertThat(manager.getActiveWorkers()).isEmpty();
        manager.registerWorker(replacement(worker));
        assertThat(manager.getWorker(worker.workerId()).healthState()).isEqualTo(HealthState.ACTIVE);
    }

    @Test
    void changedObservationCannotRemoveHeartbeatingWorker() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        var worker = TestWorkerInfoBuilder.active("worker", "localhost", 8080, 9090);
        manager.registerWorker(worker);
        var observed = manager.getWorker(worker.workerId());
        manager.onHeartbeat(new org.pangrova.coordination.api.membership.WorkerRegistrationIdentity(worker.workerId(), manager.getWorker(worker.workerId()).workerExecutionIdentity().admissionDomainId(), manager.getWorker(worker.workerId()).workerExecutionIdentity().incarnationId()), new WorkerManager.HeartbeatData(MemoryPressure.NONE, 0.2, 1, 1024));
        assertThat(manager.removeObservedWorker(observed)).isFalse();
        assertThat(manager.getWorker(worker.workerId()).runningTaskCount()).isEqualTo(1);
        assertThat(registry.getWorker(worker.workerId())).isPresent();
    }

    @Test
    void replacementPublishedDuringDeadCallbackSurvivesHealthRemoval() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        var old = TestWorkerInfoBuilder.withHeartbeat("worker", "localhost", 8080, 9090,
                Instant.now().minusSeconds(120));
        manager.registerWorker(old);
        var replacement = replacement(old);
        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            var scheduler = new WorkerHealthScheduler(manager, id -> manager.registerWorker(replacement), executor);
            scheduler.evaluateAll();
            assertThat(manager.getWorker(old.workerId()).workerExecutionIdentity())
                    .isEqualTo(replacement.workerExecutionIdentity());
            assertThat(registry.getWorker(old.workerId()).orElseThrow().incarnationId())
                    .isEqualTo(replacement.workerExecutionIdentity().incarnationId());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void differentCoordinatorReplacementSurvivesStaleLocalWithdrawal() {
        var registry = new InMemoryWorkerRegistry();
        var first = new WorkerManager(20_000, 60_000, registry);
        var second = new WorkerManager(20_000, 60_000, registry);
        var old = TestWorkerInfoBuilder.active("worker", "localhost", 8080, 9090);
        first.registerWorker(old);
        var observed = first.getWorker(old.workerId());
        var replacement = replacement(old);
        second.registerWorker(replacement);
        assertThat(first.removeObservedWorker(observed)).isTrue();
        assertThat(registry.getWorker(old.workerId()).orElseThrow().incarnationId())
                .isEqualTo(replacement.workerExecutionIdentity().incarnationId());
        assertThat(first.tryRecoverFromRegistry(old.workerId())).isTrue();
        assertThat(first.getWorker(old.workerId()).workerExecutionIdentity())
                .isEqualTo(replacement.workerExecutionIdentity());
    }

    @Test
    void failedPersistentWithdrawalDoesNotEraseLocalRegistration() {
        var registry = new InMemoryWorkerRegistry();
        var failure = new IllegalStateException("registry unavailable");
        var failing = (org.pangrova.persistence.coordination.api.membership.WorkerRegistry)
                java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{org.pangrova.persistence.coordination.api.membership.WorkerRegistry.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("unregister")) throw failure;
                            try {
                                return method.invoke(registry, args);
                            } catch (java.lang.reflect.InvocationTargetException invoked) {
                                throw invoked.getCause();
                            }
                        });
        var manager = new WorkerManager(20_000, 60_000, failing);
        var worker = TestWorkerInfoBuilder.active("worker", "localhost", 8080, 9090);
        manager.registerWorker(worker);
        var observed = manager.getWorker(worker.workerId());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> manager.removeObservedWorker(observed))
                .isSameAs(failure);
        assertThat(manager.getWorker(worker.workerId())).isSameAs(observed);
        assertThat(registry.getWorker(worker.workerId())).isPresent();
    }

    private static WorkerInfo replacement(WorkerInfo worker) {
        return TestWorkerInfoBuilder.active(worker.workerId(), "localhost", 8181, 9191,
                new WorkerExecutionIdentity(worker.workerExecutionIdentity().admissionDomainId(),
                        "00000000-0000-0000-0000-0000000000e2"));
    }
}
