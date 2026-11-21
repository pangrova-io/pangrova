package org.pangrova.coordinator.worker;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.coordination.api.membership.WorkerRegistrationIdentity;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.rest.worker.TenantUsageSnapshot;
import static org.pangrova.coordination.api.enums.WorkerHeartbeatStatus.*;
import static org.assertj.core.api.Assertions.*;

class WorkerHeartbeatIdentityTest {
    @Test
    void oldHeartbeatCannotUpdateReplacementMetricsOrTenantUsage() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        var usage = new ClusterTenantUsageAggregator();
        manager.setTenantUsageAggregator(usage);
        var old = TestWorkerInfoBuilder.active("same", "host", 8080, 9090);
        manager.registerWorker(old);
        assertThat(manager.onHeartbeat(identity(old), metrics(1))).isEqualTo(ACCEPTED);
        var replacement = replacement(old);
        manager.registerWorker(replacement);
        assertThat(usage.aggregatedMemoryBytes("tenant")).isZero();
        assertThat(manager.onHeartbeat(identity(replacement), metrics(2))).isEqualTo(ACCEPTED);
        var observed = manager.getWorker("same");
        var persisted = registry.getWorker("same");
        assertThat(manager.onHeartbeat(identity(old), metrics(99))).isEqualTo(FENCED);
        assertThat(manager.refreshWorker(old)).isFalse();
        assertThat(manager.getWorker("same")).isSameAs(observed);
        assertThat(registry.getWorker("same")).isEqualTo(persisted);
        assertThat(usage.aggregatedMemoryBytes("tenant")).isEqualTo(2);
    }

    @Test
    void backendOwnsIdentityEvenWhenCoordinatorCacheStillContainsOldProcess() {
        var registry = new InMemoryWorkerRegistry();
        var first = new WorkerManager(20_000, 60_000, registry);
        var second = new WorkerManager(20_000, 60_000, registry);
        var old = TestWorkerInfoBuilder.active("same", "host", 8080, 9090);
        first.registerWorker(old);
        var replacement = replacement(old);
        second.registerWorker(replacement);
        assertThat(first.onHeartbeat(identity(old), metrics(99))).isEqualTo(FENCED);
        assertThat(first.getWorker("same").workerExecutionIdentity()).isEqualTo(replacement.workerExecutionIdentity());
        assertThat(first.refreshWorker(old)).isFalse();
        assertThat(registry.getWorker("same").orElseThrow().runningTaskCount()).isZero();
        assertThat(first.onHeartbeat(identity(replacement), metrics(3))).isEqualTo(ACCEPTED);
        assertThat(first.getWorker("same").workerExecutionIdentity()).isEqualTo(replacement.workerExecutionIdentity());
        assertThat(first.getWorker("same").runningTaskCount()).isEqualTo(3);
    }

    @Test
    void missingRegistrationRepairCannotRaceAndReplaceANewProcess() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        var old = TestWorkerInfoBuilder.active("same", "host", 8080, 9090);
        assertThat(manager.onHeartbeat(identity(old), metrics(99))).isEqualTo(REGISTRATION_REQUIRED);
        assertThat(manager.refreshWorker(old)).isTrue();
        assertThat(registry.unregister(identity(old))).isTrue();
        assertThat(manager.onHeartbeat(identity(old), metrics(99))).isEqualTo(REGISTRATION_REQUIRED);
        new WorkerManager(20_000, 60_000, registry).registerWorker(replacement(old));
        assertThat(manager.refreshWorker(old)).isFalse();
        assertThat(registry.getWorker("same").orElseThrow().incarnationId())
                .isEqualTo(replacement(old).workerExecutionIdentity().incarnationId());
    }

    @Test
    void persistentFailureCannotPublishLocalMetricsOrUsage() {
        var registry = new InMemoryWorkerRegistry();
        var failure = new IllegalStateException("database unavailable");
        var failing = (org.pangrova.persistence.coordination.api.membership.WorkerRegistry)
                java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{org.pangrova.persistence.coordination.api.membership.WorkerRegistry.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("heartbeat")) throw failure;
                            try { return method.invoke(registry, args); }
                            catch (java.lang.reflect.InvocationTargetException invoked) { throw invoked.getCause(); }
                        });
        var manager = new WorkerManager(20_000, 60_000, failing);
        var usage = new ClusterTenantUsageAggregator();
        manager.setTenantUsageAggregator(usage);
        var worker = TestWorkerInfoBuilder.active("same", "host", 8080, 9090);
        manager.registerWorker(worker);
        var observed = manager.getWorker("same");
        assertThatThrownBy(() -> manager.onHeartbeat(identity(worker), metrics(99))).isSameAs(failure);
        assertThat(manager.getWorker("same")).isSameAs(observed);
        assertThat(usage.aggregatedMemoryBytes("tenant")).isZero();
    }

    private static WorkerRegistrationIdentity identity(WorkerInfo worker) {
        return new WorkerRegistrationIdentity(worker.workerId(), worker.workerExecutionIdentity().admissionDomainId(),
                worker.workerExecutionIdentity().incarnationId());
    }
    private static WorkerManager.HeartbeatData metrics(int count) {
        return new WorkerManager.HeartbeatData(MemoryPressure.NONE, 0.1, count, 1024, null,
                Map.of("tenant", new TenantUsageSnapshot("tenant", count, count)));
    }
    private static WorkerInfo replacement(WorkerInfo old) {
        return TestWorkerInfoBuilder.active(old.workerId(), "replacement", 8181, 9191,
                new WorkerExecutionIdentity(old.workerExecutionIdentity().admissionDomainId(),
                        "00000000-0000-0000-0000-0000000000e2"));
    }
}
