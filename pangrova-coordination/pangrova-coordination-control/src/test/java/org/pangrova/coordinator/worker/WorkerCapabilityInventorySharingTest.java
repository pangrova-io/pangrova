package org.pangrova.coordinator.worker;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Real full catalogue registration/recovery contracts; not independent Worker processes or a throughput claim. */
class WorkerCapabilityInventorySharingTest {
    @Test
    void fortySeparatelyDecodedWorkersShareOnlyTheirIdenticalImmutableInventory() throws Exception {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        CapabilityInventory first = null;
        String encoded = null;
        for (int index = 0; index < 40; index++) {
            String id = "density-worker-" + index;
            WorkerInfo template = TestWorkerInfoBuilder.active(id, "host-" + index, 8080, 9090);
            WorkerInfo original = template.withCapabilityInventory(
                    CapabilityInventory.parseFrom(template.capabilityInventory().toByteArray()));
            if (first != null) assertThat(original.capabilityInventory()).isNotSameAs(first);
            manager.registerWorker(original);
            WorkerInfo registered = manager.getWorker(id);
            if (first == null) {
                first = registered.capabilityInventory();
                encoded = registry.getWorker(id).orElseThrow().capabilityInventory();
            }
            assertThat(registered.capabilityInventory()).isSameAs(first)
                    .isEqualTo(original.capabilityInventory());
            assertThat(registry.getWorker(id).orElseThrow().capabilityInventory()).isSameAs(encoded);
            assertThat(registered.workerId()).isEqualTo(id);
            assertThat(registered.host()).isEqualTo("host-" + index);
        }
        assertThat(manager.getAllWorkers()).hasSize(40);
        assertThat(registry.workerCount()).isEqualTo(40);
    }

    @Test
    void registryRecoveryAlsoSharesEqualCataloguesWithoutChangingWorkerIdentity() {
        var registry = new InMemoryWorkerRegistry();
        var original = new WorkerManager(20_000, 60_000, registry);
        original.registerWorker(TestWorkerInfoBuilder.active("recover-a", "host-a", 8080, 9090));
        original.registerWorker(TestWorkerInfoBuilder.active("recover-b", "host-b", 8081, 9091));
        var recovered = new WorkerManager(20_000, 60_000, registry);
        recovered.loadFromRegistry();
        assertThat(recovered.getWorker("recover-a").capabilityInventory())
                .isSameAs(recovered.getWorker("recover-b").capabilityInventory())
                .isEqualTo(original.getWorker("recover-a").capabilityInventory());
        assertThat(recovered.getWorker("recover-a").host()).isEqualTo("host-a");
        assertThat(recovered.getWorker("recover-b").httpPort()).isEqualTo(8081);
        var singleRecovery = new WorkerManager(20_000, 60_000, registry);
        assertThat(singleRecovery.tryRecoverFromRegistry("recover-a")).isTrue();
        assertThat(singleRecovery.tryRecoverFromRegistry("recover-b")).isTrue();
        assertThat(singleRecovery.getWorker("recover-a").capabilityInventory())
                .isSameAs(singleRecovery.getWorker("recover-b").capabilityInventory());
    }

    @Test
    void concurrentRegistrationsPreserveOneExactInventoryAndAllDistinctWorkers() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        IntStream.range(0, 12).parallel().forEach(index -> manager.registerWorker(
                TestWorkerInfoBuilder.active("parallel-" + index, "host-" + index, 8080, 9090)));
        CapabilityInventory first = manager.getWorker("parallel-0").capabilityInventory();
        assertThat(manager.getAllWorkers()).hasSize(12)
                .allSatisfy(worker -> assertThat(worker.capabilityInventory()).isSameAs(first));
        assertThat(registry.workerCount()).isEqualTo(12);
    }

    @Test
    void differentRolloutInventoriesAreNeverSubstitutedOrShared() {
        var registry = new InMemoryWorkerRegistry();
        var manager = new WorkerManager(20_000, 60_000, registry);
        WorkerInfo original = TestWorkerInfoBuilder.active("rollout-a", "host-a", 8080, 9090);
        CapabilityInventory otherRollout = original.capabilityInventory().toBuilder()
                .setProtocol(original.capabilityInventory().getProtocol().toBuilder()
                        .setRolloutEpoch("different-rollout-control-contract"))
                .build();
        WorkerInfo other = TestWorkerInfoBuilder.active("rollout-b", "host-b", 8081, 9091)
                .withCapabilityInventory(otherRollout);
        manager.registerWorker(original);
        manager.registerWorker(other);
        assertThat(manager.getWorker("rollout-a").capabilityInventory()).isSameAs(original.capabilityInventory());
        assertThat(manager.getWorker("rollout-b").capabilityInventory()).isSameAs(otherRollout);
        assertThat(registry.getWorker("rollout-a").orElseThrow().capabilityInventory())
                .isNotEqualTo(registry.getWorker("rollout-b").orElseThrow().capabilityInventory());
    }
}
