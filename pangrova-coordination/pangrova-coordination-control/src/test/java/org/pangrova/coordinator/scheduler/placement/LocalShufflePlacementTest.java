package org.pangrova.coordinator.scheduler.placement;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pangrova.coordination.api.membership.WorkerShuffleDeployment;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalShufflePlacementTest {
    @Test
    void fixesOneLiveIncarnationAndRejectsReplacementRelocationAndForeignStorage() {
        var active = new AtomicReference<>(worker("a", "incarnation-a", "LOCAL"));
        var scans = new AtomicInteger();
        WorkerSelector selector = new WorkerSelector() {
            public WorkerInfo select(String tenantId) { return active.get(); }
            @Override
            public WorkerInfo selectScanWorker(String tenant, List<String> paths, List<OperatorBinding> bindings,
                    long scanWork, Map<WorkerExecutionIdentity, Double> windowWork) {
                assertThat(scanWork).isEqualTo(32L);
                scans.incrementAndGet();
                return active.get();
            }
            public StageResourceGrant computeResourceLimit(int p, int stages, WorkerInfo w) {
                return StageResourceGrant.getDefaultInstance();
            }
        };
        var query = new LocalShufflePlacement(selector);
        var firstStage = query.bindStagePlacement("tenant");
        var first = firstStage.selectScanWorker("tenant", List.of("source"), List.of(), 32L, Map.of());
        assertThat(firstStage.selectWithCacheAffinity("tenant", List.of("source"))).isSameAs(first);
        assertThat(scans.get()).isOne();
        assertThat(query.activeWorkerCount()).isOne();
        active.set(worker("a", "incarnation-b", "LOCAL"));
        var nextStage = query.bindStagePlacement("tenant");
        assertThatThrownBy(() -> nextStage.select("tenant")).hasMessageContaining("Worker changed");
        active.set(worker("b", "incarnation-a", "LOCAL"));
        assertThatThrownBy(() -> query.select("tenant")).hasMessageContaining("Worker changed");
        assertThatThrownBy(() -> query.selectExcluding("tenant", "a")).hasMessageContaining("cannot relocate");
        active.set(worker("c", "incarnation-c", "NATIVE"));
        assertThatThrownBy(() -> new LocalShufflePlacement(selector).select("tenant"))
                .hasMessageContaining("Local storage");
    }

    private static WorkerInfo worker(String id, String incarnation, String mode) {
        var w = TestWorkerInfoBuilder.active(id, "127.0.0.1", 8080, 8081);
        return new WorkerInfo(id, w.host(), w.httpPort(), w.transportPort(), w.resultServerPort(),
                w.healthState(), w.memoryPressure(), w.cpuUtilization(), w.inFlightTaskCount(), w.availableMemoryBytes(),
                w.lastHeartbeat(), w.performanceScore(), w.resourcePoolId(), w.protocolVersion(), w.rackId(),
                w.dataLocality(), w.cacheSummary(), w.capabilityInventory(), w.copyTaskTlsPort(),
                TestWorkerInfoBuilder.executionIdentity(incarnation), new WorkerShuffleDeployment(mode, null));
    }
}
