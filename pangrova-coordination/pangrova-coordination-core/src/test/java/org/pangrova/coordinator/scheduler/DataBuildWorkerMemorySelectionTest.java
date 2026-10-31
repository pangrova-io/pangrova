package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.core.placement.TaskScheduler;

import org.junit.jupiter.api.Test;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.common.value.MemoryPressure;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DataBuildWorkerMemorySelectionTest {

    private static final long GIB = 1024L * 1024 * 1024;

    @Test
    void exactHnswCapabilityDoesNotBypassAvailableMemoryLimit() {
        var capability = BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef();
        var format = BuiltInCapabilityCatalog.hnswArtifactFormat();
        var inventory = TestWorkerInfoBuilder.canonicalInventory();
        // A 2 GiB Worker reserves 128 MiB for its system pool before advertising.
        WorkerInfo worker = new WorkerInfo(
                "worker", "localhost", 8080, 9090, 9191,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                2 * GIB - 128 * 1024 * 1024, Instant.now(), 1.0,
                null, 2, null, List.of(), null, inventory,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker"));
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("hnsw-build")
                .setCapability(capability)
                .setOutputArtifact(ArtifactWriteBinding.newBuilder().setFormat(format))
                .build();
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());

        assertThat(scheduler.selectWorkerWithBindings(
                GIB, List.of(worker), List.of(binding))).contains(worker);
        assertThat(scheduler.selectWorkerWithBindings(
                2 * GIB, List.of(worker), List.of(binding))).isEmpty();
    }
}
