package org.pangrova.coordinator.generation;

import org.junit.jupiter.api.Test;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.common.value.MemoryPressure;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GenerationDispatchPlacementDiagnosticTest {

    private static final long GIB = 1024L * 1024 * 1024;

    @Test
    void explicitGrantAboveEveryCapableWorkerReportsResourceExhaustion() {
        var ref = BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef();
        var format = BuiltInCapabilityCatalog.hnswArtifactFormat();
        var inventory = TestWorkerInfoBuilder.canonicalInventory();
        WorkerInfo worker = new WorkerInfo(
                "worker", "localhost", 8080, 9090, 9191,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                GIB, Instant.now(), 1.0, null, 2,
                null, List.of(), null, inventory,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker"));
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("hnsw-build")
                .setCapability(ref)
                .setOutputArtifact(ArtifactWriteBinding.newBuilder().setFormat(format))
                .build();

        assertThat(DurableGenerationDispatcher.workerSelectionFailure(
                List.of(worker), List.of(binding), 2 * GIB))
                .hasMessageContaining("RESOURCE_EXHAUSTED")
                .hasMessageContaining(Long.toString(2 * GIB));
        assertThat(DurableGenerationDispatcher.workerSelectionFailure(
                List.of(), List.of(binding), 2 * GIB))
                .hasMessageNotContaining("RESOURCE_EXHAUSTED");
    }
}
