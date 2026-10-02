package org.pangrova.coordinator.scheduler;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.resource.MemoryPressure;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ArtifactBuildWorkerMemorySelectionTest {

    private static final long GIB = 1024L * 1024 * 1024;

    @Test
    void exactHnswCapabilityDoesNotBypassAvailableMemoryLimit() {
        var capability = BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef();
        var format = BuiltInCapabilityCatalog.hnswArtifactFormat();
        var inventory = TestWorkerInfoBuilder.canonicalInventory().toBuilder()
                .addCapabilities(CapabilityDescriptor.newBuilder()
                        .setRef(capability.toBuilder()
                                .setImplementationBuildId(format.getWriterBuildId()))
                        .setKind(CapabilityKind.CAPABILITY_KIND_BACKGROUND_SINK)
                        .setPlacement(ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE)
                        .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                        .setRequiredPrivilege("CREATE INDEX")
                        .addSupportedArtifactFormats(format))
                .build();
        // A 2 GiB Worker reserves 128 MiB for its system pool before advertising.
        WorkerInfo worker = new WorkerInfo(
                "worker", "localhost", 8080, 9090, 9191,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.0, 0,
                2 * GIB - 128 * 1024 * 1024, Instant.now(), 1.0,
                null, 2, null, List.of(), null, inventory);
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
