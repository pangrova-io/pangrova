package org.pangrova.coordinator.query.execution.retrieval;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.persistence.coordination.memory.membership.InMemoryWorkerRegistry;
import org.pangrova.planner.core.rerank.FullTextScoringNode;
import org.pangrova.planner.core.rerank.RerankerPlanNode;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;
import org.pangrova.scheduler.api.placement.HealthState;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Worker inventory -> shared model resolver -> rerank binding; no Provider/Native invocation claimed. */
class RetrievalRerankBindingTest {
    @Test
    void absentModelKeepsPlannerFailureAndTheCanonicalResolutionCause() {
        var workers = new WorkerManager(20000, 60000, new InMemoryWorkerRegistry());
        assertThatThrownBy(() -> new RetrievalIntentCollector(workers).collectRerankIntent(plan()))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Cannot bind Cross-Encoder")
                .hasCauseInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void replicasBindExactlyWhileConcurrentDeploymentVersionsAreRejected() {
        var workers = new WorkerManager(20000, 60000, new InMemoryWorkerRegistry());
        workers.registerWorker(worker("one", "v1"));
        workers.registerWorker(worker("two", "v1"));
        var collector = new RetrievalIntentCollector(workers);
        var bound = collector.collectRerankIntent(plan()).orElseThrow();
        assertThat(bound.modelId()).isEqualTo("ranker");
        assertThat(bound.modelVersion()).isEqualTo("v1");
        assertThat(bound.modelLimits().getMaxProviderItemsPerTask()).isEqualTo(100);
        assertThat(bound.inputLimit()).isEqualTo(10);
        assertThat(bound.outputLimit()).isEqualTo(5);
        assertThat(bound.documentField()).isEqualTo("body");
        workers.registerWorker(worker("three", "v2"));
        assertThatThrownBy(() -> collector.collectRerankIntent(plan()))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("multiple active versions")
                .hasCauseInstanceOf(NoAvailableWorkerException.class);
    }

    private static Optional<RerankerPlanNode> plan() {
        return Optional.of(new RerankerPlanNode(new RerankerPlanNode.CrossEncoderStrategy("ranker", 10),
                List.of(new RerankerPlanNode.FullTextSource(
                        new FullTextScoringNode("lake.docs.items", "body", "lakehouse", List.of()))),
                5, "lakehouse", List.of()));
    }

    private static WorkerInfo worker(String id, String version) {
        var capability = BuiltInCapabilityCatalog.crossEncoderRerankPlanRef().toBuilder()
                .setImplementationBuildId("native-build").build();
        var output = LogicalType.newBuilder().setCanonicalSql("DOUBLE")
                .setSubstraitType(io.substrait.proto.Type.newBuilder().setFp64(io.substrait.proto.Type.FP64.newBuilder()
                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED))).build();
        var model = ModelAvailability.newBuilder().setModel(VersionedRef.newBuilder()
                        .setId("ranker").setVersion(version).setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder().setCapability(capability)
                        .setOutputType(output).setBindingContractSha256(ByteString.copyFrom(new byte[32])))
                .setLimits(CapabilityLimits.newBuilder().setMaxBatchItems(16).setMaxInputTokens(8192)
                        .setMaxOutputTokens(1024).setMaxConcurrency(4).setMaxProviderItemsPerTask(100)
                        .setMaxProviderAttemptsPerTask(100).setMaxRequestBytes(1048576).setMaxResponseBytes(2097152))
                .build();
        var inventory = TestWorkerInfoBuilder.canonicalInventory().toBuilder().clearCapabilities().clearModels()
                .addCapabilities(CapabilityDescriptor.newBuilder().setRef(capability)
                        .setKind(CapabilityKind.CAPABILITY_KIND_MODEL_INVOCATION)
                        .setPlacement(ExecutionPlacement.EXECUTION_PLACEMENT_WORKER_LOCAL_GOVERNED_CALLBACK)
                        .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH).setRequiredPrivilege("AI_MODEL_USAGE"))
                .addModels(model).build();
        return new WorkerInfo(id, "localhost", 8080, 9090, 9091, HealthState.ACTIVE, MemoryPressure.NONE,
                0.1, 0, 1073741824L, Instant.now(), 1.0, "default", 2, null, List.of(), null,
                inventory, TestWorkerInfoBuilder.executionIdentity(id));
    }
}
