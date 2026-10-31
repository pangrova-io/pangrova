package org.pangrova.coordinator.query.execution.dispatch.dag;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.control.embedding.ConcurrentMapEmbeddingModelRegistry;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.ai.control.embedding.ModelUpdateRequest;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.scheduler.core.failure.NoAvailableWorkerException;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.ai.api.embedding.EmbeddingModelBindingIdentity;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.security.api.UserIdentity;

import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultWorkerSelectorModelResolutionTest {

    private static final long ONE_GIB = 1024L * 1024 * 1024;
    private static final UserIdentity USER = new UserIdentity(
            "ai-user", Set.of("embedding_user"), null, Instant.EPOCH);

    @Test
    void resolvesReplicatedExactModelAndPinsAdvertisedNativeBuild() {
        CapabilityRef required = BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
        CapabilityRef advertised = required.toBuilder()
                .setImplementationBuildId("native-build-a")
                .build();
        ModelAvailability availability = model(
                "catalog.chat", "v7", signature(1), advertised, validLimits());
        WorkerManager workers = workerManager();
        workers.registerWorker(worker("worker-a", availability));
        workers.registerWorker(worker("worker-b", availability));

        ModelAvailability resolved = selector(workers).resolveExactModel(
                "catalog.chat", required, USER);

        assertThat(resolved).isEqualTo(availability);
        assertThat(resolved.getCapabilityContracts(0).getCapability()
                .getImplementationBuildId())
                .isEqualTo("native-build-a");
    }

    @Test
    void rejectsAmbiguousDeploymentIdentityAndInvalidInventoryEnvelope() {
        CapabilityRef required = BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
        CapabilityRef advertised = required.toBuilder()
                .setImplementationBuildId("native-build-a")
                .build();
        WorkerManager ambiguous = workerManager();
        ambiguous.registerWorker(worker("worker-a", model(
                "catalog.chat", "v7", signature(1), advertised, validLimits())));
        ambiguous.registerWorker(worker("worker-b", model(
                "catalog.chat", "v8", signature(2), advertised, validLimits())));

        assertThatThrownBy(() -> selector(ambiguous).resolveExactModel(
                "catalog.chat", required, USER))
                .isInstanceOf(NoAvailableWorkerException.class)
                .hasMessageContaining("multiple active versions");

        WorkerManager incomplete = workerManager();
        assertThatThrownBy(() -> incomplete.registerWorker(worker(
                "worker-a",
                model("catalog.chat", "v7", signature(1), advertised,
                        validLimits().toBuilder()
                                .clearMaxProviderItemsPerTask().build()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete limits");
    }

    @Test
    void rejectsMissingCapabilityAndPinnedBuildMismatch() {
        CapabilityRef required = BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
        CapabilityRef advertised = required.toBuilder()
                .setImplementationBuildId("native-build-a")
                .build();
        WorkerManager workers = workerManager();
        workers.registerWorker(worker("worker-a", model(
                "catalog.chat", "v7", signature(1), advertised, validLimits())));

        assertThatThrownBy(() -> selector(workers).resolveExactModel(
                "catalog.chat", required.toBuilder()
                        .setImplementationBuildId("native-build-b").build(), USER))
                .isInstanceOf(NoAvailableWorkerException.class)
                .hasMessageContaining("No active Worker");
        assertThatThrownBy(() -> selector(workers).resolveExactModel(
                "catalog.other", required, USER))
                .isInstanceOf(NoAvailableWorkerException.class)
                .hasMessageContaining("No active Worker");
    }

    @Test
    void embeddingResolutionRequiresRegisteredFencedCatalogRevision() {
        CapabilityRef required = BuiltInCapabilityCatalog.aiEmbeddingPlanRef();
        CapabilityRef advertised = required.toBuilder()
                .setImplementationBuildId("native-build-a")
                .build();
        ModelAvailability availability = embeddingModel(
                "catalog.embed", "model-v7", signature(3), advertised, validLimits(), 768);
        WorkerManager workers = workerManager();
        workers.registerWorker(worker("worker-a", availability));

        assertThatThrownBy(() -> selector(workers).resolveExactModel(
                "catalog.embed", required, USER))
                .isInstanceOf(NoAvailableWorkerException.class)
                .hasMessageContaining("catalog is unavailable");

        ConcurrentMapEmbeddingModelRegistry registry = new ConcurrentMapEmbeddingModelRegistry(
                "embedding_admin", "embedding_user");
        UserIdentity admin = new UserIdentity(
                "admin", Set.of("embedding_admin"), null, Instant.now());
        registry.create(embeddingDescriptor(availability), admin);

        assertThat(selector(workers, registry).resolveExactModel(
                "catalog.embed", required, admin))
                .isEqualTo(availability);

        UserIdentity unauthorized = new UserIdentity(
                "other", Set.of("other"), null, Instant.EPOCH);
        assertThatThrownBy(() -> selector(workers, registry).resolveExactModel(
                "catalog.embed", required, unauthorized))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("lacks privilege");

        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://example.test/embeddings-v2"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        EmbeddingModelDescriptor requested =
                org.pangrova.ai.control.embedding.EmbeddingModelRevision.applyUpdate(
                        registry.find("catalog.embed", admin).orElseThrow(), update);
        EmbeddingModelDescriptor replacement = new EmbeddingModelDescriptor(
                requested.name(), requested.route(), requested.contract(), requested.limits(),
                requested.version(), new EmbeddingModelDescriptor.DeploymentFence(
                        "model-v8", "08" + "00".repeat(31)));
        registry.alter(registry.prepareAlter("catalog.embed", admin), replacement, admin);
        assertThatThrownBy(() -> selector(workers, registry)
                .resolveExactModel("catalog.embed", required, admin))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("does not match catalog");
    }

    private static DefaultWorkerSelector selector(WorkerManager workers) {
        return selector(workers, null);
    }

    private static DefaultWorkerSelector selector(
            WorkerManager workers, EmbeddingModelRegistry registry) {
        return new DefaultWorkerSelector(
                workers,
                new TaskScheduler(new WorkerPerformanceScorer()),
                new SplitAssignmentManager(),
                new SpillBudget(0, ""),
                registry);
    }

    private static WorkerManager workerManager() {
        return new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    private static WorkerInfo worker(String id, ModelAvailability availability) {
        CapabilityRef capability = availability.getCapabilityContracts(0)
                .getCapability();
        CapabilityInventory inventory = TestWorkerInfoBuilder.canonicalInventory().toBuilder()
                .clearCapabilities().clearModels()
                .addCapabilities(CapabilityDescriptor.newBuilder()
                        .setRef(capability)
                        .setKind(CapabilityKind.CAPABILITY_KIND_MODEL_INVOCATION)
                        .setPlacement(ExecutionPlacement
                                .EXECUTION_PLACEMENT_WORKER_LOCAL_GOVERNED_CALLBACK)
                        .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                        .setRequiredPrivilege("AI_MODEL_USAGE"))
                .addModels(availability)
                .build();
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 9091,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                ONE_GIB, Instant.now(), 1.0,
                "default", 2, null, List.of(), null,
                inventory,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }

    private static ModelAvailability model(
            String id,
            String version,
            ByteString signature,
            CapabilityRef capability,
            CapabilityLimits limits) {
        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId(id)
                        .setVersion(version)
                        .setSignatureSha256(signature))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(capability)
                        .setOutputType(varcharOutput())
                        .setBindingContractSha256(ByteString.copyFrom(new byte[32])))
                .setLimits(limits)
                .build();
    }

    private static ModelAvailability embeddingModel(
            String id,
            String version,
            ByteString signature,
            CapabilityRef capability,
            CapabilityLimits limits,
            int dimension) {
        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId(id)
                        .setVersion(version)
                        .setSignatureSha256(signature))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(capability)
                        .setOutputType(org.pangrova.protocol.type.VectorLogicalType.create(dimension))
                        .setBindingContractSha256(ByteString.copyFrom(
                                EmbeddingModelBindingIdentity.sha256(
                                        embeddingDescriptor(id, limits)))))
                .setLimits(limits)
                .build();
    }

    private static EmbeddingModelDescriptor embeddingDescriptor(ModelAvailability availability) {
        EmbeddingModelDescriptor descriptor = embeddingDescriptor(
                availability.getModel().getId(), availability.getLimits());
        return new EmbeddingModelDescriptor(
                descriptor.name(), descriptor.route(), descriptor.contract(), descriptor.limits(),
                descriptor.version(),
                new EmbeddingModelDescriptor.DeploymentFence(
                        availability.getModel().getVersion(),
                        HexFormat.of().formatHex(
                                availability.getModel().getSignatureSha256().toByteArray())));
    }

    private static EmbeddingModelDescriptor embeddingDescriptor(
            String modelId, CapabilityLimits limits) {
        return new EmbeddingModelDescriptor(
                modelId,
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://example.test/embeddings", "vault://key"),
                new EmbeddingModelDescriptor.EmbeddingContract(768, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(
                        limits.getMaxBatchItems(), 100),
                0,
                EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    private static LogicalType varcharOutput() {
        return LogicalType.newBuilder()
                .setCanonicalSql("VARCHAR")
                .setSubstraitType(io.substrait.proto.Type.newBuilder()
                        .setString(io.substrait.proto.Type.String.newBuilder()
                                .setNullability(io.substrait.proto.Type.Nullability
                                        .NULLABILITY_NULLABLE)))
                .build();
    }

    private static CapabilityLimits validLimits() {
        return CapabilityLimits.newBuilder()
                .setMaxBatchItems(16)
                .setMaxInputTokens(8_192)
                .setMaxOutputTokens(1_024)
                .setMaxConcurrency(4)
                .setMaxProviderItemsPerTask(100)
                                .setMaxProviderAttemptsPerTask(100)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152)
                .build();
    }

    private static ByteString signature(int marker) {
        byte[] bytes = new byte[32];
        bytes[0] = (byte) marker;
        return ByteString.copyFrom(bytes);
    }
}
