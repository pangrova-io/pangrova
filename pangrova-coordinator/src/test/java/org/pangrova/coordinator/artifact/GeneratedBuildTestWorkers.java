package org.pangrova.coordinator.artifact;

import com.google.protobuf.ByteString;
import org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector;
import org.pangrova.coordinator.ai.model.ExactModelResolver;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.security.UserIdentity;
import java.time.Instant;
import java.util.List;

/** Leaf Native capability contract inventory; not a live Worker. */
final class GeneratedBuildTestWorkers {
    private GeneratedBuildTestWorkers() {}

    static WorkerInfo worker(boolean withMaterializer) {
        CapabilityRef ai = BuiltInCapabilityCatalog.aiEmbeddingPlanRef().toBuilder()
                .setImplementationBuildId(BuiltInCapabilityCatalog.NATIVE_BUILD_ID)
                .build();
        CapabilityInventory actual = org.pangrova.coordinator.worker
                .TestWorkerInfoBuilder.canonicalInventory();
        CapabilityInventory.Builder inventory = actual.toBuilder().clearCapabilities();
        actual.getCapabilitiesList().stream()
                .filter(capability -> withMaterializer || !capability.getRef().getCapabilityId()
                        .equals(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef()
                                .getCapabilityId()))
                .forEach(inventory::addCapabilities);
        inventory
                .addCapabilities(CapabilityDescriptor.newBuilder()
                        .setRef(ai)
                        .setKind(CapabilityKind.CAPABILITY_KIND_MODEL_INVOCATION)
                        .setPlacement(ExecutionPlacement
                                .EXECUTION_PLACEMENT_WORKER_LOCAL_GOVERNED_CALLBACK)
                        .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                        .setRequiredPrivilege("AI_MODEL_USAGE"))
                .addModels(ModelAvailability.newBuilder()
                        .setModel(VersionedRef.newBuilder()
                                .setId("catalog.embed")
                                .setVersion("model-v7")
                                .setSignatureSha256(ByteString.copyFrom(
                                        new byte[32])))
                        .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                                .setCapability(ai)
                                .setOutputType(VectorLogicalType.create(768))
                                .setBindingContractSha256(ByteString.copyFrom(
                                        new byte[32])))
                        .setLimits(CapabilityLimits.newBuilder()
                                .setMaxBatchItems(32)
                                .setMaxInputBytes(1024 * 1024)
                                .setMaxInputTokens(512)
                                .setMaxConcurrency(4)
                                .setMaxProviderItemsPerTask(10_000)
                                .setMaxProviderAttemptsPerTask(10_000)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152)));
        return new WorkerInfo(
                "worker-1", "127.0.0.1", 8080, 8081, 8082,
                HealthState.ACTIVE, MemoryPressure.NONE, 0, 0,
                Long.MAX_VALUE, Instant.now(), 1, "default", 2,
                "rack-a", List.of(), null, inventory.build(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-1"));
    }

    static final class InventoryWorkerSelector implements WorkerSelector {
        private final WorkerInfo worker;
        UserIdentity resolvedUser;

        InventoryWorkerSelector(WorkerInfo worker) {
            this.worker = worker;
        }

        @Override
        public WorkerInfo select(String tenantId) {
            return worker;
        }

        @Override
        public StageResourceGrant computeResourceLimit(
                int parallelism, int concurrentStages, WorkerInfo selectedWorker) {
            return StageResourceGrant.newBuilder().setGrantId("test").build();
        }

        @Override
        public WorkerInfo selectForBindings(
                String tenantId,
                List<String> splitFilePaths,
                List<OperatorBinding> bindings) {
            return worker;
        }

        @Override
        public ModelAvailability resolveExactModel(
                String modelName,
                CapabilityRef capability,
                UserIdentity user) {
            resolvedUser = user;
            return ExactModelResolver.resolve(List.of(worker), modelName, capability);
        }
    }

}
