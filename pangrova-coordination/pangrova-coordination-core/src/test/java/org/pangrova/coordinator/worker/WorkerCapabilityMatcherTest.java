package org.pangrova.coordinator.worker;

import org.pangrova.scheduler.core.placement.WorkerCapabilityMatcher;
import org.pangrova.scheduler.api.placement.HealthState;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.common.value.MemoryPressure;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerCapabilityMatcherTest {

    private static final ByteString SIGNATURE = ByteString.copyFrom(new byte[32]);
    private static final ByteString FORMAT_SCHEMA = ByteString.copyFrom(
            new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    @Test
    void matchesSemanticSignatureAndExactArtifactFormat() {
        WorkerInfo worker = workerWith(capability("native-build-a", format("native-build-a")));
        CapabilityRef planRef = CapabilityRef.newBuilder()
                .setCapabilityId("pangrova.artifact.build.diskann")
                .setSemanticVersion("1.0.0")
                .setSignatureSha256(SIGNATURE)
                .build();

        assertThat(WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), planRef, format("native-build-a"))).isTrue();
        assertThat(WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), planRef, format("native-build-b"))).isFalse();
        assertThat(WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), planRef.toBuilder()
                        .setImplementationBuildId("native-build-b").build(), null))
                .isFalse();
        assertThat(WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), planRef.toBuilder()
                        .setSignatureSha256(FORMAT_SCHEMA).build(), null))
                .isFalse();
    }

    @Test
    void rejectsIncompletePlanIdentityInsteadOfFallingBackToStringCapability() {
        WorkerInfo worker = workerWith(capability("native-build-a", format("native-build-a")));
        assertThatThrownBy(() -> WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), CapabilityRef.newBuilder()
                        .setCapabilityId("pangrova.artifact.build.diskann")
                        .build(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete");
    }

    @Test
    void matchesOperatorOnlyWhenExactModelServesTheSameCapability() {
        CapabilityDescriptor capability = capability(
                "native-build-a", format("native-build-a"));
        VersionedRef model = VersionedRef.newBuilder()
                .setId("reranker")
                .setVersion("v7")
                .setSignatureSha256(FORMAT_SCHEMA)
                .build();
        CapabilityInventory inventory = TestWorkerInfoBuilder.canonicalInventory().toBuilder()
                .addCapabilities(capability)
                .addModels(ModelAvailability.newBuilder()
                        .setModel(model)
                        .setLimits(org.pangrova.protocol.v2.CapabilityLimits
                                .newBuilder()
                                .setMaxBatchItems(16)
                                .setMaxInputTokens(8_192)
                                .setMaxConcurrency(4)
                                .setMaxProviderItemsPerTask(100)
                                .setMaxProviderAttemptsPerTask(100)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152))
                        .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                                .setCapability(capability.getRef())
                                .setOutputType(outputType())
                                .setBindingContractSha256(FORMAT_SCHEMA)))
                .build();
        WorkerInfo worker = workerWith(capability, inventory);
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId("rerank-1")
                .setCapability(capability.getRef().toBuilder()
                        .clearImplementationBuildId())
                .setModel(ModelBinding.newBuilder()
                        .setModel(model)
                        .setOutputType(outputType()))
                .build();

        assertThat(WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), List.of(binding))).isTrue();
        assertThat(WorkerCapabilityMatcher.supports(
                worker.capabilityInventory(), List.of(binding.toBuilder().setModel(
                        binding.getModel().toBuilder().setModel(
                                model.toBuilder().setVersion("v8"))).build())))
                .isFalse();
    }

    @Test
    void matchesBackgroundOutputAgainstItsExactWriterFormat() {
        CapabilityDescriptor capability = capability(
                "native-build-a", format("native-build-a"));
        WorkerInfo worker = workerWith(capability);
        OperatorBinding matching = OperatorBinding.newBuilder()
                .setPlanNodeId("artifact-output")
                .setCapability(capability.getRef().toBuilder()
                        .clearImplementationBuildId())
                .setOutputArtifact(ArtifactWriteBinding.newBuilder()
                        .setFormat(format("native-build-a")))
                .build();

        assertThat(WorkerCapabilityMatcher.supports(worker.capabilityInventory(), List.of(matching)))
                .isTrue();
        assertThat(WorkerCapabilityMatcher.supports(worker.capabilityInventory(), List.of(
                matching.toBuilder().setOutputArtifact(
                        matching.getOutputArtifact().toBuilder()
                                .setFormat(format("native-build-b"))).build())))
                .isFalse();
    }

    @Test
    void outputWriterSupportCannotHideAnUnsupportedInputFormat() {
        var advertised = capability("native-build-a", format("native-build-a"));
        var worker = workerWith(advertised);
        var input = org.pangrova.protocol.v2.ArtifactBinding.newBuilder()
                .setFormat(format("native-build-b"));
        var binding = OperatorBinding.newBuilder().setPlanNodeId("read-and-write")
                .setCapability(advertised.getRef())
                .setOutputArtifact(ArtifactWriteBinding.newBuilder().setFormat(format("native-build-a")))
                .addArtifacts(input).build();
        assertThat(WorkerCapabilityMatcher.supports(worker.capabilityInventory(), List.of(binding))).isFalse();
        var compatible = binding.toBuilder().setArtifacts(0,
                input.setFormat(format("native-build-a"))).build();
        assertThat(WorkerCapabilityMatcher.supports(worker.capabilityInventory(), List.of(compatible))).isTrue();
    }

    private static CapabilityDescriptor capability(
            String buildId, ArtifactFormatRef format) {
        return CapabilityDescriptor.newBuilder()
                .setRef(CapabilityRef.newBuilder()
                        .setCapabilityId("pangrova.artifact.build.diskann")
                        .setSemanticVersion("1.0.0")
                        .setImplementationBuildId(buildId)
                        .setSignatureSha256(SIGNATURE))
                .setKind(CapabilityKind.CAPABILITY_KIND_BACKGROUND_SINK)
                .setPlacement(ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE)
                .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                .setRequiredPrivilege("CREATE INDEX")
                .addSupportedArtifactFormats(format)
                .build();
    }

    private static ArtifactFormatRef format(String writerBuildId) {
        return ArtifactFormatRef.newBuilder()
                .setFormatId("pangrova.diskann.segment")
                .setMajorVersion(1)
                .setSchemaSha256(FORMAT_SCHEMA)
                .setWriterBuildId(writerBuildId)
                .build();
    }

    private static LogicalType outputType() {
        return LogicalType.newBuilder()
                .setCanonicalSql("DOUBLE")
                .setSubstraitType(io.substrait.proto.Type.newBuilder()
                        .setFp64(io.substrait.proto.Type.FP64.newBuilder()
                                .setNullability(io.substrait.proto.Type.Nullability
                                        .NULLABILITY_NULLABLE)))
                .build();
    }

    private static WorkerInfo workerWith(CapabilityDescriptor capability) {
        return workerWith(capability,
                TestWorkerInfoBuilder.canonicalInventory().toBuilder()
                        .addCapabilities(capability).build());
    }

    private static WorkerInfo workerWith(
            CapabilityDescriptor capability, CapabilityInventory inventory) {
        return new WorkerInfo(
                "worker-1", "host-1", 8080, 9090, 9191,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                1024L * 1024 * 1024, Instant.now(), 1.0,
                null, 2, null, List.of(), null,
                inventory,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity("worker-1"));
    }
}
