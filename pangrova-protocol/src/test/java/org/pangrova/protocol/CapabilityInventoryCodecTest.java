package org.pangrova.protocol;

import org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.ProtocolVersion;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.VersionedRef;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CapabilityInventoryCodecTest {

    @Test
    void encodedSizeLimitIsInclusiveAndIdenticalForAllProducerAndConsumerPaths() {
        var exact = inventorySized(3 * 1024 * 1024);
        String encoded = CapabilityInventoryCodec.encode(exact);
        assertThat(encoded).hasSize(4 * 1024 * 1024);
        assertThat(CapabilityInventoryCodec.decode(encoded)).isEqualTo(exact);

        var oversized = inventorySized(3 * 1024 * 1024 + 1);
        String oversizedEncoded = java.util.Base64.getEncoder().encodeToString(oversized.toByteArray());
        assertThat(oversizedEncoded).hasSize(4 * 1024 * 1024 + 4);
        assertThatThrownBy(() -> CapabilityInventoryCodec.validate(oversized))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maximum encoded size");
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(oversized))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maximum encoded size");
        assertThatThrownBy(() -> CapabilityInventoryCodec.decode(oversizedEncoded))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maximum encoded size");
    }

    private static CapabilityInventory inventorySized(int bytes) {
        var base = baseInventory().clearSupportedRelationTypeUrls().build();
        int length = bytes - base.getSerializedSize();
        CapabilityInventory inventory = base;
        for (int pass = 0; pass < 2; pass++) {
            inventory = base.toBuilder().addSupportedRelationTypeUrls("x".repeat(length)).build();
            length += bytes - inventory.getSerializedSize();
        }
        assertThat(inventory.getSerializedSize()).isEqualTo(bytes);
        return inventory;
    }

    @Test
    void losslesslyRoundTripsExactCapabilityAndArtifactIdentity() {
        CapabilityInventory inventory = baseInventory()
                .addCapabilities(validCapability())
                .build();

        CapabilityInventory decoded = CapabilityInventoryCodec.decode(
                CapabilityInventoryCodec.encode(inventory));

        assertThat(decoded).isEqualTo(inventory);
        assertThat(decoded.getCapabilities(0).getSupportedArtifactFormats(0)
                .getWriterBuildId()).isEqualTo("native-test");
    }

    @Test
    void materializationDoesNotRequireARetrievalCandidateSchemaOrLimit() {
        var materializer = validCapability().toBuilder()
                .setRef(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef().toBuilder()
                        .setImplementationBuildId("native-test"))
                .setKind(CapabilityKind.CAPABILITY_KIND_MATERIALIZER)
                .setLimits(org.pangrova.protocol.v2.CapabilityLimits.newBuilder()
                        .setMaxInputBytes(1_048_576).setMaxOutputBytes(1_048_576))
                .build();
        var inventory = baseInventory().clearCandidateSchemaVersion().clearSupportedRelationTypeUrls()
                .addCapabilities(materializer).build();

        assertThat(CapabilityInventoryCodec.decode(CapabilityInventoryCodec.encode(inventory)))
                .isEqualTo(inventory);
    }

    @ParameterizedTest
    @EnumSource(value = CapabilityKind.class, names = {"CAPABILITY_KIND_RETRIEVAL_SOURCE",
            "CAPABILITY_KIND_RETRIEVAL_MERGE", "CAPABILITY_KIND_RERANK"})
    void retrievalStillRequiresCandidateBoundsAndTheTypedRelationContract(CapabilityKind kind) {
        var descriptor = validCapability().toBuilder().setKind(kind).build();
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(
                baseInventory().addCapabilities(descriptor).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("candidate bound");
        var bounded = descriptor.toBuilder().setLimits(org.pangrova.protocol.v2.CapabilityLimits.newBuilder()
                .setMaxCandidateRows(100)).build();
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(baseInventory()
                .clearCandidateSchemaVersion().clearSupportedRelationTypeUrls().addCapabilities(bounded).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("typed relation contract");
    }

    @Test
    void rejectsDescriptorDriftAndMalformedBase64() {
        CapabilityInventory drift = baseInventory()
                .setProtocol(protocol().setDescriptorSetSha256(
                        ByteString.copyFrom(new byte[32])))
                .build();

        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(drift))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical descriptor set");
        assertThatThrownBy(() -> CapabilityInventoryCodec.decode("not-base64%%%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Base64");
    }

    @Test
    void rejectsIncompleteArtifactFormatAndDuplicateExactIdentity() {
        CapabilityDescriptor invalidFormat = validCapability().toBuilder()
                .clearSupportedArtifactFormats()
                .addSupportedArtifactFormats(ArtifactFormatRef.newBuilder()
                        .setFormatId("diskann"))
                .build();
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(
                baseInventory().addCapabilities(invalidFormat).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("artifact format identity");

        CapabilityDescriptor capability = validCapability();
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(
                baseInventory().addCapabilities(capability)
                        .addCapabilities(capability).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate capability identity");
    }

    @Test
    void modelAvailabilityMustReferenceAnExactAdvertisedCapability() {
        CapabilityDescriptor capability = validCapability();
        ModelAvailability model = ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId("reranker")
                        .setVersion("v1")
                        .setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .setLimits(org.pangrova.protocol.v2.CapabilityLimits.newBuilder()
                        .setMaxBatchItems(16)
                        .setMaxInputTokens(8_192)
                        .setMaxConcurrency(4)
                        .setMaxProviderItemsPerTask(1_024)
                                .setMaxProviderAttemptsPerTask(1_024)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152))
                .addCapabilityContracts(modelContract(capability.getRef()))
                .build();
        CapabilityInventory inventory = baseInventory()
                .addCapabilities(capability)
                .addModels(model)
                .build();

        assertThat(CapabilityInventoryCodec.decode(
                CapabilityInventoryCodec.encode(inventory))).isEqualTo(inventory);
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(
                baseInventory().addModels(model).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("absent from the same inventory");

        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(
                inventory.toBuilder().setModels(0, model.toBuilder()
                        .setLimits(model.getLimits().toBuilder()
                                .clearMaxProviderItemsPerTask())).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incomplete limits");

        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(
                inventory.toBuilder().setModels(0, model.toBuilder()
                        .addCapabilityContracts(modelContract(capability.getRef()))).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate capability");
    }

    private static CapabilityInventory.Builder baseInventory() {
        return CanonicalCapabilityInventoryTestFixture.inventory().toBuilder()
                .setProtocol(protocol())
                .clearCapabilities().clearModels();
    }

    private static ModelCapabilityContract modelContract(CapabilityRef capability) {
        return ModelCapabilityContract.newBuilder()
                .setCapability(capability)
                .setOutputType(LogicalType.newBuilder()
                        .setCanonicalSql("BIGINT")
                        .setSubstraitType(io.substrait.proto.Type.newBuilder()
                                .setI64(io.substrait.proto.Type.I64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability
                                                .NULLABILITY_NULLABLE))))
                .setBindingContractSha256(ByteString.copyFrom(new byte[32]))
                .build();
    }

    private static ProtocolVersion.Builder protocol() {
        return ProtocolVersion.newBuilder()
                .setMajor(2)
                .setMinor(0)
                .setDescriptorSetSha256(ByteString.copyFrom(
                        CanonicalProtocolDescriptors.sha256()))
                .setRolloutEpoch("test");
    }

    private static CapabilityDescriptor validCapability() {
        return CapabilityDescriptor.newBuilder()
                .setRef(CapabilityRef.newBuilder()
                        .setCapabilityId("pangrova.artifact.build.test")
                        .setSemanticVersion("1.0.0")
                        .setImplementationBuildId("native-test")
                        .setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .setKind(CapabilityKind.CAPABILITY_KIND_BACKGROUND_SINK)
                .setPlacement(ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE)
                .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                .setRequiredPrivilege("CREATE INDEX")
                .addSupportedArtifactFormats(ArtifactFormatRef.newBuilder()
                        .setFormatId("pangrova.test")
                        .setMajorVersion(1)
                        .setSchemaSha256(ByteString.copyFrom(new byte[32]))
                        .setWriterBuildId("native-test"))
                .build();
    }
}
