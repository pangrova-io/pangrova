package org.pangrova.protocol;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
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
        return CapabilityInventory.newBuilder()
                .setProtocol(protocol())
                .setResultEnvelopeVersion(1)
                .setNativeEngineVersion("native-test");
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
