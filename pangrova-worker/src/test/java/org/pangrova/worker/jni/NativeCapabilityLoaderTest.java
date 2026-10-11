package org.pangrova.worker.jni;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.CapabilityResponse;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.ProtocolVersion;
import org.pangrova.testing.testkit.function.ActualNativeCatalogue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeCapabilityLoaderTest {

    @Test
    void capturesMembershipFromActualNativeCatalogue() throws Exception {
        var inventory = CapabilityInventory.parseFrom(ActualNativeCatalogue.bytes());
        var relTypes = new java.util.HashSet<>(inventory.getSupportedSubstraitRelTypesList());
        var exprTypes = new java.util.HashSet<>(inventory.getSupportedSubstraitExpressionTypesList());
        var functions = new java.util.ArrayList<>(inventory.getFunctionsList());
        var captured = new NativeCapabilityLoader.ParsedCapabilities(
                relTypes, exprTypes, functions, inventory.getFunctionCatalog(),
                inventory.getNativeEngineVersion(), inventory);
        relTypes.clear();
        exprTypes.clear();
        functions.clear();
        assertThat(captured.relTypes()).containsExactlyInAnyOrderElementsOf(
                inventory.getSupportedSubstraitRelTypesList());
        assertThat(captured.exprTypes()).containsExactlyInAnyOrderElementsOf(
                inventory.getSupportedSubstraitExpressionTypesList());
        assertThat(captured.functions()).containsExactlyElementsOf(inventory.getFunctionsList());
        assertThat(captured.inventory()).isSameAs(inventory);
        assertThatThrownBy(() -> captured.functions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void acceptsInventoryBoundToCanonicalV2Descriptor() {
        NativeCapabilityLoader.ParsedCapabilities parsed =
                NativeCapabilityLoader.parseCapabilityResponse(
                        response(baseInventory()
                                .addSupportedSubstraitRelTypes("ReadRel")
                                .addSupportedSubstraitExpressionTypes("Literal")
                                .build()));

        assertThat(parsed.inventory().getProtocol().getMajor()).isEqualTo(2);
        assertThat(parsed.relTypes()).containsExactly("ReadRel");
        assertThat(parsed.exprTypes()).containsExactly("Literal");
        assertThat(parsed.engineVersion()).isEqualTo(baseInventory().getNativeEngineVersion());
        assertThat(parsed.functionCatalog()).isEqualTo(baseInventory().getFunctionCatalog());
        assertThat(parsed.functions()).containsExactlyElementsOf(baseInventory().getFunctionsList());
    }

    @Test
    void rejectsDescriptorDriftBeforeAdvertisingWorkerReady() {
        CapabilityInventory inventory = baseInventory()
                .setProtocol(baseProtocol()
                        .setDescriptorSetSha256(ByteString.copyFromUtf8("drift")))
                .build();
        assertThatThrownBy(() -> NativeCapabilityLoader.parseCapabilityResponse(
                response(inventory)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("canonical descriptor set");
    }

    @Test
    void rejectsRetrievalCapabilityWithoutTypedRelationContract() {
        CapabilityDescriptor retrieval = CapabilityDescriptor.newBuilder()
                .setRef(CapabilityRef.newBuilder()
                        .setCapabilityId("pangrova.retrieval.topk.exact")
                        .setSemanticVersion("1.0.0")
                        .setImplementationBuildId("native-test")
                        .setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .setKind(CapabilityKind.CAPABILITY_KIND_RETRIEVAL_SOURCE)
                .setPlacement(ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE)
                .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                .setRequiredPrivilege("SELECT")
                .setLimits(CapabilityLimits.newBuilder().setMaxCandidateRows(100))
                .build();
        CapabilityInventory inventory = baseInventory()
                .addCapabilities(retrieval)
                .setCandidateSchemaVersion(1)
                .build();

        assertThatThrownBy(() -> NativeCapabilityLoader.parseCapabilityResponse(
                response(inventory)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("typed relation contract");
    }

    @Test
    void rejectsDuplicateCapabilityIdentity() {
        CapabilityDescriptor descriptor = validDescriptor("duplicate");
        CapabilityInventory inventory = baseInventory()
                .addCapabilities(descriptor)
                .addCapabilities(descriptor)
                .build();
        assertThatThrownBy(() -> NativeCapabilityLoader.parseCapabilityResponse(
                response(inventory)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate capability identity");
    }

    @Test
    void rejectsMissingInventoryOrResultEnvelopeVersion() {
        assertThatThrownBy(() -> NativeCapabilityLoader.parseCapabilityResponse(
                CapabilityResponse.getDefaultInstance().toByteArray()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no inventory");

        CapabilityInventory inventory = baseInventory()
                .setResultEnvelopeVersion(0)
                .build();
        assertThatThrownBy(() -> NativeCapabilityLoader.parseCapabilityResponse(
                response(inventory)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("result envelope");
    }

    private static byte[] response(CapabilityInventory inventory) {
        return CapabilityResponse.newBuilder().setInventory(inventory).build().toByteArray();
    }

    private static CapabilityInventory.Builder baseInventory() {
        try {
            return CapabilityInventory.parseFrom(ActualNativeCatalogue.bytes()).toBuilder()
                    .clearCapabilities().clearModels().clearSupportedRelationTypeUrls()
                    .clearSupportedSubstraitRelTypes().clearSupportedSubstraitExpressionTypes();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Actual Native catalogue exporter artifact is malformed", failure);
        }
    }

    private static ProtocolVersion.Builder baseProtocol() {
        return ProtocolVersion.newBuilder()
                .setMajor(2)
                .setMinor(0)
                .setDescriptorSetSha256(ByteString.copyFrom(
                        CanonicalProtocolDescriptors.sha256()))
                .setRolloutEpoch("test");
    }

    private static CapabilityDescriptor validDescriptor(String id) {
        return CapabilityDescriptor.newBuilder()
                .setRef(CapabilityRef.newBuilder()
                        .setCapabilityId(id)
                        .setSemanticVersion("1.0.0")
                        .setImplementationBuildId("native-test")
                        .setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .setKind(CapabilityKind.CAPABILITY_KIND_SCALAR)
                .setPlacement(ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE)
                .addExecutionModes(ExecutionMode.EXECUTION_MODE_BATCH)
                .setRequiredPrivilege("SELECT")
                .build();
    }
}
