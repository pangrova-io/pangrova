package org.pangrova.testing.e2e.jni;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityResponse;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.ai.SharedAiProvider;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.jni.NativeRuntime;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Packaged-library verification of the canonical JVM/Native handshake. */
@Tag("e2e")
class NativeCapabilityHandshakeE2ETest extends SqlE2ETestBase {

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();
    }

    @Test
    void packagedLibraryReturnsMatchingDependencyCompleteDescriptor() throws Exception {
        byte[] response = NativeRuntime.nativeCommand(
                0, NativeCommandBuilder.buildGetCapabilities());
        byte[] payload = NativeCommandBuilder.extractResultPayload(response);
        CapabilityInventory inventory = CapabilityResponse.parseFrom(payload).getInventory();
        assertThat(inventory.getProtocol().getMajor()).isEqualTo(2);
        assertThat(inventory.getNativeEngineVersion()).isNotBlank();
        assertThat(inventory.getSupportedSubstraitRelTypesList()).isNotEmpty();
        assertThat(inventory.getSupportedSubstraitExpressionTypesList()).isNotEmpty();
        assertThat(inventory.getProtocol().getDescriptorSetSha256().toByteArray())
                .isEqualTo(CanonicalProtocolDescriptors.sha256());
        assertThat(inventory.getCapabilitiesList())
                .extracting(descriptor -> descriptor.getRef().getCapabilityId())
                .contains(
                        "pangrova.retrieval.topk.exact",
                        "pangrova.retrieval.merge.rrf",
                        "pangrova.retrieval.merge.linear",
                        "pangrova.artifact.build.diskann",
                        "pangrova.artifact.build.hnsw",
                        "pangrova.artifact.build.ivf_pq",
                        "pangrova.artifact.build.bm25",
                        "pangrova.artifact.build.generated_values");
        assertThat(inventory.getCapabilitiesList()).allSatisfy(descriptor -> {
            assertThat(descriptor.getRef().getSignatureSha256().size()).isEqualTo(32);
            assertThat(descriptor.getPlacement()).isIn(
                    ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE,
                    ExecutionPlacement.EXECUTION_PLACEMENT_WORKER_LOCAL_GOVERNED_CALLBACK);
        });
        assertThat(inventory.getCapabilitiesList())
                .filteredOn(descriptor -> descriptor.getRef().getCapabilityId()
                        .equals("pangrova.retrieval.topk.exact"))
                .singleElement()
                .satisfies(descriptor -> {
                    assertThat(descriptor.getPlacement())
                            .isEqualTo(ExecutionPlacement.EXECUTION_PLACEMENT_NATIVE);
                    assertThat(descriptor.getLimits().getMaxCandidateRows()).isPositive();
                });
        assertThat(inventory.getSupportedRelationTypeUrlsList()).containsExactly(
                "type.googleapis.com/pangrova.protocol.v2.RelationDetailEnvelope");
        assertThat(inventory.getCandidateSchemaVersion()).isEqualTo(2);
        CapabilityInventoryCodec.validate(inventory);
        CapabilityInventory publication = e2eNativeCapabilityInventory();
        CapabilityInventoryCodec.validate(publication);
        assertThat(publication.toBuilder().clearModels().build())
                .as("Worker publication must retain the entire negotiated Native inventory")
                .isEqualTo(inventory.toBuilder().clearModels().build());
        assertThat(publication.getModelsList()).extracting(model -> model.getModel().getId())
                .containsExactlyInAnyOrder(SharedAiProvider.LOGICAL_MODEL, SharedAiProvider.EMBEDDING_LOGICAL_MODEL);
        String output = System.getProperty("pangrova.e2e.native-inventory-output");
        if (output != null) {
            Path destination = Path.of(output);
            if (!destination.isAbsolute()) {
                throw new IllegalArgumentException("Native inventory output must be an absolute path");
            }
            Files.write(destination, publication.toByteArray());
        }
    }

}
