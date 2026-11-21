/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.control.model;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.ai.api.embedding.EmbeddingModelBindingIdentity;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingModelExecutionAdmissionTest {

    @Test
    void bindsAndRequiresExactWorkerDeploymentContract() {
        EmbeddingModelDescriptor bound = EmbeddingModelExecutionAdmission.bind(
                descriptor(768, 32), availability("model-v7", 1, 768, 32));

        assertThat(bound.deploymentVersion()).isEqualTo("model-v7");
        assertThat(bound.deploymentSignatureSha256())
                .isEqualTo("01" + "00".repeat(31));
        EmbeddingModelExecutionAdmission.requireCompatible(
                bound, availability("model-v7", 1, 768, 32));
    }

    @Test
    void rejectsUnboundOrChangedDeploymentIdentity() {
        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.requireCompatible(
                descriptor(768, 32), availability("model-v7", 1, 768, 32)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("not bound");

        EmbeddingModelDescriptor bound = EmbeddingModelExecutionAdmission.bind(
                descriptor(768, 32), availability("model-v7", 1, 768, 32));
        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.requireCompatible(
                bound, availability("model-v8", 2, 768, 32)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("does not match catalog revision");
    }

    @Test
    void rejectsDimensionAndBatchContractDrift() {
        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.bind(
                descriptor(768, 32), availability("model-v7", 1, 384, 32)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("dimension 384");
        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.bind(
                descriptor(768, 32), availability("model-v7", 1, 768, 64)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("max batch size");
    }

    @Test
    void rejectsProviderRouteCredentialAndPrefixDrift() {
        ModelAvailability availability = availability("model-v7", 1, 768, 32);
        EmbeddingModelDescriptor baseline = descriptor(768, 32);

        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.bind(
                withRoute(baseline, "https://other.test/embeddings", "vault://key"),
                availability))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("execution configuration");
        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.bind(
                withRoute(baseline, baseline.endpoint(), "vault://rotated"), availability))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("execution configuration");
        EmbeddingModelDescriptor prefixDrift = new EmbeddingModelDescriptor(
                baseline.name(), baseline.route(),
                new EmbeddingModelDescriptor.EmbeddingContract(
                        baseline.dimension(), "query-v2: ", baseline.documentPrefix()),
                baseline.limits(), baseline.version(), baseline.deployment());
        assertThatThrownBy(() -> EmbeddingModelExecutionAdmission.bind(
                prefixDrift, availability))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("execution configuration");
    }

    private static EmbeddingModelDescriptor withRoute(
            EmbeddingModelDescriptor baseline,
            String endpoint,
            String secretReference) {
        return new EmbeddingModelDescriptor(
                baseline.name(),
                new EmbeddingModelDescriptor.ProviderRoute(
                        baseline.provider(), endpoint, secretReference),
                baseline.contract(), baseline.limits(), baseline.version(),
                baseline.deployment());
    }

    private static EmbeddingModelDescriptor descriptor(int dimension, int maxBatchSize) {
        return new EmbeddingModelDescriptor(
                "catalog.embed",
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://example.test/embeddings", "vault://key"),
                new EmbeddingModelDescriptor.EmbeddingContract(dimension, "query: ", "passage: "),
                new EmbeddingModelDescriptor.ServingLimits(maxBatchSize, 100),
                0,
                EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    private static ModelAvailability availability(
            String version, int signatureMarker, int dimension, int maxBatchSize) {
        byte[] signature = new byte[32];
        signature[0] = (byte) signatureMarker;
        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId("catalog.embed")
                        .setVersion(version)
                        .setSignatureSha256(ByteString.copyFrom(signature)))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                        .setOutputType(VectorLogicalType.create(dimension))
                        .setBindingContractSha256(ByteString.copyFrom(
                                EmbeddingModelBindingIdentity.sha256(
                                        descriptor(768, 32)))))
                .setLimits(CapabilityLimits.newBuilder()
                        .setMaxBatchItems(maxBatchSize)
                        .setMaxInputTokens(8_192)
                        .setMaxOutputTokens(1)
                        .setMaxConcurrency(4)
                        .setMaxProviderItemsPerTask(100)
                                .setMaxProviderAttemptsPerTask(100)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152))
                .build();
    }
}
