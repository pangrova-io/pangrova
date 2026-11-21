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
package org.pangrova.ai.api.embedding;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.capability.AiRequestConstraints;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.api.enums.Modality;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingModelBindingIdentityTest {

    @Test
    void catalogAndWorkerDescriptorsProduceSameIdentity() {
        assertThat(EmbeddingModelBindingIdentity.sha256(catalog("vault://key")))
                .isEqualTo(EmbeddingModelBindingIdentity.sha256(worker("vault://key")));
    }

    @Test
    void credentialReferenceAndExecutionSettingsAreFenced() {
        byte[] baseline = EmbeddingModelBindingIdentity.sha256(catalog("vault://key"));

        assertThat(baseline).isNotEqualTo(
                EmbeddingModelBindingIdentity.sha256(catalog("vault://rotated")));
        assertThat(baseline).isNotEqualTo(
                EmbeddingModelBindingIdentity.sha256(workerWithPrefix("query-v2: ")));
    }

    @Test
    void explicitlyUnauthenticatedDeploymentHasStableDistinctIdentity() {
        AiModelDescriptor bearer = worker("vault://key");
        Map<String, String> noAuthProperties = new java.util.HashMap<>(
                bearer.properties());
        noAuthProperties.put("authentication", "none");
        AiModelDescriptor noAuth = new AiModelDescriptor(
                bearer.name(), bearer.version(), bearer.provider(),
                bearer.endpoint(), "", bearer.capabilities(), bearer.enabled(),
                noAuthProperties);

        assertThat(EmbeddingModelBindingIdentity.sha256(noAuth))
                .hasSize(32)
                .isEqualTo(EmbeddingModelBindingIdentity.sha256(noAuth))
                .isNotEqualTo(EmbeddingModelBindingIdentity.sha256(bearer));
        assertThatThrownBy(() -> new AiModelDescriptor(
                bearer.name(), bearer.version(), bearer.provider(),
                bearer.endpoint(), "vault://key", bearer.capabilities(),
                bearer.enabled(), noAuthProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authentication=none");
        noAuthProperties.put("authentication", "unknown");
        assertThatThrownBy(() -> new AiModelDescriptor(
                bearer.name(), bearer.version(), bearer.provider(),
                bearer.endpoint(), "vault://key", bearer.capabilities(),
                bearer.enabled(), noAuthProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authentication must be canonical");
    }

    private static EmbeddingModelDescriptor catalog(String secretReference) {
        return new EmbeddingModelDescriptor(
                "embed",
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://models.example/v1/embeddings",
                        secretReference),
                new EmbeddingModelDescriptor.EmbeddingContract(
                        768, "query: ", "passage: "),
                new EmbeddingModelDescriptor.ServingLimits(32, 100),
                0,
                EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    private static AiModelDescriptor worker(String secretReference) {
        return worker(secretReference, "query: ");
    }

    private static AiModelDescriptor workerWithPrefix(String queryPrefix) {
        return worker("vault://key", queryPrefix);
    }

    private static AiModelDescriptor worker(
            String secretReference, String queryPrefix) {
        AiCapabilityDescriptor capability = new AiCapabilityDescriptor(
                "ai:embedding", "v1", AiOperation.EMBEDDING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VECTOR(768)", List.of("VARCHAR"))),
                Set.of(AiExecutionMode.BATCH),
                Set.of(Modality.TEXT), 32,
                new AiCapabilityDescriptor.ResourceLimits(
                        new org.pangrova.ai.api.capability.AiRequestConstraints(8192, 1, 1048576, 4194304),
                        0, 4, 100, 300),
                true,
                new AiCapabilityDescriptor.RetryPolicy(
                        3, Duration.ofMillis(100), Duration.ofSeconds(1), Set.of()),
                Set.of("AI_MODEL_USAGE"));
        return new AiModelDescriptor(
                "embed", "model-v1", "openai-compatible",
                URI.create("https://models.example/v1/embeddings"),
                secretReference, List.of(capability), true,
                Map.of(
                        "embedding-dimension", "768",
                        "max-batch-size", "32",
                        "max-requests-per-second", "100.0",
                        "query-prefix", queryPrefix,
                        "document-prefix", "passage: "));
    }
}
