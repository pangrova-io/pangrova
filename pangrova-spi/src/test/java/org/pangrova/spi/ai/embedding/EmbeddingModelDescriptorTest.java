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
package org.pangrova.spi.ai.embedding;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingModelDescriptorTest {

    @Test
    void exposesImmutableGroupedConfiguration() {
        EmbeddingModelDescriptor descriptor = descriptor(
                new EmbeddingModelDescriptor.DeploymentFence(
                        "model-v7", "01" + "00".repeat(31)));

        assertThat(descriptor.provider()).isEqualTo("openai-compatible");
        assertThat(descriptor.dimension()).isEqualTo(768);
        assertThat(descriptor.maxBatchSize()).isEqualTo(32);
        assertThat(descriptor.isDeploymentBound()).isTrue();
    }

    @Test
    void unboundFenceRequiresBothIdentityPartsToBeAbsent() {
        EmbeddingModelDescriptor.DeploymentFence unbound =
                EmbeddingModelDescriptor.DeploymentFence.unbound();

        assertThat(unbound.isBound()).isFalse();
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.DeploymentFence("model-v7", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("present together");
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.DeploymentFence(
                "model-v7", "not-a-sha256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("64 lowercase hexadecimal");
    }

    @Test
    void rejectsMissingGroupsAndNonPositiveLimits() {
        EmbeddingModelDescriptor descriptor = descriptor(
                EmbeddingModelDescriptor.DeploymentFence.unbound());

        assertThatThrownBy(() -> new EmbeddingModelDescriptor(
                descriptor.name(), null, descriptor.contract(), descriptor.limits(),
                descriptor.version(), descriptor.deployment()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("route");
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.ServingLimits(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBatchSize");
    }

    @Test
    void canonicalEmptyCredentialIdentifiesUnauthenticatedRoute() {
        var anonymous = new EmbeddingModelDescriptor.ProviderRoute(
                "openai-compatible", "http://127.0.0.1:11434/v1/embeddings", "");
        assertThat(anonymous.unauthenticated()).isTrue();
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.ProviderRoute(
                "openai-compatible", "https://example.test/embeddings", "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("canonical empty value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "LOCALHOST", "127.0.0.1", "[::1]"})
    void httpRequiresAnExplicitAnonymousExactLoopbackRoute(String host) {
        String endpoint = "http://" + host + ":11434/v1/embeddings";
        assertThat(new EmbeddingModelDescriptor.ProviderRoute("provider", endpoint, "").unauthenticated()).isTrue();
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.ProviderRoute("provider", endpoint, "vault://key"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("HTTPS");
    }

    @Test
    void httpsAllowsBothCanonicalAuthenticationModes() {
        for (String credential : List.of("", "vault://key")) {
            var route = new EmbeddingModelDescriptor.ProviderRoute("provider", "HTTPS://example.test/embed", credential);
            assertThat(route.unauthenticated()).isEqualTo(credential.isEmpty());
            assertThat(route.apiKeySecret()).isEqualTo(credential);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://example.test/embed", "http://localhost.evil.test/embed", "http://127.0.0.2/embed",
            "ftp://localhost/embed", "/relative", "urn:opaque", "file:/tmp/model", "https://user:key@api.test/embed",
            "https://api.test/embed?token=secret", "https://api.test/embed?", "https://api.test/embed#fragment",
            "https://api.test/embed#", "https://bad host/embed"})
    void rejectsInsecureOrNonCanonicalUrisEvenWithoutCredentials(String endpoint) {
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.ProviderRoute("provider", endpoint, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingOrBlankRouteValuesKeepTheirExplicitValidationFailures() {
        for (String missing : Arrays.asList(null, "", " ")) {
            assertThatThrownBy(() -> new EmbeddingModelDescriptor.ProviderRoute(missing, "https://api.test/embed", ""))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("provider");
            assertThatThrownBy(() -> new EmbeddingModelDescriptor.ProviderRoute("provider", missing, ""))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("endpoint");
        }
        assertThatThrownBy(() -> new EmbeddingModelDescriptor.ProviderRoute("provider", "https://api.test/embed", null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("apiKeySecret");
    }

    private static EmbeddingModelDescriptor descriptor(
            EmbeddingModelDescriptor.DeploymentFence fence) {
        return new EmbeddingModelDescriptor(
                "catalog.embed",
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://example.test/embeddings", "vault://key"),
                new EmbeddingModelDescriptor.EmbeddingContract(768, "query: ", "passage: "),
                new EmbeddingModelDescriptor.ServingLimits(32, 100),
                0,
                fence);
    }
}
