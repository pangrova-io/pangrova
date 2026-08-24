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
package org.pangrova.coordinator.ai.embedding.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.ai.embedding.ConcurrentMapEmbeddingModelRegistry;
import org.pangrova.coordinator.ai.embedding.EmbeddingModelNotReadyException;
import org.pangrova.coordinator.ai.embedding.EmbeddingModelRegistry;
import org.pangrova.security.secret.SecretResolver;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RegistryBackedEmbeddingProvider} default-model resolution
 * (P1-1 semantic-cache embedding wiring). Uses the real in-memory registry and a
 * real {@link SecretResolver} (no mocks, per project testing rules). HTTP paths are
 * not exercised here — these tests assert the lazy resolution contract that gates
 * whether the semantic cache activates.
 */
class RegistryBackedEmbeddingProviderTest {

    private static final UserIdentity ADMIN =
            new UserIdentity("admin", Set.of("admin"), Map.of(), Instant.EPOCH);

    private static EmbeddingModelDescriptor model(String name, int dimension) {
        return new EmbeddingModelDescriptor(
                name, "openai-compatible", "http://localhost:1/v1/embeddings",
                dimension, "sk-test", 16, 10, "", "", 0);
    }

    private static RegistryBackedEmbeddingProvider provider(EmbeddingModelRegistry registry) {
        return new RegistryBackedEmbeddingProvider(registry, new SecretResolver());
    }

    @Test
    @DisplayName("no registered model -> embed signals not-ready (semantic cache stays inert)")
    void noModelSignalsNotReady() {
        var registry = new ConcurrentMapEmbeddingModelRegistry("admin", "users");
        assertThatThrownBy(() -> provider(registry).embed("select 1"))
                .isInstanceOf(EmbeddingModelNotReadyException.class)
                .hasMessageContaining("no embedding model");
    }

    @Test
    @DisplayName("more than one model with no default -> embed signals not-ready")
    void ambiguousModelsSignalNotReady() {
        var registry = new ConcurrentMapEmbeddingModelRegistry("admin", "users");
        registry.create(model("m1", 8), ADMIN);
        registry.create(model("m2", 8), ADMIN);
        assertThatThrownBy(() -> provider(registry).embed("select 1"))
                .isInstanceOf(EmbeddingModelNotReadyException.class)
                .hasMessageContaining("multiple embedding models");
    }

    @Test
    @DisplayName("exactly one model -> metadata resolves lazily to that model's config")
    void soleModelResolvesMetadata() {
        var registry = new ConcurrentMapEmbeddingModelRegistry("admin", "users");
        registry.create(model("text-embed-3", 1536), ADMIN);
        var provider = provider(registry);
        assertThat(provider.modelName()).isEqualTo("text-embed-3");
        assertThat(provider.dimensions()).isEqualTo(1536);
        assertThat(provider.maxBatchSize()).isEqualTo(16);
    }

    @Test
    @DisplayName("a model registered after construction is picked up (runtime CREATE MODEL)")
    void resolvesModelRegisteredAfterConstruction() {
        var registry = new ConcurrentMapEmbeddingModelRegistry("admin", "users");
        var provider = provider(registry);
        assertThatThrownBy(provider::modelName)
                .isInstanceOf(EmbeddingModelNotReadyException.class);
        registry.create(model("late-model", 64), ADMIN);
        assertThat(provider.modelName()).isEqualTo("late-model");
        assertThat(provider.dimensions()).isEqualTo(64);
    }

    @Test
    @DisplayName("blob embedding is worker-side only -> unsupported here")
    void blobEmbeddingUnsupported() {
        var registry = new ConcurrentMapEmbeddingModelRegistry("admin", "users");
        registry.create(model("m1", 8), ADMIN);
        assertThatThrownBy(() -> provider(registry).embedBlob(java.util.List.of()))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
