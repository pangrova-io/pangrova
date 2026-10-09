package org.pangrova.testing.e2e.ai;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiProviderDeploymentTest {
    @ParameterizedTest
    @ValueSource(strings = {"", "ollama/ollama:0.6.2", "sha256:abc", "sha256:ABC"})
    void mutableOrMalformedImageCannotEnterDeployment(String image) {
        assertThatThrownBy(() -> AiProviderDeployment.select(image))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("immutable SHA-256 image ID");
    }

    @Test
    void exactBaseAndBothModelDigestsAreRequired() {
        assertThat(AiProviderDeployment.expectedModels(labels())).containsExactlyInAnyOrderEntriesOf(
                Map.of(SharedAiProvider.PROVIDER_MODEL, "a".repeat(64),
                        SharedAiProvider.EMBEDDING_PROVIDER_MODEL, "b".repeat(64)));
        for (String key : labels().keySet()) {
            var incomplete = new HashMap<>(labels());
            incomplete.remove(key);
            assertThatThrownBy(() -> AiProviderDeployment.expectedModels(incomplete))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertThatThrownBy(() -> AiProviderDeployment.expectedModels(null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void wrongBaseVersionOrModelDigestCannotBecomeExpectedInventory() {
        var wrongVersion = new HashMap<>(labels());
        wrongVersion.put(AiProviderDeployment.LABEL_PREFIX + "ollama-version", "0.6.1");
        assertThatThrownBy(() -> AiProviderDeployment.expectedModels(wrongVersion))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("base deployment receipt");
        var wrongDigest = new HashMap<>(labels());
        wrongDigest.put(AiProviderDeployment.LABEL_PREFIX + "completion-digest", "tinyllama:latest");
        assertThatThrownBy(() -> AiProviderDeployment.expectedModels(wrongDigest))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("completion-digest");
    }

    private static Map<String, String> labels() {
        return Map.of(AiProviderDeployment.LABEL_PREFIX + "base-image", "sha256:" + "c".repeat(64),
                AiProviderDeployment.LABEL_PREFIX + "ollama-version", "0.6.2",
                AiProviderDeployment.LABEL_PREFIX + "completion-digest", "a".repeat(64),
                AiProviderDeployment.LABEL_PREFIX + "embedding-digest", "b".repeat(64));
    }
}
