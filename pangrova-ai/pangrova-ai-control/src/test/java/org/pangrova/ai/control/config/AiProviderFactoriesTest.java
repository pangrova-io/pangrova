package org.pangrova.ai.control.config;

import java.net.URI;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.provider.AiProviderFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiProviderFactoriesTest {
    @Test
    void discoversIndependentProvidersAndAllRegisteredDialects() {
        var installed = AiProviderFactories.discover();
        var openAi = installed.require("openai-compatible", "chat");

        assertThat(installed.require("openai-responses", "chat")).isSameAs(openAi);
        assertThat(installed.require("openai-transcription", "audio")).isSameAs(openAi);
        assertThat(installed.require("qwen", "chat")).isNotSameAs(openAi);
    }

    @Test
    void rejectsDuplicateDialectsAtDiscovery() {
        var first = new Factory(AiProviderFactory.API_VERSION);
        var second = new Factory(AiProviderFactory.API_VERSION);
        assertThatThrownBy(() -> new AiProviderFactories(List.of(first, second)))
                .hasMessageContaining("duplicate AI provider dialect");
    }

    @Test
    void rejectsIncompatibleApiVersionsBeforeClientCreation() {
        assertThatThrownBy(() -> new AiProviderFactories(List.of(new Factory(1))))
                .hasMessageContaining("Unsupported AI factory API version 1");
    }

    private record Factory(int apiVersion) implements AiProviderFactory {
        @Override public Set<String> dialects() { return Set.of("test"); }

        @Override public void validate(String dialect, URI endpoint, Set<Operation> capabilities,
                Set<Modality> modalities, int maxBatchSize, String modelName) {
            throw new AssertionError("Discovery must not validate an unconfigured model");
        }
    }
}
