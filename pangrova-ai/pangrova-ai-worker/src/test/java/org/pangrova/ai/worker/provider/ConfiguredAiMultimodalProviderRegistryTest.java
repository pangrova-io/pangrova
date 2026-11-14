package org.pangrova.ai.worker.provider;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.ai.worker.provider.AiProviderTestConfigurations.inferenceModelConfig;
import static org.pangrova.ai.worker.provider.AiProviderTestConfigurations.multimodalConfig;

class ConfiguredAiMultimodalProviderRegistryTest {

    @Test
    void rejectsMultimodalAdvertisementWithoutGovernedBlobLoader() {
        var config = new java.util.LinkedHashMap<>(inferenceModelConfig());
        config.put("models.chat.capabilities", "inference,multimodal");
        config.put("models.chat.modalities", "text,image");
        config.put("models.chat.max-media-bytes", "1048576");

        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(config))
                .hasMessageContaining("no governed FILE content loader");
    }

    @Test
    void rejectsUnsupportedChatCompletionDocumentModality() {
        var config = new java.util.LinkedHashMap<>(inferenceModelConfig());
        config.put("models.chat.capabilities", "inference,multimodal");
        config.put("models.chat.modalities", "text,image,document");
        config.put("models.chat.max-media-bytes", "1048576");

        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(withFileStorage(config)))
                .hasMessageContaining("supports only TEXT and IMAGE");
    }

    @Test
    void admitsResponsesImageAndDocumentDialectWithExactEndpoint() {
        var config = multimodalConfig(
                "openai-responses",
                "https://api.example/v1/responses",
                "text,image,document",
                "4");

        var model = new WorkerAiModelCatalog().describeModels(withFileStorage(config))
                .iterator().next();

        assertThat(model.provider()).isEqualTo("openai-responses");
        assertThat(model.capabilities())
                .extracting(AiCapabilityDescriptor::operation)
                .containsExactly(AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION);
        try (var registry = new ConfiguredAiProviderRegistry(
                config, ignored -> "key", ignored -> new byte[0])) {
            assertThat(registry.resolveInference("media").supportedModalities())
                    .containsExactlyInAnyOrder(
                            org.pangrova.ai.api.enums.Modality.TEXT,
                            org.pangrova.ai.api.enums.Modality.IMAGE,
                            org.pangrova.ai.api.enums.Modality.DOCUMENT);
        }
    }

    @Test
    void admitsTranscriptionOnlyForExactAudioDialect() {
        var config = multimodalConfig(
                "openai-transcription",
                "https://api.example/v1/audio/transcriptions",
                "text,audio",
                "1");

        var model = new WorkerAiModelCatalog().describeModels(withFileStorage(config))
                .iterator().next();

        assertThat(model.provider()).isEqualTo("openai-transcription");
        try (var registry = new ConfiguredAiProviderRegistry(
                config, ignored -> "key", ignored -> new byte[0])) {
            assertThat(registry.resolveInference("media").supportedModalities())
                    .containsExactlyInAnyOrder(
                            org.pangrova.ai.api.enums.Modality.TEXT,
                            org.pangrova.ai.api.enums.Modality.AUDIO);
        }
    }

    @Test
    void rejectsDialectCapabilityModalityAndEndpointDrift() {
        var responsesAudio = multimodalConfig(
                "openai-responses",
                "https://api.example/v1/responses",
                "text,audio",
                "4");
        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(withFileStorage(responsesAudio)))
                .hasMessageContaining("TEXT, IMAGE and DOCUMENT");

        var transcriptionBatch = multimodalConfig(
                "openai-transcription",
                "https://api.example/v1/audio/transcriptions",
                "text,audio",
                "2");
        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(withFileStorage(transcriptionBatch)))
                .hasMessageContaining("max-batch-size must be 1");

        var wrongEndpoint = multimodalConfig(
                "openai-transcription",
                "https://api.example/v1/chat/completions",
                "text,audio",
                "1");
        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(withFileStorage(wrongEndpoint)))
                .hasMessageContaining("endpoint path must be /v1/audio/transcriptions");

        var endpointQuery = multimodalConfig(
                "openai-responses",
                "https://api.example/v1/responses?api-version=2026-09-01",
                "text,image,document",
                "4");
        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(withFileStorage(endpointQuery)))
                .hasMessageContaining("without user-info, query or fragment");
    }

    @Test
    void publishesMultimodalDescriptorWhenGovernedBlobLoaderIsInstalled() {
        var config = new java.util.LinkedHashMap<>(inferenceModelConfig());
        config.put("models.chat.capabilities", "inference,multimodal");
        config.put("models.chat.modalities", "text,image");
        config.put("models.chat.max-media-bytes", "1048576");
        // Blob-capable models must consent to the exact egress host.
        config.put("models.chat.authorization.egress-hosts", "models.example");

        var model = new WorkerAiModelCatalog().describeModels(withFileStorage(config)).iterator().next();

        assertThat(model.capabilities())
                .extracting(AiCapabilityDescriptor::operation)
                .containsExactlyInAnyOrder(
                        AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                        AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION);
        try (var registry = new ConfiguredAiProviderRegistry(
                config, reference -> "key", reference -> new byte[0])) {
            assertThat(registry.resolveInference("chat").supportedModalities())
                    .containsExactlyInAnyOrder(
                            org.pangrova.ai.api.enums.Modality.TEXT,
                            org.pangrova.ai.api.enums.Modality.IMAGE);
        }
    }

    @Test
    void rejectsNonTextOpenAiCompatibleEmbedding() {
        var config = Map.ofEntries(
                Map.entry("models.embed.provider", "openai-compatible"),
                Map.entry("models.embed.provider-model", "embed-deployment"),
                Map.entry("models.embed.version", "1"),
                Map.entry("models.embed.endpoint", "https://models.example/v1/embeddings"),
                Map.entry("models.embed.secret-reference", "env://KEY"),
                Map.entry("models.embed.capabilities", "embedding"),
                Map.entry("models.embed.modalities", "text,image"),
                Map.entry("models.embed.max-input-tokens", "8192"),
                Map.entry("models.embed.max-concurrency", "4"),
                Map.entry("models.embed.max-provider-attempts-per-task", "4096"),
                Map.entry("models.embed.max-request-bytes", "262144"),
                Map.entry("models.embed.max-response-bytes", "1048576"),
                Map.entry("models.embed.max-provider-items-per-task", "1024"),
                Map.entry("models.embed.embedding-dimension", "384"));

        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(config))
                .hasMessageContaining("support only TEXT inputs");
    }

    @Test
    void rejectsNonTextEmbeddingEvenWhenBlobLoaderExists() {
        var config = Map.ofEntries(
                Map.entry("models.embed.provider", "openai-compatible"),
                Map.entry("models.embed.provider-model", "embed-deployment"),
                Map.entry("models.embed.version", "1"),
                Map.entry("models.embed.endpoint", "https://models.example/v1/embeddings"),
                Map.entry("models.embed.secret-reference", "env://KEY"),
                Map.entry("models.embed.capabilities", "embedding"),
                Map.entry("models.embed.modalities", "text,image"),
                Map.entry("models.embed.max-input-tokens", "8192"),
                Map.entry("models.embed.max-concurrency", "4"),
                Map.entry("models.embed.max-provider-attempts-per-task", "4096"),
                Map.entry("models.embed.max-request-bytes", "262144"),
                Map.entry("models.embed.max-response-bytes", "1048576"),
                Map.entry("models.embed.max-provider-items-per-task", "1024"),
                Map.entry("models.embed.embedding-dimension", "384"));

        assertThatThrownBy(() -> new WorkerAiModelCatalog().describeModels(withFileStorage(config)))
                .hasMessageContaining("support only TEXT inputs");
    }

    private static Map<String, String> withFileStorage(Map<String, String> config) {
        var withStorage = new java.util.HashMap<>(config);
        withStorage.put("file.storage-backend", "local");
        return withStorage;
    }
}
