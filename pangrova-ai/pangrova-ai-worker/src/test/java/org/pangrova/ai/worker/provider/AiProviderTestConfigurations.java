package org.pangrova.ai.worker.provider;

import java.util.Map;

/** Explicit complete model settings for registry contract tests; no missing-property defaults. */
final class AiProviderTestConfigurations {
    private AiProviderTestConfigurations() {}

    static Map<String, String> inferenceModelConfig() {
        return Map.ofEntries(
                Map.entry("models.chat.provider", "openai-compatible"),
                Map.entry("models.chat.provider-model", "chat-deployment"),
                Map.entry("models.chat.version", "1"),
                Map.entry("models.chat.endpoint", "https://models.example/v1/chat/completions"),
                Map.entry("models.chat.secret-reference", "env://KEY"),
                Map.entry("models.chat.capabilities", "inference"),
                Map.entry("models.chat.max-input-tokens", "8192"),
                Map.entry("models.chat.max-output-tokens", "1024"),
                Map.entry("models.chat.max-concurrency", "4"),
                Map.entry("models.chat.max-provider-attempts-per-task", "4096"),
                Map.entry("models.chat.max-request-bytes", "262144"),
                Map.entry("models.chat.max-response-bytes", "1048576"),
                Map.entry("models.chat.max-provider-items-per-task", "1024"));
    }

    static Map<String, String> multimodalConfig(
            String provider, String endpoint, String modalities, String maxBatchSize) {
        return Map.ofEntries(
                Map.entry("models.media.provider", provider),
                Map.entry("models.media.provider-model", "provider-deployment-pinned"),
                Map.entry("models.media.version", "sha256:deployment"),
                Map.entry("models.media.endpoint", endpoint),
                Map.entry("models.media.secret-reference", "env://MEDIA_KEY"),
                Map.entry("models.media.capabilities", "multimodal"),
                Map.entry("models.media.modalities", modalities),
                Map.entry("models.media.max-batch-size", maxBatchSize),
                Map.entry("models.media.max-input-tokens", "8192"),
                Map.entry("models.media.max-output-tokens", "4096"),
                Map.entry("models.media.max-media-bytes", "16777216"),
                Map.entry("models.media.max-concurrency", "4"),
                Map.entry("models.media.max-provider-attempts-per-task", "4096"),
                Map.entry("models.media.max-request-bytes", "262144"),
                Map.entry("models.media.max-response-bytes", "1048576"),
                Map.entry("models.media.max-provider-items-per-task", "1024"));
    }

}
