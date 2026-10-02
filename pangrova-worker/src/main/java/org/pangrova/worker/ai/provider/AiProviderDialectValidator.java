package org.pangrova.worker.ai.provider;

import org.pangrova.spi.ai.Modality;

import java.net.URI;
import java.util.Set;

/** Validates exact capability, modality and endpoint contracts per provider dialect. */
final class AiProviderDialectValidator {

    private static final String OPENAI_COMPATIBLE = "openai-compatible";
    private static final String OPENAI_RESPONSES = "openai-responses";
    private static final String OPENAI_TRANSCRIPTION = "openai-transcription";

    private AiProviderDialectValidator() {}

    static void validate(
            String provider,
            String endpoint,
            Set<String> capabilityNames,
            Set<Modality> modalities,
            int maxBatchSize,
            String modelName) {
        switch (provider) {
            case OPENAI_COMPATIBLE -> compatible(
                    endpoint, capabilityNames, modalities, modelName);
            case OPENAI_RESPONSES -> responses(
                    endpoint, capabilityNames, modalities, modelName);
            case OPENAI_TRANSCRIPTION -> transcription(
                    endpoint, capabilityNames, modalities, maxBatchSize, modelName);
            default -> throw failure(
                    "Unsupported AI provider '" + provider + "' for model '"
                            + modelName + "'");
        }
    }

    private static void compatible(
            String endpoint,
            Set<String> capabilities,
            Set<Modality> modalities,
            String modelName) {
        if (capabilities.contains("embedding")
                && !modalities.equals(Set.of(Modality.TEXT))) {
            throw failure(
                    "OpenAI-compatible embeddings for model '" + modelName
                            + "' support only TEXT inputs");
        }
        if (capabilities.contains("multimodal") && hasOutside(
                modalities, Set.of(Modality.TEXT, Modality.IMAGE))) {
            throw failure(
                    "OpenAI chat-completions for model '" + modelName
                            + "' supports only TEXT and IMAGE inputs");
        }
        String endpointPath = compatibleEndpointPath(capabilities, modelName);
        requirePath(endpoint, endpointPath, modelName);
    }

    private static String compatibleEndpointPath(
            Set<String> capabilities, String modelName) {
        if (capabilities.equals(Set.of("embedding"))) {
            return "/v1/embeddings";
        }
        if (capabilities.equals(Set.of("reranking"))) {
            return "/v1/rerank";
        }
        if (!capabilities.isEmpty()
                && Set.of("inference", "multimodal").containsAll(capabilities)) {
            return "/v1/chat/completions";
        }
        throw failure(
                "OpenAI-compatible model '" + modelName
                        + "' cannot combine chat, embedding, or reranking endpoint contracts");
    }

    private static void responses(
            String endpoint,
            Set<String> capabilities,
            Set<Modality> modalities,
            String modelName) {
        if (hasOutside(capabilities, Set.of("inference", "multimodal"))) {
            throw failure(
                    "OpenAI Responses model '" + modelName
                            + "' supports only inference and multimodal capabilities");
        }
        if (hasOutside(
                modalities,
                Set.of(Modality.TEXT, Modality.IMAGE, Modality.DOCUMENT))) {
            throw failure(
                    "OpenAI Responses model '" + modelName
                            + "' supports only TEXT, IMAGE and DOCUMENT inputs");
        }
        if (!capabilities.contains("multimodal")
                && !modalities.equals(Set.of(Modality.TEXT))) {
            throw failure(
                    "OpenAI Responses model '" + modelName
                            + "' needs the multimodal capability for IMAGE or DOCUMENT inputs");
        }
        requirePath(endpoint, "/v1/responses", modelName);
    }

    private static void transcription(
            String endpoint,
            Set<String> capabilities,
            Set<Modality> modalities,
            int maxBatchSize,
            String modelName) {
        if (!capabilities.equals(Set.of("multimodal"))) {
            throw failure(
                    "OpenAI transcription model '" + modelName
                            + "' must declare only the multimodal capability");
        }
        if (!modalities.equals(Set.of(Modality.TEXT, Modality.AUDIO))) {
            throw failure(
                    "OpenAI transcription model '" + modelName
                            + "' modalities must be exactly TEXT,AUDIO");
        }
        if (maxBatchSize != 1) {
            throw failure(
                    "OpenAI transcription model '" + modelName
                            + "' max-batch-size must be 1");
        }
        requirePath(endpoint, "/v1/audio/transcriptions", modelName);
    }

    private static <T> boolean hasOutside(Set<T> actual, Set<T> supported) {
        return !supported.containsAll(actual);
    }

    private static void requirePath(
            String endpoint, String expectedPath, String modelName) {
        if (!expectedPath.equals(URI.create(endpoint).normalize().getPath())) {
            throw failure(
                    "AI model '" + modelName + "' endpoint path must be " + expectedPath);
        }
    }

    private static IllegalArgumentException failure(String message) {
        return new IllegalArgumentException(message);
    }
}
