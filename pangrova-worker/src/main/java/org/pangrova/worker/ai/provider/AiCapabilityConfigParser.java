package org.pangrova.worker.ai.provider;

import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.Modality;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Builds canonical AI capabilities from model configuration. */
final class AiCapabilityConfigParser {

    private AiCapabilityConfigParser() {}

    static List<AiCapabilityDescriptor> parse(
            String raw,
            String modelName,
            String version,
            int maxBatchSize,
            Set<Modality> modalities,
            AiCapabilityDescriptor.ResourceLimits resourceLimits,
            boolean governedBlobLoaderAvailable) {
        List<AiCapabilityDescriptor> result = new ArrayList<>();
        for (String value : csv(raw)) {
            CapabilitySpec spec = switch (value.toLowerCase(Locale.ROOT)) {
                case "inference" -> inference(modelName, maxBatchSize);
                case "multimodal" -> {
                    if (!governedBlobLoaderAvailable) {
                        throw configurationFailure(
                                "AI model '" + modelName + "' cannot advertise multimodal: "
                                        + "no governed FILE content loader is installed");
                    }
                    yield multimodal(modelName, modalities);
                }
                case "embedding" -> embedding(modelName, maxBatchSize, modalities);
                case "reranking" -> reranking(modelName, maxBatchSize);
                default -> throw configurationFailure(
                        "Unknown capability '" + value + "' for AI model '" + modelName + "'");
            };
            result.add(capability(version, resourceLimits, spec));
        }
        if (result.isEmpty()) {
            throw configurationFailure("AI model '" + modelName + "' has no capabilities");
        }
        return List.copyOf(result);
    }

    static Set<String> csv(String raw) {
        if (raw == null || raw.isBlank()) {
            return Set.of();
        }
        var result = new java.util.LinkedHashSet<String>();
        for (String token : raw.split(",")) {
            String value = token.trim();
            if (!value.isEmpty()) {
                result.add(value);
            }
        }
        return Set.copyOf(result);
    }

    private static CapabilitySpec inference(String modelName, int maxBatchSize) {
        return new CapabilitySpec(
                "inference:" + modelName,
                AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("VARCHAR"))),
                Set.of(
                        AiCapabilityDescriptor.ExecutionMode.SCALAR,
                        AiCapabilityDescriptor.ExecutionMode.BATCH,
                        AiCapabilityDescriptor.ExecutionMode.SYNC),
                Set.of(Modality.TEXT),
                maxBatchSize,
                false);
    }

    private static CapabilitySpec multimodal(
            String modelName, Set<Modality> modalities) {
        boolean hasMedia = modalities.stream()
                .anyMatch(modality -> modality != Modality.TEXT);
        if (!hasMedia) {
            throw configurationFailure(
                    "AI model '" + modelName
                            + "' multimodal capability requires a non-text modality");
        }
        return new CapabilitySpec(
                "multimodal-inference:" + modelName,
                AiCapabilityDescriptor.Operation.MULTIMODAL_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("MULTIMODAL_PROMPT"))),
                Set.of(
                        AiCapabilityDescriptor.ExecutionMode.SCALAR,
                        AiCapabilityDescriptor.ExecutionMode.SYNC),
                modalities,
                1,
                false);
    }

    private static CapabilitySpec embedding(
            String modelName, int maxBatchSize, Set<Modality> modalities) {
        return new CapabilitySpec(
                "embedding:" + modelName,
                AiCapabilityDescriptor.Operation.EMBEDDING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VECTOR", List.of("VARCHAR"))),
                Set.of(
                        AiCapabilityDescriptor.ExecutionMode.SCALAR,
                        AiCapabilityDescriptor.ExecutionMode.BATCH,
                        AiCapabilityDescriptor.ExecutionMode.SYNC,
                        AiCapabilityDescriptor.ExecutionMode.ASYNC),
                modalities,
                maxBatchSize,
                true);
    }

    private static CapabilitySpec reranking(String modelName, int maxBatchSize) {
        return new CapabilitySpec(
                "reranking:" + modelName,
                AiCapabilityDescriptor.Operation.RERANKING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "DOUBLE", List.of("VARCHAR", "VARCHAR"))),
                Set.of(
                        AiCapabilityDescriptor.ExecutionMode.SCALAR,
                        AiCapabilityDescriptor.ExecutionMode.BATCH,
                        AiCapabilityDescriptor.ExecutionMode.SYNC),
                Set.of(Modality.TEXT),
                maxBatchSize,
                false);
    }

    private static AiCapabilityDescriptor capability(
            String version,
            AiCapabilityDescriptor.ResourceLimits resourceLimits,
            CapabilitySpec spec) {
        return new AiCapabilityDescriptor(
                spec.id(),
                version,
                spec.operation(),
                spec.signatures(),
                spec.modes(),
                spec.modalities(),
                spec.maxBatchSize(),
                resourceLimits,
                spec.deterministic(),
                AiCapabilityDescriptor.RetryPolicy.noRetry(),
                Set.of("AI_MODEL_USAGE"));
    }

    private static IllegalArgumentException configurationFailure(String message) {
        return new IllegalArgumentException(message);
    }

    private record CapabilitySpec(
            String id,
            AiCapabilityDescriptor.Operation operation,
            List<AiCapabilityDescriptor.Signature> signatures,
            Set<AiCapabilityDescriptor.ExecutionMode> modes,
            Set<Modality> modalities,
            int maxBatchSize,
            boolean deterministic) {}
}
