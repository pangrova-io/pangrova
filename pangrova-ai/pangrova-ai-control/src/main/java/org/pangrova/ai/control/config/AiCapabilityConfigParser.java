package org.pangrova.ai.control.config;

import org.pangrova.ai.api.enums.AiExecutionMode;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.enums.Modality;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Builds canonical AI capabilities from model configuration. */
final class AiCapabilityConfigParser {

    private AiCapabilityConfigParser() {}

    static Set<AiOperation> operations(String raw, String modelName) {
        var operations = EnumSet.noneOf(AiOperation.class);
        for (String token : csv(raw)) {
            operations.add(switch (token.toLowerCase(Locale.ROOT)) {
                case "inference" -> AiOperation.TEXT_GENERATION;
                case "multimodal" -> AiOperation.MULTIMODAL_GENERATION;
                case "embedding" -> AiOperation.EMBEDDING;
                case "reranking" -> AiOperation.RERANKING;
                default -> throw configurationFailure(
                        "Unknown capability '" + token + "' for AI model '" + modelName + "'");
            });
        }
        if (operations.isEmpty()) {
            throw configurationFailure("AI model '" + modelName + "' has no capabilities");
        }
        return Collections.unmodifiableSet(operations);
    }

    static List<AiCapabilityDescriptor> parse(
            Set<AiOperation> operations,
            String modelName,
            String version,
            int maxBatchSize,
            Set<Modality> modalities,
            AiCapabilityDescriptor.ResourceLimits resourceLimits,
            boolean governedBlobLoaderAvailable) {
        List<AiCapabilityDescriptor> result = new ArrayList<>();
        for (AiOperation operation : operations) {
            CapabilitySpec spec = switch (operation) {
                case TEXT_GENERATION -> inference(modelName, maxBatchSize);
                case MULTIMODAL_GENERATION -> {
                    if (!governedBlobLoaderAvailable) {
                        throw configurationFailure(
                                "AI model '" + modelName + "' cannot advertise multimodal: "
                                        + "no governed FILE content loader is installed");
                    }
                    yield multimodal(modelName, modalities);
                }
                case EMBEDDING -> embedding(modelName, maxBatchSize, modalities);
                case RERANKING -> reranking(modelName, maxBatchSize);
            };
            result.add(capability(version, resourceLimits, spec));
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
                AiOperation.TEXT_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("VARCHAR"))),
                Set.of(
                        AiExecutionMode.SCALAR,
                        AiExecutionMode.BATCH,
                        AiExecutionMode.SYNC),
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
                AiOperation.MULTIMODAL_GENERATION,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VARCHAR", List.of("MULTIMODAL_PROMPT"))),
                Set.of(
                        AiExecutionMode.SCALAR,
                        AiExecutionMode.SYNC),
                modalities,
                1,
                false);
    }

    private static CapabilitySpec embedding(
            String modelName, int maxBatchSize, Set<Modality> modalities) {
        return new CapabilitySpec(
                "embedding:" + modelName,
                AiOperation.EMBEDDING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "VECTOR", List.of("VARCHAR"))),
                Set.of(
                        AiExecutionMode.SCALAR,
                        AiExecutionMode.BATCH,
                        AiExecutionMode.SYNC,
                        AiExecutionMode.ASYNC),
                modalities,
                maxBatchSize,
                true);
    }

    private static CapabilitySpec reranking(String modelName, int maxBatchSize) {
        return new CapabilitySpec(
                "reranking:" + modelName,
                AiOperation.RERANKING,
                List.of(new AiCapabilityDescriptor.Signature(
                        "DOUBLE", List.of("VARCHAR", "VARCHAR"))),
                Set.of(
                        AiExecutionMode.SCALAR,
                        AiExecutionMode.BATCH,
                        AiExecutionMode.SYNC),
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
                spec.modalities().stream().anyMatch(modality -> modality != Modality.TEXT)
                        ? Set.of("AI_MODEL_USAGE", "AI_MEDIA_EGRESS")
                        : Set.of("AI_MODEL_USAGE"));
    }

    private static IllegalArgumentException configurationFailure(String message) {
        return new IllegalArgumentException(message);
    }

    private record CapabilitySpec(
            String id,
            AiOperation operation,
            List<AiCapabilityDescriptor.Signature> signatures,
            Set<AiExecutionMode> modes,
            Set<Modality> modalities,
            int maxBatchSize,
            boolean deterministic) {}
}
