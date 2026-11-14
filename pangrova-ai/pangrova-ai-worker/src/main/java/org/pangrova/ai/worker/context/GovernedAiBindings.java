package org.pangrova.ai.worker.context;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.function.GovernedBindingProjection;
import org.pangrova.protocol.function.GovernedScalarValue;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.capability.AiRequestConstraints;

import java.util.Map;

/**
 * The only canonical authority -> provider binding projection, shared with logical budget
 * admission.
 */
public final class GovernedAiBindings {
    private GovernedAiBindings() {}

    /** Decodes the exact model operation contract already admitted with the Native task. */
    public static BoundAiInvocation fromAdmitted(OperatorBinding binding) {
        if (binding == null) throw mismatch();
        AiOperation operation = operation(binding.getCapability());
        String layout = operation == AiOperation.RERANKING ? "AI_RERANK" : "AI_MODEL";
        var values = GovernedBindingProjection.values(layout, binding);
        return new BoundAiInvocation(
                text(values, "invocation_id"),
                text(values, "model_id"),
                text(values, "model_version"),
                signature(values, "model_signature"),
                operation,
                text(values, "idempotency_base"),
                new AiRequestConstraints(
                        unsigned(values, "max_input_tokens"),
                        unsigned(values, "max_output_tokens"),
                        unsigned(values, "max_request_bytes"),
                        unsigned(values, "max_response_bytes")),
                Math.toIntExact(unsigned(values, "provider_concurrency")),
                unsigned(values, "provider_items"),
                unsigned(values, "provider_attempts"),
                unsigned(values, "provider_request_bytes"),
                unsigned(values, "provider_response_bytes"),
                unsigned(values, "media_bytes"),
                operation == AiOperation.RERANKING
                        ? 0
                        : Math.toIntExact(unsigned(values, "output_vector_dimension")));
    }

    private static AiOperation operation(CapabilityRef actual) {
        CapabilityRef expected = switch (actual.getCapabilityId()) {
            case BuiltInCapabilityIdentifiers.AI_TEXT_GENERATION_ID ->
                            BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
            case BuiltInCapabilityIdentifiers.AI_MULTIMODAL_GENERATION_ID ->
                            BuiltInCapabilityCatalog.aiMultimodalGenerationPlanRef();
            case BuiltInCapabilityIdentifiers.AI_EMBEDDING_ID ->
                            BuiltInCapabilityCatalog.aiEmbeddingPlanRef();
            case BuiltInCapabilityIdentifiers.CROSS_ENCODER_RERANK_ID ->
                            BuiltInCapabilityCatalog.crossEncoderRerankPlanRef();
            default -> throw mismatch();
        };
        if (!expected.getSemanticVersion().equals(actual.getSemanticVersion())
                || !expected.getSignatureSha256().equals(actual.getSignatureSha256())
                || !actual.getUnknownFields().asMap().isEmpty()) throw mismatch();
        return switch (actual.getCapabilityId()) {
            case BuiltInCapabilityIdentifiers.AI_TEXT_GENERATION_ID -> AiOperation.TEXT_GENERATION;
            case BuiltInCapabilityIdentifiers.AI_MULTIMODAL_GENERATION_ID ->
                    AiOperation.MULTIMODAL_GENERATION;
            case BuiltInCapabilityIdentifiers.AI_EMBEDDING_ID -> AiOperation.EMBEDDING;
            default -> AiOperation.RERANKING;
        };
    }

    private static AiInvocationException mismatch() {
        return new AiInvocationException(
                AiFailureCategory.CAPABILITY_MISMATCH,
                false,
                "Admitted binding does not identify an exact governed provider capability");
    }

    private static String text(Map<String, GovernedScalarValue> values, String name) {
        return ((GovernedScalarValue.Text) values.get(name)).value();
    }

    private static byte[] signature(Map<String, GovernedScalarValue> values, String name) {
        return ((GovernedScalarValue.Signature) values.get(name)).value().toByteArray();
    }

    private static long unsigned(Map<String, GovernedScalarValue> values, String name) {
        return ((GovernedScalarValue.Unsigned) values.get(name)).value();
    }
}
