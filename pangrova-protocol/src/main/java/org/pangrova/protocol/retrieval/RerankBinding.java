package org.pangrova.protocol.retrieval;

import java.util.HashSet;
import java.util.List;

import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.resource.ModelResourceEnvelopes;
import org.pangrova.common.api.annotation.Internal;

/** Wire-neutral contract for a bounded, version-exact model rerank stage. */
@Internal
public record RerankBinding(
        int scoreFieldIndex,
        int documentFieldIndex,
        List<Integer> stableKeyFieldIndexes,
        int inputLimit,
        int outputLimit,
        String modelId,
        String modelVersion,
        byte[] modelSignature,
        LogicalType modelOutputType,
        CapabilityLimits modelLimits,
        String query,
        byte[] candidateSchema) {

    public static final int MAX_INPUT_ROWS = 100_000;

    public RerankBinding {
        stableKeyFieldIndexes = List.copyOf(stableKeyFieldIndexes);
        validateFieldIndexes(scoreFieldIndex, documentFieldIndex, stableKeyFieldIndexes);
        validateLimits(inputLimit, outputLimit);
        requireText(modelId, "modelId");
        requireText(modelVersion, "modelVersion");
        modelSignature = modelSignature.clone();
        validateModelSignature(modelSignature);
        validateModelOutputType(modelOutputType);
        ModelResourceEnvelopes.validate(modelLimits);
        ModelResourceEnvelopes.resources(modelLimits, inputLimit, false);
        requireText(query, "query");
        candidateSchema = candidateSchema.clone();
        validateCandidateSchema(candidateSchema);
    }

    private static void validateFieldIndexes(
            int scoreFieldIndex,
            int documentFieldIndex,
            List<Integer> stableKeyFieldIndexes) {
        if (scoreFieldIndex < 0 || documentFieldIndex < 0
                || scoreFieldIndex == documentFieldIndex
                || stableKeyFieldIndexes.isEmpty()
                || new HashSet<>(stableKeyFieldIndexes).size()
                        != stableKeyFieldIndexes.size()
                || stableKeyFieldIndexes.stream().anyMatch(index -> index < 0
                        || index == scoreFieldIndex)) {
            throw new IllegalArgumentException(
                    "rerank field indexes are invalid");
        }
    }

    private static void validateLimits(int inputLimit, int outputLimit) {
        if (inputLimit <= 0 || inputLimit > MAX_INPUT_ROWS
                || outputLimit <= 0 || outputLimit > inputLimit) {
            throw new IllegalArgumentException("rerank limits are invalid");
        }
    }

    private static void validateModelSignature(byte[] modelSignature) {
        if (modelSignature.length != 32) {
            throw new IllegalArgumentException(
                    "model signature must contain exactly 32 bytes");
        }
    }

    private static void validateModelOutputType(LogicalType outputType) {
        if (outputType == null
                || !outputType.getCanonicalSql().equals("DOUBLE")
                || !outputType.hasSubstraitType()
                || !outputType.getSubstraitType().hasFp64()
                || outputType.getSubstraitType().getFp64().getNullability()
                != io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE
                || outputType.getConstraintsCount() != 0) {
            throw new IllegalArgumentException(
                    "reranking model output must be the canonical nullable DOUBLE contract");
        }
    }

    private static void validateCandidateSchema(byte[] candidateSchema) {
        if (candidateSchema.length == 0) {
            throw new IllegalArgumentException("candidate schema is required");
        }
    }

    @Override
    public byte[] modelSignature() {
        return modelSignature.clone();
    }

    @Override
    public byte[] candidateSchema() {
        return candidateSchema.clone();
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
