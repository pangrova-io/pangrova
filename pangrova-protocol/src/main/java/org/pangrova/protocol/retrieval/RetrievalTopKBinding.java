package org.pangrova.protocol.retrieval;

import org.pangrova.protocol.BuiltInCapabilityCatalog;

import java.util.List;

import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.spi.annotation.Internal;

/** Wire-binding input for one planner-recognized local retrieval/Top-K relation. */
@Internal
public record RetrievalTopKBinding(
        int scoreFieldIndex,
        int candidateLimit,
        ScoreDirection scoreDirection,
        List<Integer> stableKeyFieldIndexes,
        byte[] candidateSchema,
        String sourceId,
        CapabilityRef capability,
        List<NamedValue> invocationOptions,
        List<ArtifactBinding> artifacts,
        String approximationProfileId,
        int exactRescoreLimit,
        double requiredRecallAtK,
        int recallK) {

    /**
     * Approximation-specific state for an artifact-backed retrieval binding.
     *
     * @param sourceId retrieval source identifier
     * @param capability exact retrieval capability
     * @param invocationOptions typed invocation options
     * @param artifact bound artifact generation
     * @param approximationProfileId approximation profile identifier
     * @param exactRescoreLimit exact re-score bound
     * @param requiredRecallAtK required recall threshold
     * @param recallK recall measurement K
     */
    public record ArtifactInput(
            String sourceId,
            CapabilityRef capability,
            List<NamedValue> invocationOptions,
            ArtifactBinding artifact,
            String approximationProfileId,
            int exactRescoreLimit,
            double requiredRecallAtK,
            int recallK) {}

    public RetrievalTopKBinding {
        validateCore(scoreFieldIndex, candidateLimit, scoreDirection);
        if (recallK <= 0 || recallK > candidateLimit) {
            throw new IllegalArgumentException("retrieval output K differs from its recall/work contract");
        }
        stableKeyFieldIndexes = List.copyOf(stableKeyFieldIndexes);
        validateStableKeys(stableKeyFieldIndexes);
        candidateSchema = candidateSchema.clone();
        validateCandidateSchema(candidateSchema);
        validateCapability(capability);
        invocationOptions = List.copyOf(invocationOptions);
        artifacts = List.copyOf(artifacts);
        validateOptions(invocationOptions);
        boolean exact = capability.getCapabilityId().equals(
                BuiltInCapabilityCatalog.EXACT_TOP_K_ID);
        ValidationState validation = new ValidationState(
                sourceId, artifacts, invocationOptions, approximationProfileId,
                exactRescoreLimit, requiredRecallAtK, recallK, candidateLimit);
        if (exact) {
            validateExact(validation);
            sourceId = "";
        } else {
            validateApproximate(validation);
        }
    }

    private static void validateCore(
            int scoreFieldIndex, int candidateLimit, ScoreDirection scoreDirection) {
        if (scoreFieldIndex < 0 || candidateLimit <= 0
                || scoreDirection == ScoreDirection.SCORE_DIRECTION_UNSPECIFIED) {
            throw new IllegalArgumentException("invalid retrieval Top-K binding");
        }
    }

    private static void validateStableKeys(List<Integer> stableKeyFieldIndexes) {
        if (stableKeyFieldIndexes.isEmpty()) {
            throw new IllegalArgumentException("at least one stable key is required");
        }
    }

    private static void validateCandidateSchema(byte[] candidateSchema) {
        if (candidateSchema.length == 0) {
            throw new IllegalArgumentException("candidate schema is required");
        }
    }

    private static void validateCapability(CapabilityRef capability) {
        if (capability == null || capability.getCapabilityId().isBlank()
                || capability.getSemanticVersion().isBlank()
                || capability.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException("retrieval capability identity is incomplete");
        }
    }

    private static void validateOptions(List<NamedValue> invocationOptions) {
        java.util.HashSet<String> optionNames = new java.util.HashSet<>();
        if (invocationOptions.stream().anyMatch(option -> option.getName().isBlank()
                || !option.hasValue() || !optionNames.add(option.getName()))) {
            throw new IllegalArgumentException("retrieval options are invalid or duplicated");
        }
    }

    private static void validateExact(ValidationState state) {
        if (!hasNoApproximateInputs(state) || !hasExactRecall(state)) {
            throw new IllegalArgumentException("exact Top-K binding carries approximate state");
        }
    }

    private static boolean hasNoApproximateInputs(ValidationState state) {
        return state.artifacts().isEmpty() && state.invocationOptions().isEmpty()
                && (state.sourceId() == null || state.sourceId().isBlank());
    }

    private static boolean hasExactRecall(ValidationState state) {
        return "exact".equals(state.approximationProfileId())
                && state.exactRescoreLimit() == 0 && state.requiredRecallAtK() == 1.0
                && state.recallK() == state.candidateLimit();
    }

    private static void validateApproximate(ValidationState state) {
        validateApproximateIdentity(state);
        validateRescoreLimit(state);
        validateRecallThreshold(state);
        validateRecallK(state);
    }

    private static void validateApproximateIdentity(ValidationState state) {
        if (state.sourceId() == null || state.sourceId().isBlank()
                || state.artifacts().isEmpty()
                || state.approximationProfileId() == null
                || state.approximationProfileId().isBlank()) {
            throw new IllegalArgumentException("approximate retrieval binding is incomplete");
        }
    }

    private static void validateRescoreLimit(ValidationState state) {
        if (state.exactRescoreLimit() <= 0
                || state.exactRescoreLimit() > state.candidateLimit()) {
            throw new IllegalArgumentException("approximate exact-rescore limit is invalid");
        }
    }

    private static void validateRecallThreshold(ValidationState state) {
        if (!Double.isFinite(state.requiredRecallAtK())
                || state.requiredRecallAtK() <= 0.0 || state.requiredRecallAtK() > 1.0) {
            throw new IllegalArgumentException("approximate recall threshold is invalid");
        }
    }

    private static void validateRecallK(ValidationState state) {
        if (state.recallK() <= 0 || state.recallK() > state.candidateLimit()) {
            throw new IllegalArgumentException("approximate recall K is invalid");
        }
    }

    private record ValidationState(
            String sourceId,
            List<ArtifactBinding> artifacts,
            List<NamedValue> invocationOptions,
            String approximationProfileId,
            int exactRescoreLimit,
            double requiredRecallAtK,
            int recallK,
            int candidateLimit) {}

    /**
     * Creates a canonical exact Top-K binding.
     *
     * @param scoreFieldIndex score column index
     * @param candidateLimit maximum candidate rows
     * @param scoreDirection score ordering
     * @param stableKeyFieldIndexes deterministic tie-break keys
     * @param candidateSchema serialized candidate schema
     * @return exact binding
     */
    public static RetrievalTopKBinding exact(
            int scoreFieldIndex,
            int candidateLimit,
            ScoreDirection scoreDirection,
            List<Integer> stableKeyFieldIndexes,
            byte[] candidateSchema) {
        return new RetrievalTopKBinding(
                scoreFieldIndex, candidateLimit, scoreDirection,
                stableKeyFieldIndexes, candidateSchema, "",
                BuiltInCapabilityCatalog.exactTopKPlanRef(), List.of(), List.of(),
                "exact", 0, 1.0, candidateLimit);
    }

    /**
     * Creates an artifact-backed approximate retrieval binding.
     *
     * @param scoreFieldIndex score column index
     * @param candidateLimit maximum candidate rows
     * @param scoreDirection score ordering
     * @param stableKeyFieldIndexes deterministic tie-break keys
     * @param candidateSchema serialized candidate schema
     * @param input approximation-specific state
     * @return artifact-backed binding
     */
    public static RetrievalTopKBinding artifact(
            int scoreFieldIndex,
            int candidateLimit,
            ScoreDirection scoreDirection,
            List<Integer> stableKeyFieldIndexes,
            byte[] candidateSchema,
            ArtifactInput input) {
        return artifact(scoreFieldIndex, candidateLimit, candidateLimit, scoreDirection,
                stableKeyFieldIndexes, candidateSchema, input);
    }

    /** Separates visible K from the admitted exploration/rescore bound. */
    public static RetrievalTopKBinding artifact(
            int scoreFieldIndex, int outputLimit, int candidateLimit,
            ScoreDirection scoreDirection, List<Integer> stableKeyFieldIndexes,
            byte[] candidateSchema, ArtifactInput input) {
        if (outputLimit != input.recallK()) {
            throw new IllegalArgumentException("retrieval visible K differs from its recall measurement K");
        }
        return new RetrievalTopKBinding(
                scoreFieldIndex, candidateLimit, scoreDirection,
                stableKeyFieldIndexes, candidateSchema, input.sourceId(), input.capability(),
                input.invocationOptions(), List.of(input.artifact()),
                input.approximationProfileId(), input.exactRescoreLimit(),
                input.requiredRecallAtK(), input.recallK());
    }

    /** SQL K is the same authoritative measurement K; exploration has its own candidate bound. */
    public int outputLimit() {
        return recallK;
    }

    /** Returns whether this binding uses the built-in exact Top-K capability. */
    public boolean exact() {
        return capability.getCapabilityId().equals(
                BuiltInCapabilityCatalog.EXACT_TOP_K_ID);
    }

    @Override
    public byte[] candidateSchema() {
        return candidateSchema.clone();
    }
}
