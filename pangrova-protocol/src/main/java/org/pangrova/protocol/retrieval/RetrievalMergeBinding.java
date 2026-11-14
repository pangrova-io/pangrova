package org.pangrova.protocol.retrieval;

import org.pangrova.execution.api.enums.RetrievalMergeStrategy;


import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.common.api.annotation.Internal;

/** Wire-neutral input used to bind a Native retrieval merge at a singleton stage. */
@Internal
public record RetrievalMergeBinding(
        RetrievalMergeStrategy strategy,
        int scoreFieldIndex,
        int sourceIdFieldIndex,
        List<Integer> stableKeyFieldIndexes,
        int outputLimit,
        int maxFanIn,
        long rrfK,
        List<Source> sources,
        byte[] candidateSchema) {

    public static final int MAX_CANDIDATE_ROWS = 1_000_000;
    public static final int MAX_FAN_IN = 64;

    public RetrievalMergeBinding {
        strategy = Objects.requireNonNull(strategy, "strategy");
        validateCanonicalFields(scoreFieldIndex, sourceIdFieldIndex);
        stableKeyFieldIndexes = List.copyOf(stableKeyFieldIndexes);
        validateStableKeys(scoreFieldIndex, sourceIdFieldIndex, stableKeyFieldIndexes);
        validateLimits(outputLimit, maxFanIn);
        sources = List.copyOf(sources);
        validateSources(sources, maxFanIn);
        validateCandidateBudget(sources);
        validateStrategy(strategy, rrfK);
        candidateSchema = candidateSchema.clone();
        validateCandidateSchema(candidateSchema);
    }

    private static void validateCanonicalFields(int scoreFieldIndex, int sourceIdFieldIndex) {
        if (scoreFieldIndex < 0 || sourceIdFieldIndex < 0
                || scoreFieldIndex == sourceIdFieldIndex) {
            throw new IllegalArgumentException("canonical merge field indexes are invalid");
        }
    }

    private static void validateStableKeys(
            int scoreFieldIndex,
            int sourceIdFieldIndex,
            List<Integer> stableKeyFieldIndexes) {
        if (stableKeyFieldIndexes.isEmpty()
                || new HashSet<>(stableKeyFieldIndexes).size()
                        != stableKeyFieldIndexes.size()
                || stableKeyFieldIndexes.stream().anyMatch(index -> index < 0
                        || index == scoreFieldIndex
                        || index == sourceIdFieldIndex)) {
            throw new IllegalArgumentException("stable-key field indexes are invalid");
        }
    }

    private static void validateLimits(int outputLimit, int maxFanIn) {
        if (outputLimit <= 0 || outputLimit > MAX_CANDIDATE_ROWS
                || maxFanIn <= 0 || maxFanIn > MAX_FAN_IN) {
            throw new IllegalArgumentException("retrieval merge limits are invalid");
        }
    }

    private static void validateSources(List<Source> sources, int maxFanIn) {
        if (sources.isEmpty() || sources.size() > maxFanIn
                || sources.stream().map(Source::sourceId).distinct().count()
                        != sources.size()) {
            throw new IllegalArgumentException("retrieval merge sources are invalid");
        }
    }

    private static void validateCandidateBudget(List<Source> sources) {
        long totalCandidates = sources.stream().mapToLong(Source::candidateLimit).sum();
        if (totalCandidates > MAX_CANDIDATE_ROWS) {
            throw new IllegalArgumentException(
                    "retrieval merge candidate budget exceeds the bounded operator limit");
        }
    }

    private static void validateStrategy(RetrievalMergeStrategy strategy, long rrfK) {
        if (strategy == RetrievalMergeStrategy.RRF && rrfK <= 0) {
            throw new IllegalArgumentException("RRF k must be positive");
        }
    }

    private static void validateCandidateSchema(byte[] candidateSchema) {
        if (candidateSchema.length == 0) {
            throw new IllegalArgumentException("candidate schema is required");
        }
    }

    @Override
    public byte[] candidateSchema() {
        return candidateSchema.clone();
    }

    public record Source(
            String sourceId,
            ScoreDirection scoreDirection,
            double weight,
            int candidateLimit) {
        public Source {
            validateSourceIdentity(sourceId, scoreDirection);
            validateWeight(weight);
            validateCandidateLimit(candidateLimit);
        }

        private static void validateSourceIdentity(
                String sourceId, ScoreDirection scoreDirection) {
            if (sourceId == null || sourceId.isBlank() || scoreDirection == null
                    || scoreDirection == ScoreDirection.SCORE_DIRECTION_UNSPECIFIED
                    || scoreDirection == ScoreDirection.UNRECOGNIZED) {
                throw new IllegalArgumentException("retrieval merge source is invalid");
            }
        }

        private static void validateWeight(double weight) {
            if (!Double.isFinite(weight) || weight <= 0) {
                throw new IllegalArgumentException("retrieval merge source weight is invalid");
            }
        }

        private static void validateCandidateLimit(int candidateLimit) {
            if (candidateLimit <= 0 || candidateLimit > MAX_CANDIDATE_ROWS) {
                throw new IllegalArgumentException(
                        "retrieval merge source candidate limit is invalid");
            }
        }
    }
}
