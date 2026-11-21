package org.pangrova.planner.api.retrieval;

import org.pangrova.planner.api.enums.RetrievalKind;
import org.pangrova.planner.api.enums.RetrievalScoreDirection;

import java.util.List;

import org.pangrova.common.api.annotation.Internal;

/** Planner-owned, wire-neutral intent for replacing a Sort/Fetch pair with typed Top-K retrieval. */
@Internal
public record RetrievalTopKPlanHint(
        RetrievalKind kind,
        String sourceKey,
        int scoreFieldIndex,
        int candidateLimit,
        RetrievalScoreDirection scoreDirection,
        List<Integer> stableKeyFieldIndexes,
        byte[] candidateSchema) {


    public RetrievalTopKPlanHint {
        kind = java.util.Objects.requireNonNull(kind, "kind");
        sourceKey = sourceKey == null ? "" : sourceKey;
        if ((kind == RetrievalKind.EXACT) != sourceKey.isBlank()) {
            throw new IllegalArgumentException(
                    "only artifact-backed retrieval hints require a source key");
        }
        if (scoreFieldIndex < 0 || candidateLimit <= 0) {
            throw new IllegalArgumentException("score field and candidate limit must be positive");
        }
        scoreDirection = java.util.Objects.requireNonNull(scoreDirection, "scoreDirection");
        stableKeyFieldIndexes = List.copyOf(stableKeyFieldIndexes);
        if (stableKeyFieldIndexes.isEmpty()) {
            throw new IllegalArgumentException("at least one stable key is required");
        }
        candidateSchema = candidateSchema.clone();
    }

    @Override
    public byte[] candidateSchema() {
        return candidateSchema.clone();
    }
}
