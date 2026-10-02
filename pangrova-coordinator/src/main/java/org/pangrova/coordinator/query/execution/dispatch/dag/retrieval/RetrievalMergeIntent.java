package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import java.util.List;

import org.pangrova.protocol.v2.ScoreDirection;

/** Query-scoped metadata for a Native global retrieval merge stage. */
public record RetrievalMergeIntent(
        Strategy strategy,
        int outputLimit,
        long rrfK,
        List<Source> sources) {

    public RetrievalMergeIntent {
        if (strategy == null || outputLimit <= 0 || sources == null
                || sources.size() < 2
                || sources.size() > org.pangrova.protocol.retrieval.RetrievalMergeBinding.MAX_FAN_IN
                || sources.stream().map(Source::sourceId).distinct().count()
                        != sources.size()
                || (strategy == Strategy.RRF && rrfK <= 0)) {
            throw new IllegalArgumentException("retrieval merge intent is invalid");
        }
        sources = List.copyOf(sources);
    }

    public enum Strategy {
        RRF,
        LINEAR
    }

    /** Typed retrieval semantics; {@code sourceId} remains an opaque identity. */
    public enum SourceKind {
        VECTOR,
        FULL_TEXT
    }

    public record Source(
            String sourceId,
            SourceKind kind,
            ScoreDirection scoreDirection,
            double weight,
            int candidateLimit) {
        public Source {
            if (sourceId == null || sourceId.isBlank()
                    || kind == null
                    || scoreDirection == null
                    || scoreDirection == ScoreDirection.SCORE_DIRECTION_UNSPECIFIED
                    || scoreDirection == ScoreDirection.UNRECOGNIZED
                    || !Double.isFinite(weight) || weight <= 0.0
                    || candidateLimit <= 0
                    || candidateLimit
                            > org.pangrova.protocol.retrieval.RetrievalMergeBinding
                                    .MAX_CANDIDATE_ROWS) {
                throw new IllegalArgumentException(
                        "retrieval merge source intent is invalid");
            }
        }
    }
}
