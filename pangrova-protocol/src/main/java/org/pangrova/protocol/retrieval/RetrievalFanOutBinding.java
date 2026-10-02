package org.pangrova.protocol.retrieval;

import java.util.Arrays;
import java.util.List;

import org.pangrova.spi.annotation.Internal;

/** Canonical Native candidate fan-out from one SQL Top-K into independent sources. */
@Internal
public record RetrievalFanOutBinding(List<Source> sources) {

    public static final int MAX_SOURCES = RetrievalMergeBinding.MAX_FAN_IN;

    public RetrievalFanOutBinding {
        sources = List.copyOf(sources);
        if (sources.size() < 2 || sources.size() > MAX_SOURCES
                || sources.stream().map(Source::sourceId).distinct().count()
                        != sources.size()) {
            throw new IllegalArgumentException("retrieval fan-out sources are invalid");
        }
        RetrievalTopKBinding first = sources.get(0).binding();
        for (Source source : sources) {
            RetrievalTopKBinding binding = source.binding();
            if (binding.scoreFieldIndex() != first.scoreFieldIndex()
                    || !binding.stableKeyFieldIndexes().equals(
                            first.stableKeyFieldIndexes())
                    || !Arrays.equals(binding.candidateSchema(),
                            first.candidateSchema())) {
                throw new IllegalArgumentException(
                        "retrieval fan-out sources require one canonical row contract");
            }
        }
    }

    public record Source(String sourceId, RetrievalTopKBinding binding) {
        public Source {
            if (sourceId == null || sourceId.isBlank()) {
                throw new IllegalArgumentException("fan-out source ID is required");
            }
            if (binding == null) {
                throw new IllegalArgumentException("fan-out source binding is required");
            }
        }
    }
}
