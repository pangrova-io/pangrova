package org.pangrova.shuffle.api;

import org.pangrova.common.api.annotation.Experimental;

/** Exact registered allocation requested for publication; an older attempt cannot finalize its successor. */
@Experimental(reason = "Exchange publication across providers")
public record ShufflePublication(String queryId, int stageId, long generation, String allocationId) {
    public ShufflePublication {
        if (queryId == null || queryId.isBlank() || stageId < 0 || generation <= 0
                || allocationId == null || allocationId.isBlank()) {
            throw new IllegalArgumentException("Publication requires a query, stage, generation and allocation");
        }
    }
}
