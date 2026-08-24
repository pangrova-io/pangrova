package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Request body for a Worker to pull the next split from the Coordinator.
 * Used in pull-mode split assignment where Workers request splits on demand.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SplitRequest(
        @JsonProperty("query_id") String queryId,
        @JsonProperty("stage_id") int stageId,
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("worker_locality") List<String> workerLocality
) {

    public SplitRequest {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be null or blank");
        }
        workerLocality = workerLocality == null ? List.of() : List.copyOf(workerLocality);
    }
}
