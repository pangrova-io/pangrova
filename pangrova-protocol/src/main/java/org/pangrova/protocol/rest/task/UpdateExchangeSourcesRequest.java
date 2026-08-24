package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Request to update exchange sources for a running task (Coordinator to Worker).
 *
 * <p>Sent from Coordinator to Worker when upstream stage tasks produce
 * shuffle output that the downstream task needs to consume.</p>
 */
public record UpdateExchangeSourcesRequest(
    @JsonProperty("task_id") String taskId,
    @JsonProperty("query_id") String queryId,
    @JsonProperty("exchange_sources") List<ExchangeSourceDescriptor> exchangeSources,
    @JsonProperty("reason") String reason
) {

    public UpdateExchangeSourcesRequest {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        exchangeSources = exchangeSources == null ? List.of() : List.copyOf(exchangeSources);
    }
}
