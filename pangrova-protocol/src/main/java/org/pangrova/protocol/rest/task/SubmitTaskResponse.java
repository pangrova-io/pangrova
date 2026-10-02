package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.protocol.rest.task.exchange.ShuffleEndpointDescriptor;

/**
 * Response body after submitting a task via REST API.
 */
public record SubmitTaskResponse(
    @JsonProperty("accepted") boolean accepted,
    @JsonProperty("reject_reason") String rejectReason,
    @JsonProperty("task_id") String taskId,
    @JsonProperty("result_endpoint") ResultEndpoint resultEndpoint,
    @JsonProperty("shuffle_endpoint") ShuffleEndpointDescriptor shuffleEndpoint
) {}
