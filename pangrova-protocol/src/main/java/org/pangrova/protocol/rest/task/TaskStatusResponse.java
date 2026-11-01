package org.pangrova.protocol.rest.task;

import org.pangrova.client.error.ErrorDetail;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for querying the status of a submitted task.
 */
public record TaskStatusResponse(
    @JsonProperty("task_id") String taskId,
    @JsonProperty("state") String state,
    @JsonProperty("stats") TaskStats stats,
    @JsonProperty("error") ErrorDetail error
) {}
