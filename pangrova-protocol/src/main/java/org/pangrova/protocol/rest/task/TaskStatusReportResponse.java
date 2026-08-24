package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Coordinator response to a Worker task status report.
 */
public record TaskStatusReportResponse(
    @JsonProperty("task_id") String taskId,
    @JsonProperty("command") String command
) {}
