package org.pangrova.common.client.query;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Query execution statistics for REST API responses.
 */
public record QueryStats(
    @JsonProperty("elapsed_time_ms") long elapsedTimeMs,
    @JsonProperty("queued_time_ms") long queuedTimeMs,
    @JsonProperty("rows_returned") long rowsReturned
) {}
