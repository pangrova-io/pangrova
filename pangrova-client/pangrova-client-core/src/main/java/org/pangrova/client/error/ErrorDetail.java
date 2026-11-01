package org.pangrova.client.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Error detail for REST API error responses.
 *
 * <p>errorCode is String to align with proto ErrorInfo.error_code (string),
 * supporting richer error codes like "QUERY_TIMEOUT", "MEMORY_EXCEEDED",
 * "WORKER_LOST" rather than numeric HTTP status codes.</p>
 *
 * <p>Null fields are omitted from JSON serialization to maintain backward
 * compatibility with existing clients.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorDetail(
    @JsonProperty("error_code") String errorCode,
    @JsonProperty("error_type") String errorType,
    @JsonProperty("message") String message,
    @JsonProperty("stack_trace") String stackTrace
) {
    /**
     * Convenience constructor without stack trace.
     */
    public ErrorDetail(String errorCode, String errorType, String message) {
        this(errorCode, errorType, message, null);
    }
}
