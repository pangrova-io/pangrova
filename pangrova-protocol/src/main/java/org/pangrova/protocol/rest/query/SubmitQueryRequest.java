package org.pangrova.protocol.rest.query;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Request body for submitting a new query via REST API.
 */
public record SubmitQueryRequest(
    @JsonProperty("query") String query,
    @JsonProperty("session_properties") Map<String, String> sessionProperties
) {

    public SubmitQueryRequest {
        sessionProperties = sessionProperties == null ? Map.of() : Map.copyOf(sessionProperties);
    }
}
