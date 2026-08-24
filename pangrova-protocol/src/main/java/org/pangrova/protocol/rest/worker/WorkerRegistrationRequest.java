package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Registration payload sent by Worker to Coordinator on startup.
 * Coordinator uses this to add the Worker to the active worker pool.
 *
 * <p>{@code protocolVersion} and {@code capabilities} are optional protocol evolution fields.
 * Existing Workers that omit them will get defaults (version=1, empty capabilities)
 * via Jackson deserialization + compact constructor normalization.
 */
public record WorkerRegistrationRequest(
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("http_host") String httpHost,
        @JsonProperty("http_port") int httpPort,
        @JsonProperty("transport_port") int transportPort,
        @JsonProperty("result_server_port") int resultServerPort,
        @JsonProperty("protocol_version") int protocolVersion,
        @JsonProperty("capabilities") List<String> capabilities,
        @JsonProperty("rack_id") String rackId,
        @JsonProperty("data_locality") List<String> dataLocality,
        @JsonProperty("shuffle_mode") String shuffleMode) {

    public WorkerRegistrationRequest {
        requireNonBlank(workerId, "workerId");
        requireNonBlank(httpHost, "httpHost");
        requireNonNegative(httpPort, "httpPort");
        requireNonNegative(transportPort, "transportPort");
        requireNonNegative(resultServerPort, "resultServerPort");
        if (protocolVersion <= 0) {
            protocolVersion = 1;
        }
        capabilities = copyList(capabilities);
        // rackId is nullable — no validation needed
        dataLocality = copyList(dataLocality);
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static List<String> copyList(List<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
