package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.CapabilityInventoryCodec;

import java.util.List;

/**
 * Registration payload sent by Worker to Coordinator on startup.
 * Coordinator uses this to add the Worker to the active worker pool.
 *
 * <p>{@code protocolVersion} is mandatory. Pangrova has one canonical v2
 * execution protocol; omitted, legacy, and invalid versions fail closed.
 */
public record WorkerRegistrationRequest(
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("http_host") String httpHost,
        @JsonProperty("http_port") int httpPort,
        @JsonProperty("transport_port") int transportPort,
        @JsonProperty("result_server_port") int resultServerPort,
        @JsonProperty("protocol_version") int protocolVersion,
        @JsonProperty("rack_id") String rackId,
        @JsonProperty("data_locality") List<String> dataLocality,
        @JsonProperty("shuffle_mode") String shuffleMode,
        @JsonProperty("capability_inventory") String capabilityInventory) {

    /** Canonical inventory plus bounded JSON framing, addresses and locality metadata. */
    public static final int MAX_BODY_BYTES = CapabilityInventoryCodec.MAX_ENCODED_BYTES + 64 * 1024;

    public WorkerRegistrationRequest {
        requireNonBlank(workerId, "workerId");
        requireNonBlank(httpHost, "httpHost");
        requireNonNegative(httpPort, "httpPort");
        requireNonNegative(transportPort, "transportPort");
        requireNonNegative(resultServerPort, "resultServerPort");
        if (protocolVersion != CanonicalTaskRequests.PROTOCOL_MAJOR) {
            throw new IllegalArgumentException(
                    "protocolVersion must equal canonical v2");
        }
        CapabilityInventoryCodec.decode(capabilityInventory);
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
