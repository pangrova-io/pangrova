package org.pangrova.protocol.rest.worker;

import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;

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
        @JsonProperty(WorkerRegistrationRequest.INVENTORY_PROPERTY) String capabilityInventory,
        @JsonProperty("copy_task_tls_port") int copyTaskTlsPort,
        @JsonProperty("worker_execution_identity") WorkerExecutionIdentity workerExecutionIdentity) {

    public WorkerRegistrationRequest(String workerId, String httpHost, int httpPort,
                                     int transportPort, int resultServerPort, int protocolVersion,
                                     String rackId, List<String> dataLocality, String shuffleMode,
                                     String capabilityInventory, WorkerExecutionIdentity workerExecutionIdentity) {
        this(workerId, httpHost, httpPort, transportPort, resultServerPort, protocolVersion,
                rackId, dataLocality, shuffleMode, capabilityInventory, 0, workerExecutionIdentity);
    }

    public static final String INVENTORY_PROPERTY = "capability_inventory";
    private static final int MAX_TCP_PORT = 65535;
    public static final int MAX_METADATA_JSON_BYTES = 1024 * 1024;
    /** Maximum serialized body: bounded metadata plus Base64 inventory and its JSON field. */
    public static final int MAX_REGISTRATION_BODY_BYTES = MAX_METADATA_JSON_BYTES
            + CapabilityInventoryCodec.MAX_ENCODED_BYTES
            + (",\"" + INVENTORY_PROPERTY + "\":\"\"").length();

    /** Canonical inventory plus bounded JSON framing, addresses and locality metadata. */
    public static final int MAX_BODY_BYTES = CapabilityInventoryCodec.MAX_ENCODED_BYTES + 64 * 1024;

    public WorkerRegistrationRequest {
        java.util.Objects.requireNonNull(workerExecutionIdentity, "workerExecutionIdentity");
        requireNonBlank(workerId, "workerId");
        requireNonBlank(httpHost, "httpHost");
        requireTcpPort(httpPort, "httpPort");
        requireTcpPort(transportPort, "transportPort");
        requireTcpPort(resultServerPort, "resultServerPort");
        requireTcpPort(copyTaskTlsPort, "copyTaskTlsPort");
        if (protocolVersion != CanonicalProtocolIdentity.PROTOCOL_MAJOR) {
            throw new IllegalArgumentException(
                    "protocolVersion must equal canonical v2");
        }
        CapabilityInventoryCodec.decode(capabilityInventory);
        // rackId is nullable — no validation needed
        dataLocality = copyList(dataLocality);
    }

    /** Checks the actual JSON representation of every registration field except the inventory. */
    public void requireBoundedMetadata(ObjectMapper mapper) {
        ObjectNode metadata = mapper.valueToTree(this);
        metadata.remove(INVENTORY_PROPERTY);
        try {
            if (mapper.writeValueAsBytes(metadata).length > MAX_METADATA_JSON_BYTES) {
                throw new IllegalArgumentException("Worker registration metadata exceeds maximum JSON size");
            }
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Worker registration metadata is not serializable", failure);
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requireTcpPort(int value, String name) {
        if (value < 0 || value > MAX_TCP_PORT) {
            throw new IllegalArgumentException(name + " must be between 0 and 65535");
        }
    }

    private static List<String> copyList(List<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
