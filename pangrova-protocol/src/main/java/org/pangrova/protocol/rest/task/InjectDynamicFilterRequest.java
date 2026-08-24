package org.pangrova.protocol.rest.task;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request sent from Coordinator to Worker to inject a dynamic filter
 * into a running task's TableScan node.
 *
 * @param planNodeId  the TableScan plan node to receive the filter
 * @param filterProto serialized filter expression (protobuf bytes, Base64-encoded by Jackson)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record InjectDynamicFilterRequest(
        @JsonProperty("plan_node_id") String planNodeId,
        @JsonProperty("filter_proto") byte[] filterProto
) {

    public InjectDynamicFilterRequest {
        Objects.requireNonNull(planNodeId, "planNodeId must not be null");
        Objects.requireNonNull(filterProto, "filterProto must not be null");
        filterProto = filterProto.clone();
    }

    @Override
    public byte[] filterProto() {
        return filterProto.clone();
    }
}
