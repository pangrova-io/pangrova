package org.pangrova.protocol.rest.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Coordinator acknowledgement for Worker registration and heartbeat control traffic.
 *
 * <p>The epoch is issued only by the Coordinator control plane. Workers use it to
 * arm and monotonically advance their task-admission fence; task submissions are
 * deliberately not allowed to advance that fence themselves.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkerControlResponse(
        @JsonProperty("status") String status,
        @JsonProperty("action") String action,
        @JsonProperty("message") String message,
        @JsonProperty("coordinator_epoch") Long coordinatorEpoch) {

    /** Returns the required positive Coordinator epoch or fails the control handshake. */
    public long requireCoordinatorEpoch() {
        if (coordinatorEpoch == null || coordinatorEpoch <= 0) {
            throw new IllegalStateException(
                    "Coordinator control response is missing a positive coordinator_epoch");
        }
        return coordinatorEpoch;
    }

    /** Requires the response status expected by the specific control operation. */
    public WorkerControlResponse requireStatus(String expectedStatus) {
        if (!expectedStatus.equals(status)) {
            throw new IllegalStateException(
                    "Coordinator control response status is '" + status
                            + "'; expected '" + expectedStatus + "'");
        }
        return this;
    }
}
