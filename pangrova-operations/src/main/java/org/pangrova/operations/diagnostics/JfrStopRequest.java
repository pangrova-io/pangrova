package org.pangrova.operations.diagnostics;

/**
 * Request parameters for stopping a JFR recording.
 */
public record JfrStopRequest(long recordingId) {

    public JfrStopRequest {
        if (recordingId <= 0) {
            throw new IllegalArgumentException("recordingId must be positive");
        }
    }
}
