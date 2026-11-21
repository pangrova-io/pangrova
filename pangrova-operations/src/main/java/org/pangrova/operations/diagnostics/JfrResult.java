package org.pangrova.operations.diagnostics;

import org.pangrova.operations.enums.JfrState;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable result of a JFR start or stop operation.
 */
public record JfrResult(long recordingId, JfrState state, String outputPath,
                        Duration duration, Instant timestamp) {

    public JfrResult {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(outputPath, "outputPath");
        Objects.requireNonNull(duration, "duration");
        Objects.requireNonNull(timestamp, "timestamp");
    }
}
