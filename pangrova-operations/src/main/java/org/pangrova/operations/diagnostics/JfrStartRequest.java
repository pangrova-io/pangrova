package org.pangrova.operations.diagnostics;

import java.time.Duration;

/**
 * Request parameters for starting a JFR recording.
 * Both fields are optional; defaults come from DiagnosticConfig.
 */
public record JfrStartRequest(Duration duration, String settings) {

    /**
     * Creates a request using all defaults from config.
     */
    public static JfrStartRequest withDefaults() {
        return new JfrStartRequest(null, null);
    }
}
