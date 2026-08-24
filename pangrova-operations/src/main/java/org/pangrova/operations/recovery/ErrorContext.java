package org.pangrova.operations.recovery;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Context for an error event that triggers SOP recovery.
 * Optionally carries key-value attributes for action-specific context
 * (e.g. workerId, queryId).
 */
public record ErrorContext(String errorCode, String message, Instant timestamp,
                           Map<String, String> attributes) {

    /**
     * Primary constructor with attributes.
     */
    public ErrorContext {
        Objects.requireNonNull(errorCode, "errorCode");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(timestamp, "timestamp");
        attributes = attributes != null ? Map.copyOf(attributes) : Map.of();
    }

    /**
     * Convenience constructor without attributes.
     */
    public ErrorContext(String errorCode, String message, Instant timestamp) {
        this(errorCode, message, timestamp, Map.of());
    }

    /**
     * Returns the attribute value for the given key, or null if not present.
     */
    public String attribute(String key) {
        return attributes.get(key);
    }
}
