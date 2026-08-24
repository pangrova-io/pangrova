package org.pangrova.protocol.validation;

import java.util.List;

/**
 * Thrown when a protobuf message fails field validation at deserialization time.
 * Carries the message type name and a list of specific field violations.
 */
public final class ProtoValidationException extends RuntimeException {

    private final String messageType;
    private final List<String> violations;

    public ProtoValidationException(String messageType, List<String> violations) {
        super("Proto validation failed for " + messageType + ": " + violations);
        this.messageType = messageType;
        this.violations = List.copyOf(violations);
    }

    /** The proto message type that failed validation. */
    public String messageType() {
        return messageType;
    }

    /** Specific field violations found during validation. */
    public List<String> violations() {
        return violations;
    }
}
