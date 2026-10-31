package org.pangrova.protocol.transport;

/**
 * Transport-level status codes mirroring gRPC status semantics.
 */
public enum TransportStatus {
    OK,
    NOT_FOUND,
    INVALID_ARGUMENT,
    RESOURCE_EXHAUSTED,
    INTERNAL,
    UNAVAILABLE,
    CANCELLED
}
