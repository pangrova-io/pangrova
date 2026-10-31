package org.pangrova.observability.api;

/**
 * Enumerates the kind of {@link Span}, indicating the role it plays in a trace.
 *
 * <p>This is Pangrova's own lightweight abstraction, independent of any
 * specific tracing SDK such as OpenTelemetry.
 */
public enum SpanKind {

    /** Default span kind for internal operations. */
    INTERNAL,

    /** Indicates the span covers server-side handling of a request. */
    SERVER,

    /** Indicates the span covers the client-side of an outgoing request. */
    CLIENT,

    /** Indicates the span describes a message producer. */
    PRODUCER,

    /** Indicates the span describes a message consumer. */
    CONSUMER
}
