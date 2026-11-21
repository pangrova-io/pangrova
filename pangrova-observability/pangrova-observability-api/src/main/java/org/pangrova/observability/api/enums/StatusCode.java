package org.pangrova.observability.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Status codes for a {@link Span}, indicating whether the operation
 * it represents succeeded, failed, or has not been set.
 *
 * <p>This is Pangrova's own lightweight abstraction, independent of any
 * specific tracing SDK such as OpenTelemetry.
 */
@Experimental
public enum StatusCode {

    /** The default status — the operation has not been explicitly marked. */
    UNSET,

    /** The operation completed successfully. */
    OK,

    /** The operation completed with an error. */
    ERROR
}
