package org.pangrova.connector.api.exception;

import org.pangrova.connector.api.enums.MaterializationSourceFailureReason;
import org.pangrova.common.api.annotation.Stable;

/**
 * Fail-closed failure reported by a {@code SourceProvider}.
 *
 * <p>Connectors never express unsupported or unprovable operations by returning
 * an empty or optimistic result. The kernel translates these failures into
 * {@code INCREMENTAL_UNSUPPORTED} or a failed attempt without falling back to a
 * weaker execution mode.
 */
@Stable(since = "1.0")
public final class MaterializationSourceException extends RuntimeException {


    private final MaterializationSourceFailureReason reason;

    /** Creates a source failure with a stable reason and diagnostic message. */
    public MaterializationSourceException(MaterializationSourceFailureReason reason, String message) {
        super(message);
        if (reason == null) {
            throw new IllegalArgumentException("reason must not be null");
        }
        this.reason = reason;
    }

    /** Creates a source failure retaining the underlying connector cause. */
    public MaterializationSourceException(MaterializationSourceFailureReason reason, String message, Throwable cause) {
        super(message, cause);
        if (reason == null) {
            throw new IllegalArgumentException("reason must not be null");
        }
        this.reason = reason;
    }

    /**
     * Returns the stable failure classification.
     *
     * @return failure reason
     */
    public MaterializationSourceFailureReason reason() {
        return reason;
    }
}
