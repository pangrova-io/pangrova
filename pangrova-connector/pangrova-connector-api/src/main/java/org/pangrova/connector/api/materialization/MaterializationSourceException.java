package org.pangrova.connector.api.materialization;

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

    /** Stable failure classification understood by the kernel. */
    public enum Reason {

        /** The source table does not exist or is not readable by this provider. */
        SOURCE_UNAVAILABLE,

        /** The connector cannot retain the observed snapshot. */
        PIN_UNSUPPORTED,

        /** The pinned snapshot has been expired or removed. */
        SNAPSHOT_UNAVAILABLE,

        /** The two snapshots were not observed against the same schema and policy shape. */
        SHAPE_MISMATCH,

        /** The connector cannot serve the requested change-set mode. */
        CHANGESET_UNSUPPORTED,

        /** The connector cannot prove that the change set is fully evaluated. */
        PROOF_INCOMPLETE
    }

    private final Reason reason;

    /** Creates a source failure with a stable reason and diagnostic message. */
    public MaterializationSourceException(Reason reason, String message) {
        super(message);
        if (reason == null) {
            throw new IllegalArgumentException("reason must not be null");
        }
        this.reason = reason;
    }

    /** Creates a source failure retaining the underlying connector cause. */
    public MaterializationSourceException(Reason reason, String message, Throwable cause) {
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
    public Reason reason() {
        return reason;
    }
}
