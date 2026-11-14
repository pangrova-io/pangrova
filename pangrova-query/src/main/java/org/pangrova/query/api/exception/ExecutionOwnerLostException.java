package org.pangrova.query.api.exception;

import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;

/** A stale execution owner, distinct from database failure, corrupt data and invalid input. */
@Internal
public final class ExecutionOwnerLostException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    /** The backend fact that rejected the supplied execution owner. */
    public enum Reason { ABSENT, REPLACED, EXPIRED }

    private final Reason reason;

    public ExecutionOwnerLostException(Reason reason) {
        super("Execution publication owner is absent, replaced or expired");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public Reason reason() { return reason; }
}
