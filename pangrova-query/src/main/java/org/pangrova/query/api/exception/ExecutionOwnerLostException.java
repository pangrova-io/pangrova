package org.pangrova.query.api.exception;

import org.pangrova.query.api.enums.ExecutionOwnerLossReason;
import org.pangrova.common.api.annotation.Internal;

import java.util.Objects;

/** A stale execution owner, distinct from database failure, corrupt data and invalid input. */
@Internal
public final class ExecutionOwnerLostException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private final ExecutionOwnerLossReason reason;

    public ExecutionOwnerLostException(ExecutionOwnerLossReason reason) {
        super("Execution publication owner is absent, replaced or expired");
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ExecutionOwnerLossReason reason() { return reason; }
}
