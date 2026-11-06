package org.pangrova.security.api.grant;

import org.pangrova.common.api.annotation.Experimental;

import java.util.UUID;

/** Commit acknowledgement was lost. Confirm by replaying the same request ID and payload; never invent a new ID. */
@Experimental
public final class PrivilegeMutationOutcomeUnknownException extends RuntimeException {
    private final UUID requestId;

    public PrivilegeMutationOutcomeUnknownException(UUID requestId, Throwable cause) {
        super("Privilege mutation outcome is unknown for request " + requestId, cause);
        this.requestId = requestId;
    }

    public UUID requestId() { return requestId; }
}
