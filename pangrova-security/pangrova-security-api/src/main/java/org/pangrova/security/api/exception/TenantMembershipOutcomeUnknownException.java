package org.pangrova.security.api.exception;

import org.pangrova.common.api.annotation.Experimental;

/** Commit confirmation was lost. Inspect current membership before deciding any further operation. */
@Experimental
public final class TenantMembershipOutcomeUnknownException extends RuntimeException {
    public TenantMembershipOutcomeUnknownException(Throwable cause) {
        super("Tenant membership commit outcome is unknown; confirm current membership before retrying", cause);
    }
}
