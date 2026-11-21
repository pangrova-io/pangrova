package org.pangrova.security.api.grant;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable outcome. Revision is the authorization revision at commit; replay never repeats a mutation. */
@Experimental
public record PrivilegeMutationReceipt(UUID requestId, long authorizationRevision,
        int changedGrants, Instant recordedAt, boolean replayed) {
    public PrivilegeMutationReceipt {
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(recordedAt, "recordedAt");
        if (authorizationRevision < 0 || changedGrants < 0) {
            throw new IllegalArgumentException("Negative receipt count");
        }
    }
}
