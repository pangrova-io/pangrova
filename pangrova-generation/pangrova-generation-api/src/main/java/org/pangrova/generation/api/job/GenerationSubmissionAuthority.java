package org.pangrova.generation.api.job;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import java.util.Objects;

/** Persisted real subject and exact scope authorized before external snapshot effects. */
@Experimental
public record GenerationSubmissionAuthority(String tenantId, String principal,
                                              DeferredAuthorizationScope authorizationScope) {
    public GenerationSubmissionAuthority {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("principal must not be blank");
        }
        authorizationScope = Objects.requireNonNull(authorizationScope, "authorizationScope");
    }
}
