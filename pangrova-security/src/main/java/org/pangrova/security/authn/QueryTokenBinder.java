package org.pangrova.security.authn;

import org.pangrova.spi.security.UserIdentity;

import java.util.UUID;

/**
 * Binds a successful authentication result to a query-scoped security context,
 * stripping the original token to prevent credential leakage.
 */
public class QueryTokenBinder {

    /**
     * Immutable security context bound to a single query execution.
     */
    public record QuerySecurityContext(String queryId, UserIdentity identity) {}

    /**
     * Creates a query security context from a successful authentication result.
     *
     * @throws IllegalArgumentException if the authentication result indicates failure
     */
    public QuerySecurityContext bind(AuthenticationResult authResult) {
        if (!authResult.success()) {
            throw new IllegalArgumentException("Cannot bind failed authentication");
        }
        String queryId = UUID.randomUUID().toString();
        return new QuerySecurityContext(queryId, authResult.userIdentity());
    }
}
