package org.pangrova.security.api.authentication;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.security.api.identity.UserIdentity;

/**
 * Outcome of an authentication attempt.
 */
@Experimental
public record AuthenticationResult(
        boolean success,
        UserIdentity userIdentity,
        String errorMessage
) {

    public static AuthenticationResult success(UserIdentity identity) {
        return new AuthenticationResult(true, identity, null);
    }

    public static AuthenticationResult failure(String errorMessage) {
        return new AuthenticationResult(false, null, errorMessage);
    }
}
