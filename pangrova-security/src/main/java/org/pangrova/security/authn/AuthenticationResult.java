package org.pangrova.security.authn;

import org.pangrova.spi.security.UserIdentity;

/**
 * Outcome of an authentication attempt.
 */
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
