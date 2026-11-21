package org.pangrova.security.api.exception;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Thrown when a user lacks the required privilege for an operation.
 * Public authorization failure shared by policy validation, providers and consumers.
 */
@Experimental
public class AuthorizationDeniedException extends AccessDeniedException {

    public AuthorizationDeniedException(String message) {
        super(message);
    }
}
