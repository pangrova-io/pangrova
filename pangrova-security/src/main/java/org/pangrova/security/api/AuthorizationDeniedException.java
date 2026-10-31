package org.pangrova.security.api;

/**
 * Thrown when a user lacks the required privilege for an operation.
 * Public authorization failure shared by policy validation, providers and consumers.
 */
public class AuthorizationDeniedException extends AccessDeniedException {

    public AuthorizationDeniedException(String message) {
        super(message);
    }
}
