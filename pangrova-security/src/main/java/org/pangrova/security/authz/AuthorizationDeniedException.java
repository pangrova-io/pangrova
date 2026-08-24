package org.pangrova.security.authz;

/**
 * Thrown when a user lacks the required privilege for an operation.
 * Extends the SPI AccessDeniedException for backward compatibility.
 */
public class AuthorizationDeniedException extends org.pangrova.spi.security.AccessDeniedException {

    public AuthorizationDeniedException(String message) {
        super(message);
    }
}
