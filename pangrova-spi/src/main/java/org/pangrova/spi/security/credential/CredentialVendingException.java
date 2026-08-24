package org.pangrova.spi.security.credential;

import org.pangrova.spi.annotation.Experimental;

/**
 * Thrown when a credential vending operation fails.
 *
 * <p>Wraps cloud provider STS / token-service errors so callers do not need
 * to depend on provider-specific exception types.</p>
 */
@Experimental(reason = "G-META-03: credential vending exception")
public class CredentialVendingException extends RuntimeException {

    public CredentialVendingException(String message) {
        super(message);
    }

    public CredentialVendingException(String message, Throwable cause) {
        super(message, cause);
    }
}
