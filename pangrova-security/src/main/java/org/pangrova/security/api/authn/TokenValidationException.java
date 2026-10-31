package org.pangrova.security.api.authn;

/**
 * Raised when an authentication token fails validation.
 *
 * <p>Covers structural, expiry, issuer, audience, algorithm, and signature
 * failures. Authentication providers translate their internal parsing errors
 * into this contract so callers do not bind to a specific provider.</p>
 */
public class TokenValidationException extends RuntimeException {

    public TokenValidationException(String message) {
        super(message);
    }
}
