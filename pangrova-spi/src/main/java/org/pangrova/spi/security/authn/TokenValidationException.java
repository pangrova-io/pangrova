package org.pangrova.spi.security.authn;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Raised when an authentication token fails validation.
 *
 * <p>Covers structural, expiry, issuer, audience, algorithm, and signature
 * failures. Authentication providers translate their internal parsing errors
 * into this contract so callers do not bind to a specific provider.</p>
 */
@Experimental
public class TokenValidationException extends RuntimeException {

    public TokenValidationException(String message) {
        super(message);
    }
}
