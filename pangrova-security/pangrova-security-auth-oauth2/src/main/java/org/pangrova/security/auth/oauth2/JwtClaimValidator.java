package org.pangrova.security.auth.oauth2;

import org.pangrova.security.api.authentication.TokenValidationException;

import java.time.Instant;

/**
 * Validates JWT claims (issuer, expiration, audience) using strongly-typed {@link JwtClaims}.
 */
final class JwtClaimValidator {

    private JwtClaimValidator() {}

    static void validateIssuer(JwtClaims claims, String expectedIssuer) {
        String issuer = claims.issuer();
        if (issuer == null || !issuer.equals(expectedIssuer)) {
            throw new TokenValidationException("Token issuer mismatch");
        }
    }

    static void validateExpiration(JwtClaims claims) {
        long expSeconds = claims.expiration();
        if (expSeconds == 0L) {
            throw new TokenValidationException("Token missing expiration claim");
        }
        if (Instant.ofEpochSecond(expSeconds).isBefore(Instant.now())) {
            throw new TokenValidationException("Token expired");
        }
    }

    static void validateAudience(JwtClaims claims, String expectedAudience) {
        if (expectedAudience == null || expectedAudience.isBlank()) {
            return;
        }
        String audience = claims.audience();
        if (audience == null) {
            throw new TokenValidationException("Token missing audience claim");
        }
        if (!audience.equals(expectedAudience) && !audience.contains(expectedAudience)) {
            throw new TokenValidationException("Token audience mismatch");
        }
    }
}
