package org.pangrova.security.authn.oauth2;

import org.pangrova.spi.security.authn.TokenValidationException;

import java.util.Set;

/**
 * Enforces a whitelist of allowed JWT signing algorithms.
 *
 * <p>Rejects tokens using disallowed algorithms including:
 * <ul>
 *   <li>{@code alg=none} — unsigned tokens are never accepted</li>
 *   <li>Symmetric algorithms (HS256, HS384, HS512) when asymmetric keys are configured</li>
 *   <li>Any algorithm not in the explicit whitelist</li>
 * </ul>
 *
 * <p>This prevents algorithm confusion attacks where an attacker crafts a token
 * with a weaker or inappropriate algorithm to bypass signature verification.</p>
 */
final class JwtAlgorithmValidator {

    /**
     * Allowed asymmetric signing algorithms. Only these algorithms are accepted
     * when the system is configured with asymmetric (public/private) keys.
     */
    static final Set<String> ALLOWED_ALGORITHMS = Set.of("RS256", "ES256");

    /**
     * Symmetric algorithms that must be rejected when asymmetric keys are configured.
     * Accepting these would allow an attacker to sign tokens using the public key as
     * an HMAC secret (algorithm confusion attack).
     */
    private static final Set<String> SYMMETRIC_ALGORITHMS = Set.of("HS256", "HS384", "HS512");

    static final String ERROR_ALG_NONE = "JWT_ALG_NONE_REJECTED";
    static final String ERROR_ALG_SYMMETRIC = "JWT_ALG_SYMMETRIC_REJECTED";
    static final String ERROR_ALG_NOT_ALLOWED = "JWT_ALG_NOT_ALLOWED";

    private JwtAlgorithmValidator() {}

    /**
     * Validates the JWT algorithm against the allowed whitelist.
     *
     * @param header the decoded JWT header containing the algorithm
     * @throws TokenValidationException if the algorithm is not allowed
     */
    static void validate(JwtHeader header) {
        String alg = header.alg();
        validateAlgorithm(alg);
    }

    /**
     * Validates the algorithm from a raw JWT token string before JJWT parsing.
     * Extracts the algorithm from the Base64url-encoded header segment and validates
     * it against the whitelist. This ensures rejection with specific error codes
     * before the JJWT library processes the token.
     *
     * @param token the raw compact JWT token string
     * @throws TokenValidationException if the algorithm is not allowed
     */
    static void validateRawToken(String token) {
        if (token == null || token.isBlank()) {
            throw new TokenValidationException(
                    ERROR_ALG_NONE + ": Token is empty or null");
        }
        int firstDot = token.indexOf('.');
        if (firstDot <= 0) {
            throw new TokenValidationException(
                    ERROR_ALG_NONE + ": Invalid JWT structure");
        }
        String headerSegment = token.substring(0, firstDot);
        String alg = extractAlgorithmFromHeader(headerSegment);
        validateAlgorithm(alg);
    }

    private static String extractAlgorithmFromHeader(String base64Header) {
        try {
            byte[] decoded = java.util.Base64.getUrlDecoder().decode(base64Header);
            String json = new String(decoded, java.nio.charset.StandardCharsets.UTF_8);
            int algIdx = json.indexOf("\"alg\"");
            if (algIdx < 0) {
                return null;
            }
            int colonIdx = json.indexOf(':', algIdx + 5);
            if (colonIdx < 0) {
                return null;
            }
            int valueStart = json.indexOf('"', colonIdx + 1);
            if (valueStart < 0) {
                return null;
            }
            int valueEnd = json.indexOf('"', valueStart + 1);
            if (valueEnd < 0) {
                return null;
            }
            return json.substring(valueStart + 1, valueEnd);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static void validateAlgorithm(String alg) {
        if (alg == null || alg.isBlank() || "none".equalsIgnoreCase(alg)) {
            throw new TokenValidationException(
                    ERROR_ALG_NONE + ": Unsigned tokens (alg=none) are not accepted");
        }

        if (SYMMETRIC_ALGORITHMS.contains(alg)) {
            throw new TokenValidationException(
                    ERROR_ALG_SYMMETRIC + ": Symmetric algorithm '" + alg
                            + "' is not allowed when asymmetric keys are configured");
        }

        if (!ALLOWED_ALGORITHMS.contains(alg)) {
            throw new TokenValidationException(
                    ERROR_ALG_NOT_ALLOWED + ": Algorithm '" + alg
                            + "' is not in the allowed list " + ALLOWED_ALGORITHMS);
        }
    }
}
