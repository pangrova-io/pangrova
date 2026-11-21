package org.pangrova.security.auth.oauth2;

import org.pangrova.security.api.exception.TokenValidationException;

import java.io.IOException;
import java.util.Base64;
import java.util.Set;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

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

    private static final ObjectReader HEADER = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build().readerFor(JsonNode.class);

    private JwtAlgorithmValidator() {}

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
        int payloadEnd = token.indexOf('.', firstDot + 1);
        if (payloadEnd < 0 || token.indexOf('.', payloadEnd + 1) >= 0) {
            throw new TokenValidationException("Invalid JWT structure");
        }
        int limit = SecureJwtParser.MAX_PAYLOAD_DECOMPRESSION_BYTES;
        // Bound all compact segments before allocating decoded arrays. Compressed data may include framing.
        if (token.length() > 2L * limit) {
            throw new TokenValidationException("JWT exceeds the size limit");
        }
        JsonNode header;
        try {
            header = HEADER.readTree(Base64.getUrlDecoder().decode(token.substring(0, firstDot)));
        } catch (IllegalArgumentException | IOException e) {
            throw new TokenValidationException("Invalid JWT header");
        }
        if (header == null || !header.isObject()) {
            throw new TokenValidationException("Invalid JWT header");
        }
        validateAlgorithm(header.path("alg").textValue());
        if (!header.hasNonNull("zip")) {
            int last = payloadEnd;
            while (last > firstDot + 1 && token.charAt(last - 1) == '=') {
                last--;
            }
            if ((last - firstDot - 1) * 3L / 4 > limit) {
                throw new TokenValidationException("JWT payload exceeds the size limit");
            }
        }
    }

    static void validateAlgorithm(String alg) {
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
