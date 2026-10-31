package org.pangrova.security.core.authn.provider.oauth2;

import java.util.Map;

/**
 * Strongly-typed JWT header replacing raw Map&lt;String, Object&gt;.
 * Extracts the three standard JOSE header fields used in signature verification.
 *
 * @param alg the signing algorithm (e.g., "RS256", "none")
 * @param typ the token type (typically "JWT")
 * @param kid the key ID for JWKS lookup, may be null
 */
record JwtHeader(String alg, String typ, String kid) {

    /**
     * Constructs a JwtHeader from a raw decoded JWT header map.
     *
     * @param raw the decoded header map
     * @return strongly-typed header
     */
    static JwtHeader fromRawMap(Map<String, Object> raw) {
        return new JwtHeader(
                extractString(raw, "alg"),
                extractString(raw, "typ"),
                extractString(raw, "kid"));
    }

    private static String extractString(Map<String, Object> map, String key) {
        Object val = map.get(key);
        return val != null ? val.toString() : null;
    }
}
