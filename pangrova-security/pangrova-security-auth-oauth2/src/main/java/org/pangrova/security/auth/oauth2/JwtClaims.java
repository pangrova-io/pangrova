package org.pangrova.security.auth.oauth2;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Strongly-typed JWT claims replacing raw Map&lt;String, Object&gt;.
 * Extracts standard OIDC claims used in authentication and validation.
 *
 * @param issuer       the "iss" claim, may be null
 * @param subject      the "sub" claim, may be null
 * @param audience     the "aud" claim (single string), may be null
 * @param expiration   the "exp" claim as epoch seconds, 0 if absent
 * @param groups       the "groups" claim as a set of strings, empty if absent
 * @param customClaims remaining claims not covered by named fields
 */
record JwtClaims(
        String issuer,
        String subject,
        String audience,
        long expiration,
        Set<String> groups,
        Map<String, String> customClaims
) {

    private static final Set<String> KNOWN_CLAIMS = Set.of("iss", "sub", "aud", "exp", "groups", "roles");

    /**
     * Constructs JwtClaims from a raw decoded JWT payload map.
     * Falls back to "roles" claim if "groups" is absent (OIDC compatibility).
     *
     * @param raw the decoded payload map
     * @return strongly-typed claims
     */
    static JwtClaims fromRawMap(Map<String, Object> raw) {
        String issuer = extractString(raw, "iss");
        String subject = extractString(raw, "sub");
        String audience = extractAudience(raw);
        long expiration = extractLong(raw, "exp");
        Set<String> groups = extractGroups(raw);
        Map<String, String> custom = extractCustomClaims(raw);
        return new JwtClaims(issuer, subject, audience, expiration, groups, custom);
    }

    private static String extractString(Map<String, Object> map, String key) {
        Object val = map.get(key);
        return val != null ? val.toString() : null;
    }

    private static String extractAudience(Map<String, Object> raw) {
        Object aud = raw.get("aud");
        if (aud == null) {
            return null;
        }
        return aud.toString();
    }

    private static long extractLong(Map<String, Object> map, String key) {
        Object val = map.get(key);
        if (val instanceof Number num) {
            return num.longValue();
        }
        if (val instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
        return 0L;
    }

    @SuppressWarnings("unchecked")
    private static Set<String> extractGroups(Map<String, Object> raw) {
        Object groups = raw.get("groups");
        if (groups == null) {
            groups = raw.get("roles");
        }
        if (groups == null) {
            return Set.of();
        }
        return parseGroupValue(groups);
    }

    private static Set<String> parseGroupValue(Object groups) {
        if (groups instanceof List<?> list) {
            return list.stream()
                    .map(Object::toString)
                    .collect(Collectors.toUnmodifiableSet());
        }
        if (groups instanceof String s) {
            return java.util.stream.Stream.of(s.split(","))
                    .map(String::trim)
                    .filter(g -> !g.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }
        return Set.of();
    }

    private static Map<String, String> extractCustomClaims(Map<String, Object> raw) {
        Map<String, String> custom = new LinkedHashMap<>();
        for (var entry : raw.entrySet()) {
            if (!KNOWN_CLAIMS.contains(entry.getKey()) && entry.getValue() != null) {
                custom.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return Collections.unmodifiableMap(custom);
    }
}
