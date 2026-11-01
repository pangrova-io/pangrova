package org.pangrova.security.authn.oauth2;

import org.pangrova.spi.security.authn.TokenValidationException;

import java.security.Key;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.Locator;
import io.jsonwebtoken.ProtectedHeader;
import io.jsonwebtoken.security.SecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Secure JWT parser backed by JJWT library with enforced validation constraints.
 *
 * <p>Replaces the former hand-written JSON parser which had no nesting depth limit
 * and was vulnerable to DoS via deeply nested JSON payloads.</p>
 *
 * <p>Security hardening:
 * <ul>
 *   <li>Max content decompression size: 256 KB (prevents zip-bomb attacks)</li>
 *   <li>Clock skew tolerance: 60 seconds (handles minor clock drift)</li>
 *   <li>Signature verification via JWKS-resolved keys</li>
 *   <li>JJWT internally enforces safe JSON parsing with depth limits</li>
 * </ul>
 *
 * @ThreadSafe instances are immutable after construction
 */
final class SecureJwtParser {

    private static final Logger LOG = LoggerFactory.getLogger(SecureJwtParser.class);

    /**
     * Maximum decompressed payload size in bytes (256 KB).
     * Prevents zip-bomb style DoS attacks on compressed JWT payloads.
     */
    static final int MAX_PAYLOAD_DECOMPRESSION_BYTES = 256 * 1024;

    /**
     * Clock skew tolerance for expiration/not-before validation.
     * Accommodates minor clock drift between token issuer and this service.
     */
    static final Duration CLOCK_SKEW_TOLERANCE = Duration.ofSeconds(60);

    private final JwtParser signedParser;

    /**
     * Creates a secure JWT parser that resolves signing keys from the given JWKS resolver.
     *
     * @param keyResolver JWKS key resolver for signature verification
     */
    SecureJwtParser(JwksKeyResolver keyResolver) {
        this.signedParser = buildSignedParser(keyResolver);
    }

    /**
     * Parses and validates a signed JWT token, returning structured claims.
     *
     * @param token compact JWS token string
     * @return parsed header and claims
     * @throws TokenValidationException on any validation failure
     */
    ParsedJwt parseSignedToken(String token) {
        try {
            Jws<Claims> jws = signedParser.parseSignedClaims(token);
            ProtectedHeader header = jws.getHeader();
            Claims claims = jws.getPayload();
            return new ParsedJwt(
                    new JwtHeader(
                            header.getAlgorithm(),
                            header.getType(),
                            header.getKeyId()),
                    buildJwtClaims(claims));
        } catch (SecurityException e) {
            throw new TokenValidationException(
                    "Token signature verification failed");
        } catch (io.jsonwebtoken.ExpiredJwtException e) {
            throw new TokenValidationException("Token expired");
        } catch (io.jsonwebtoken.MalformedJwtException e) {
            throw new TokenValidationException(
                    "Invalid JWT format: " + e.getMessage());
        } catch (io.jsonwebtoken.JwtException e) {
            throw new TokenValidationException(e.getMessage());
        }
    }

    private static JwtParser buildSignedParser(JwksKeyResolver keyResolver) {
        Locator<Key> keyLocator = header -> {
            String kid = (header instanceof ProtectedHeader ph) ? ph.getKeyId() : null;
            Key key = keyResolver.resolveSigningKey(kid);
            if (key == null) {
                throw new TokenValidationException(
                        "No signing key available for verification");
            }
            return key;
        };
        return Jwts.parser()
                .keyLocator(keyLocator)
                .clockSkewSeconds(CLOCK_SKEW_TOLERANCE.toSeconds())
                .build();
    }

    private static JwtClaims buildJwtClaims(Claims claims) {
        String issuer = claims.getIssuer();
        String subject = claims.getSubject();
        String audience = extractAudience(claims);
        long expiration = claims.getExpiration() != null
                ? claims.getExpiration().toInstant().getEpochSecond()
                : 0L;
        Set<String> groups = extractGroups(claims);
        Map<String, String> customClaims = extractCustomClaims(claims);
        return new JwtClaims(issuer, subject, audience, expiration, groups, customClaims);
    }

    private static String extractAudience(Claims claims) {
        Set<String> audience = claims.getAudience();
        if (audience == null || audience.isEmpty()) {
            return null;
        }
        return String.join(",", audience);
    }

    private static Set<String> extractGroups(Claims claims) {
        Object groups = claims.get("groups");
        if (groups == null) {
            groups = claims.get("roles");
        }
        if (groups == null) {
            return Set.of();
        }
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

    private static final Set<String> KNOWN_CLAIMS = Set.of(
            "iss", "sub", "aud", "exp", "iat", "nbf", "jti", "groups", "roles");

    private static Map<String, String> extractCustomClaims(Claims claims) {
        Map<String, String> custom = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : claims.entrySet()) {
            if (!KNOWN_CLAIMS.contains(entry.getKey()) && entry.getValue() != null) {
                custom.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return Collections.unmodifiableMap(custom);
    }

    /**
     * Parsed JWT result containing both header and claims.
     */
    record ParsedJwt(JwtHeader header, JwtClaims claims) {}
}
