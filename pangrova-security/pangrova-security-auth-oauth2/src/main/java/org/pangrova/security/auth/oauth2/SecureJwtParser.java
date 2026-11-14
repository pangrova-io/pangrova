package org.pangrova.security.auth.oauth2;

import org.pangrova.security.api.exception.TokenValidationException;

import java.security.Key;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.Locator;
import io.jsonwebtoken.ProtectedHeader;
import io.jsonwebtoken.security.SecurityException;

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
    JwtClaims parseSignedToken(String token) {
        JwtAlgorithmValidator.validateRawToken(token);
        try {
            Jws<Claims> jws = signedParser.parseSignedClaims(token);
            return buildJwtClaims(jws.getPayload());
        } catch (SecurityException e) {
            throw new TokenValidationException(
                    "Token signature verification failed");
        } catch (io.jsonwebtoken.ExpiredJwtException e) {
            throw new TokenValidationException("Token expired");
        } catch (io.jsonwebtoken.MalformedJwtException e) {
            throw new TokenValidationException(
                    "Invalid JWT format");
        } catch (io.jsonwebtoken.JwtException e) {
            throw new TokenValidationException(e.getMessage());
        }
    }

    private static JwtParser buildSignedParser(JwksKeyResolver keyResolver) {
        Locator<Key> keyLocator = header -> {
            JwtAlgorithmValidator.validateAlgorithm(header.getAlgorithm());
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
                .sig().clear().add(Jwts.SIG.RS256).add(Jwts.SIG.ES256).and()
                .zip().clear()
                    .add(new BoundedJwtCompression(Jwts.ZIP.DEF))
                    .add(new BoundedJwtCompression(Jwts.ZIP.GZIP)).and()
                .clockSkewSeconds(CLOCK_SKEW_TOLERANCE.toSeconds())
                .build();
    }

    private static JwtClaims buildJwtClaims(Claims claims) {
        String issuer = claims.getIssuer();
        String subject = claims.getSubject();
        long expiration = claims.getExpiration() != null
                ? claims.getExpiration().toInstant().getEpochSecond()
                : 0L;
        return new JwtClaims(issuer, subject, claims.getAudience(), expiration, extractGroups(claims));
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
            Set<String> names = new HashSet<>();
            for (Object value : list) {
                if (!(value instanceof String name)) {
                    throw new TokenValidationException("Token group names must be strings");
                }
                names.add(name);
            }
            return names;
        }
        if (groups instanceof String s) {
            return java.util.stream.Stream.of(s.split(","))
                    .map(String::trim)
                    .filter(g -> !g.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }
        throw new TokenValidationException("Token group names must be strings");
    }
}
