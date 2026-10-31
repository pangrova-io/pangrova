package org.pangrova.security.core.authn.provider.oauth2;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.security.api.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.security.api.authn.AuthenticationRequest;
import org.pangrova.security.api.authn.AuthenticationResult;
import org.pangrova.security.api.authn.Authenticator;
import org.pangrova.security.api.authn.TokenValidationException;

/**
 * OAuth2/OIDC token-based authenticator with JJWT-backed JWT validation.
 *
 * <p>Delegates to:
 * <ul>
 *   <li>{@link SecureJwtParser} — JJWT-based JWT parsing with depth/size limits</li>
 *   <li>{@link JwksKeyResolver} — JWKS discovery and public key caching</li>
 *   <li>{@link JwtClaimValidator} — issuer, audience validation</li>
 * </ul>
 *
 */
@ThreadSafe
public class OAuth2Authenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth2Authenticator.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);
    private static final int HTTP_SUCCESS_MINIMUM = 200;
    private static final int HTTP_REDIRECT_MINIMUM = 300;

    private final AtomicReference<OAuth2Config> configRef = new AtomicReference<>();
    private final AtomicReference<Boolean> lastHealthStatus = new AtomicReference<>(false);
    private final HttpClient httpClient;
    final JwksKeyResolver keyResolver;
    private final AtomicReference<SecureJwtParser> jwtParserRef = new AtomicReference<>();

    /**
     * Immutable OAuth2 configuration snapshot.
     */
    record OAuth2Config(
            String issuerUri,
            String audience
    ) {}

    /** Creates an OAuth2 authenticator with a default HTTP client. */
    public OAuth2Authenticator() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .build();
        this.keyResolver = new JwksKeyResolver(httpClient);
    }

    /** Allows injecting a custom HttpClient. */
    @org.pangrova.common.VisibleForTesting
    OAuth2Authenticator(HttpClient httpClient) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.keyResolver = new JwksKeyResolver(httpClient);
    }

    @Override
    public String getName() {
        return "oauth2";
    }

    @Override
    public void init(Map<String, String> config) {
        String uri = config.get("issuerUri");
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("OAuth2 requires issuerUri");
        }
        String audience = config.get("audience");
        if (config.containsKey("requireSignatureVerification")) {
            throw new IllegalArgumentException("OAuth2 signatures are mandatory; no signature toggle is supported");
        }

        OAuth2Config oauthConfig = new OAuth2Config(uri, audience);
        configRef.set(oauthConfig);
        jwtParserRef.set(new SecureJwtParser(keyResolver));

        keyResolver.discoverJwksEndpoint(uri);

        LOG.info("event=oauth2_init issuerUri={} audience={} signatureRequired=true", uri, audience);
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request) {
        if (request.token() == null || request.token().isBlank()) {
            return AuthenticationResult.failure("Token required for OAuth2");
        }

        OAuth2Config cfg = configRef.get();
        if (cfg == null || cfg.issuerUri() == null || cfg.issuerUri().isBlank()) {
            LOG.warn("event=oauth2_auth_failed reason=issuer_not_configured clientIp={}", request.clientIp());
            return AuthenticationResult.failure("OAuth2 issuer not configured");
        }

        return authenticateConfigured(request, cfg);
    }

    private AuthenticationResult authenticateConfigured(AuthenticationRequest request, OAuth2Config config) {
        try {
            SecureJwtParser.ParsedJwt parsed = parseToken(request.token());
            JwtClaims claims = parsed.claims();

            JwtClaimValidator.validateIssuer(claims, config.issuerUri());
            JwtClaimValidator.validateExpiration(claims);
            JwtClaimValidator.validateAudience(claims, config.audience());

            String subject = claims.subject();
            if (subject == null || subject.isBlank()) {
                LOG.warn("event=oauth2_auth_failed reason=missing_subject clientIp={}", request.clientIp());
                return AuthenticationResult.failure("Token missing subject claim");
            }

            Set<String> groups = claims.groups();
            UserIdentity identity = new UserIdentity(subject, groups, Map.of(), Instant.now());

            LOG.info("event=oauth2_auth_success principal={} groups={} clientIp={}",
                    subject, groups.size(), request.clientIp());
            return AuthenticationResult.success(identity);

        } catch (TokenValidationException e) {
            LOG.warn("event=oauth2_auth_failed reason={} clientIp={}", e.getMessage(), request.clientIp());
            return AuthenticationResult.failure(e.getMessage());
        } catch (Exception e) {
            LOG.debug("event=oauth2_internal_error clientIp={} errorType={}",
                    request.clientIp(), e.getClass().getSimpleName());
            throw new AuthenticationException("OAuth2 authentication error", e);
        }
    }

    private SecureJwtParser.ParsedJwt parseToken(String token) {
        SecureJwtParser parser = jwtParserRef.get();
        if (parser == null) {
            parser = new SecureJwtParser(keyResolver);
            jwtParserRef.set(parser);
        }
        JwtAlgorithmValidator.validateRawToken(token);
        SecureJwtParser.ParsedJwt parsed = parser.parseSignedToken(token);
        JwtAlgorithmValidator.validate(parsed.header());
        return parsed;
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        return request.token() != null;
    }

    @Override
    public boolean isHealthy() {
        OAuth2Config cfg = configRef.get();
        if (cfg == null || cfg.issuerUri() == null || cfg.issuerUri().isBlank()) {
            lastHealthStatus.set(false);
            return false;
        }
        try {
            String wellKnown = JwksKeyResolver.buildWellKnownUrl(cfg.issuerUri());
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(wellKnown))
                    .timeout(HTTP_TIMEOUT)
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            boolean healthy = resp.statusCode() >= HTTP_SUCCESS_MINIMUM
                    && resp.statusCode() < HTTP_REDIRECT_MINIMUM;
            lastHealthStatus.set(healthy);
            return healthy;
        } catch (Exception e) {
            LOG.debug("event=oauth2_health_check_failed issuerUri={} error={}", cfg.issuerUri(), e.getMessage());
            lastHealthStatus.set(false);
            return false;
        }
    }

    @Override
    public void close() {
        configRef.set(null);
        jwtParserRef.set(null);
        lastHealthStatus.set(false);
        keyResolver.clear();
        LOG.debug("event=oauth2_close");
    }

    /**
     * Exception for unexpected authentication errors (I/O, parsing, etc.).
     */
    static class AuthenticationException extends RuntimeException {
        AuthenticationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
