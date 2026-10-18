package org.pangrova.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.pangrova.security.authn.BruteForceGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * JAX-RS ContainerRequestFilter that authenticates HTTP requests via AuthenticationManager.
 *
 * <p>Extracts credentials from the Authorization header (Bearer token or Basic auth),
 * sets UserIdentity as a request property on success, aborts with HTTP 401 on failure.
 * Skips authentication for the readiness and liveness health endpoints.</p>
 *
 * <p>Integrates per-IP brute force protection: blocked IPs receive HTTP 429 before
 * authentication is attempted. Failed attempts are recorded; successful authentication
 * resets the counter.</p>
 */
@Provider
public final class AuthenticationFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

    /** Request property key for the authenticated UserIdentity. */
    public static final String USER_IDENTITY_PROPERTY = "pangrova.userIdentity";
    private static final String INTERNAL_IDENTITY_PROPERTY = "pangrova.internalServiceIdentity";

    private enum InternalServiceIdentity { AUTHENTICATED }

    private static final String HEALTH_PATH = "v1/health";
    private static final String HEALTH_LIVE_PATH = "v1/health/live";
    private static final String UNKNOWN_IP = "unknown";

    private static final int HTTP_STATUS_TOO_MANY_REQUESTS = 429;

    /**
     * Internal RPC route prefixes. Internal service tokens are only accepted on these
     * routes. F-CHAIN-SEC-06: prevents an internal token from reaching user-facing
     * endpoints (e.g., /v1/query) where it would bypass user identity resolution.
     */
    private static final String[] INTERNAL_ROUTE_PREFIXES = {
            "v1/worker", "v1/workers", "v1/task", "v1/internal/catalog-deployments",
            "v1/internal/copy-source-read"
    };

    private final AuthenticationManager authenticationManager;
    private final String internalAuthSecret;
    private final BruteForceGuard bruteForceGuard;
    private final int trustedProxyCount;

    public AuthenticationFilter(AuthenticationManager authenticationManager) {
        this(authenticationManager, null, new BruteForceGuard(), 1);
    }

    public AuthenticationFilter(AuthenticationManager authenticationManager, String internalAuthSecret) {
        this(authenticationManager, internalAuthSecret, new BruteForceGuard(), 1);
    }

    public AuthenticationFilter(AuthenticationManager authenticationManager,
                                String internalAuthSecret,
                                BruteForceGuard bruteForceGuard) {
        this(authenticationManager, internalAuthSecret, bruteForceGuard, 1);
    }

    /** Full constructor with configurable brute-force guard and trusted proxy count. */
    public AuthenticationFilter(AuthenticationManager authenticationManager,
                                String internalAuthSecret,
                                BruteForceGuard bruteForceGuard,
                                int trustedProxyCount) {
        this.authenticationManager = checkNotNull(authenticationManager,
                "authenticationManager must not be null");
        this.internalAuthSecret = internalAuthSecret;
        this.bruteForceGuard = checkNotNull(bruteForceGuard, "bruteForceGuard must not be null");
        this.trustedProxyCount = Math.max(1, trustedProxyCount);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (isHealthCheck(requestContext)) {
            return;
        }

        // F-CHAIN-SEC-06: check internal token. If valid on an internal route, pass through.
        // If valid on a user-facing route, reject with 403 (token isolation).
        if (isInternalToken(requestContext)) {
            if (isInternalRoute(requestContext)) {
                requestContext.setProperty(INTERNAL_IDENTITY_PROPERTY, InternalServiceIdentity.AUTHENTICATED);
                return;
            }
            rejectForbidden(requestContext, "internal service token not allowed on user-facing endpoints");
            return;
        }

        String clientIp = resolveClientIp(requestContext);

        if (bruteForceGuard.isBlocked(clientIp)) {
            rejectTooManyRequests(requestContext, clientIp);
            return;
        }

        AuthenticationResult result = authenticate(requestContext);
        if (!result.success()) {
            bruteForceGuard.recordFailedAttempt(clientIp);
            rejectUnauthenticated(requestContext, result.errorMessage());
            return;
        }

        bruteForceGuard.recordSuccess(clientIp);
        requestContext.setProperty(USER_IDENTITY_PROPERTY, result.userIdentity());
    }

    /**
     * Requires the internal identity produced by this filter, not an ordinary user
     * identity or a caller-supplied HTTP header.
     *
     * @param context the actual server request context
     * @throws jakarta.ws.rs.ForbiddenException when no internal service was authenticated
     */
    public static void requireInternalServiceIdentity(ContainerRequestContext context) {
        if (context == null || context.getProperty(INTERNAL_IDENTITY_PROPERTY)
                != InternalServiceIdentity.AUTHENTICATED) {
            throw new jakarta.ws.rs.ForbiddenException("Authenticated internal service identity is required");
        }
    }

    private boolean isInternalToken(ContainerRequestContext ctx) {
        if (internalAuthSecret == null || internalAuthSecret.isBlank()) {
            return false;
        }
        String authHeader = ctx.getHeaderString("Authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return false;
        }
        String token = authHeader.substring("Bearer ".length()).trim();
        return java.security.MessageDigest.isEqual(
                internalAuthSecret.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                token.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private AuthenticationResult authenticate(ContainerRequestContext ctx) {
        AuthenticationRequest authRequest = extractCredentials(ctx);
        return authenticationManager.authenticate(authRequest);
    }

    private static AuthenticationRequest extractCredentials(ContainerRequestContext ctx) {
        String authHeader = ctx.getHeaderString("Authorization");
        String clientIp = extractClientIp(ctx);

        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring("Bearer ".length()).trim();
            return new AuthenticationRequest(null, null, token, null, Map.of(), clientIp);
        }
        if (authHeader != null && authHeader.startsWith("Basic ")) {
            String[] decoded = decodeBasicAuth(authHeader);
            return new AuthenticationRequest(
                    decoded[0], decoded[1].toCharArray(), null, null, Map.of(), clientIp);
        }
        return new AuthenticationRequest(null, null, null, null, Map.of(), clientIp);
    }

    private static String extractClientIp(ContainerRequestContext ctx) {
        return extractClientIp(ctx, 1);
    }

    /**
     * Extracts the client IP from X-Forwarded-For, respecting the trustedProxyCount.
     * The Nth entry from the right is the real client IP when N proxies are trusted.
     */
    private static String extractClientIp(ContainerRequestContext ctx, int trustedProxyCount) {
        String forwarded = ctx.getHeaderString("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            String[] parts = forwarded.split(",");
            int idx = parts.length - trustedProxyCount;
            return parts[Math.max(0, idx)].trim();
        }
        return null;
    }

    /**
     * Resolves the client IP for brute force tracking.
     * Falls back to "unknown" if no IP can be determined (still tracked to prevent bypass).
     */
    private static String resolveClientIp(ContainerRequestContext ctx) {
        String ip = extractClientIp(ctx);
        return (ip != null && !ip.isBlank()) ? ip : UNKNOWN_IP;
    }

    private static String[] decodeBasicAuth(String header) {
        String encoded = header.substring("Basic ".length()).trim();
        String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        int colon = decoded.indexOf(':');
        if (colon < 0) {
            return new String[]{decoded, ""};
        }
        return new String[]{decoded.substring(0, colon), decoded.substring(colon + 1)};
    }

    private static boolean isHealthCheck(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return HEALTH_PATH.equals(path) || HEALTH_LIVE_PATH.equals(path);
    }

    /**
     * Returns true if the request targets an internal RPC route
     * (worker heartbeat, task status, etc.).
     */
    private static boolean isInternalRoute(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        for (String prefix : INTERNAL_ROUTE_PREFIXES) {
            if (path.equals(prefix) || path.startsWith(prefix + "/")) {
                return true;
            }
        }
        return false;
    }

    private static void rejectForbidden(ContainerRequestContext ctx, String message) {
        LOG.warn("event=internal_token_isolation_rejected path={}", ctx.getUriInfo().getPath());
        ErrorDetail error = new ErrorDetail("FORBIDDEN", "FORBIDDEN", message);
        ctx.abortWith(Response.status(Response.Status.FORBIDDEN)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }

    private static void rejectTooManyRequests(ContainerRequestContext ctx, String ip) {
        LOG.warn("event=brute_force_blocked ip={}", ip);

        ErrorDetail error = new ErrorDetail(
                "TOO_MANY_REQUESTS", "TOO_MANY_REQUESTS",
                "Too many failed authentication attempts. Please try again later.");
        ctx.abortWith(Response.status(HTTP_STATUS_TOO_MANY_REQUESTS)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }

    private static void rejectUnauthenticated(ContainerRequestContext ctx, String message) {
        String errorMessage = (message != null) ? message : "Authentication required";
        LOG.warn("Authentication failed: {}", errorMessage);

        ErrorDetail error = new ErrorDetail("UNAUTHORIZED", "UNAUTHORIZED", errorMessage);
        ctx.abortWith(Response.status(Response.Status.UNAUTHORIZED)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }
}
