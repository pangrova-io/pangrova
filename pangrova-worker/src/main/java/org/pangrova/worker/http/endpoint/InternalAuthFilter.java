package org.pangrova.worker.http.endpoint;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Shared-secret authentication filter for Worker HTTP endpoints.
 *
 * <p>Validates that incoming requests from the Coordinator carry the correct
 * shared secret in the {@code X-Internal-Auth} header. This prevents
 * unauthorized task submission, cancellation, and exchange source injection
 * from untrusted clients that can reach the Worker HTTP port.</p>
 *
 * <p>The exact readiness and liveness endpoints ({@code /v1/health} and
 * {@code /v1/health/live}) are exempt from authentication to allow probes.</p>
 *
 * <p>The shared secret should be configured via environment variable
 * {@code PANGROVA_INTERNAL_AUTH_SECRET} or YAML config
 * {@code worker.internal-auth-secret}.</p>
 */
@Provider
public final class InternalAuthFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(InternalAuthFilter.class);

    static final String AUTH_HEADER = "X-Internal-Auth";
    private static final String HEALTH_PATH = "v1/health";
    private static final String LIVE_PATH = "v1/health/live";

    private final byte[] expectedSecret;

    /**
     * @param expectedSecret the shared secret that Coordinator must present
     */
    public InternalAuthFilter(String expectedSecret) {
        if (expectedSecret == null || expectedSecret.isBlank()) {
            throw new IllegalArgumentException("expectedSecret must not be blank");
        }
        this.expectedSecret = expectedSecret.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (isHealthCheck(requestContext)) {
            return;
        }

        String provided = requestContext.getHeaderString(AUTH_HEADER);
        byte[] providedBytes = provided == null
                ? new byte[0]
                : provided.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expectedSecret, providedBytes)) {
            LOG.warn("event=internal_auth_rejected path={} remoteAddr={}",
                    requestContext.getUriInfo().getPath(),
                    requestContext.getHeaderString("X-Forwarded-For"));
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .entity(new ErrorDetail("UNAUTHORIZED", "INTERNAL_AUTH",
                            "Missing or invalid X-Internal-Auth header"))
                    .build());
        }
    }

    private static boolean isHealthCheck(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        return HEALTH_PATH.equals(path) || ("/" + HEALTH_PATH).equals(path)
                || LIVE_PATH.equals(path) || ("/" + LIVE_PATH).equals(path);
    }
}
