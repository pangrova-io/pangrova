package org.pangrova.server.coordinator.http.endpoint;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.common.client.constants.ApiVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JAX-RS filter that enforces REST API version negotiation.
 *
 * <p>On the request path:
 * <ul>
 *   <li>If the client sends {@code X-Pangrova-Api-Version} with a version outside the
 *       supported range [{@link ApiVersion#MIN_SUPPORTED}, {@link ApiVersion#CURRENT}],
 *       the filter aborts with HTTP 422 (Unprocessable Entity).</li>
 *   <li>If the header is absent (legacy client or probes), the request is allowed through
 *       with a WARN log to support graceful migration.</li>
 *   <li>Health-check and server-info endpoints are exempt so that load balancers and
 *       version-discovery calls always succeed.</li>
 * </ul>
 *
 * <p>On the response path, the filter always sets the {@code X-Pangrova-Api-Version}
 * response header so clients can detect the server's current API version regardless
 * of whether they sent the header.
 */
@Provider
public final class ApiVersionFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ApiVersionFilter.class);

    /** Paths exempt from version checking (probes, version discovery). */
    private static final String HEALTH_PATH = "v1/health";
    private static final String HEALTH_LIVE_PATH = "v1/health/live";
    private static final String SERVER_INFO_PATH = "v1/server/info";

    private static final int HTTP_STATUS_UNPROCESSABLE_ENTITY = 422;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (isExempt(requestContext)) {
            return;
        }

        String versionHeader = requestContext.getHeaderString(ApiVersion.HEADER_NAME);
        if (versionHeader == null || versionHeader.isBlank()) {
            LOG.warn("event=missing_api_version_header path={} remoteIp={}",
                    requestContext.getUriInfo().getPath(), resolveClientIp(requestContext));
            return;
        }

        int clientVersion;
        try {
            clientVersion = Integer.parseInt(versionHeader.trim());
        } catch (NumberFormatException e) {
            rejectInvalidVersion(requestContext, versionHeader);
            return;
        }

        if (clientVersion < ApiVersion.MIN_SUPPORTED || clientVersion > ApiVersion.CURRENT) {
            rejectUnsupportedVersion(requestContext, clientVersion);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) {
        responseContext.getHeaders().putSingle(
                ApiVersion.HEADER_NAME, String.valueOf(ApiVersion.CURRENT));
    }

    private static boolean isExempt(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        // Strip leading slash if present
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return HEALTH_PATH.equals(path) || HEALTH_LIVE_PATH.equals(path)
                || SERVER_INFO_PATH.equals(path);
    }

    private static void rejectUnsupportedVersion(ContainerRequestContext ctx, int clientVersion) {
        LOG.warn("event=api_version_unsupported clientVersion={} minSupported={} current={} path={}",
                clientVersion, ApiVersion.MIN_SUPPORTED, ApiVersion.CURRENT,
                ctx.getUriInfo().getPath());
        String message = String.format(
                "Client API version %d is not supported. Server supports versions %d through %d.",
                clientVersion, ApiVersion.MIN_SUPPORTED, ApiVersion.CURRENT);
        ErrorDetail error = new ErrorDetail(
                ApiVersion.ERROR_CODE_UNSUPPORTED, "API_VERSION_ERROR", message);
        ctx.abortWith(Response.status(HTTP_STATUS_UNPROCESSABLE_ENTITY)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }

    private static void rejectInvalidVersion(ContainerRequestContext ctx, String headerValue) {
        LOG.warn("event=api_version_invalid headerValue={} path={}",
                headerValue, ctx.getUriInfo().getPath());
        String message = String.format(
                "Invalid API version header value: '%s'. Expected an integer.", headerValue);
        ErrorDetail error = new ErrorDetail(
                ApiVersion.ERROR_CODE_UNSUPPORTED, "API_VERSION_ERROR", message);
        ctx.abortWith(Response.status(HTTP_STATUS_UNPROCESSABLE_ENTITY)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }

    private static String resolveClientIp(ContainerRequestContext ctx) {
        String forwarded = ctx.getHeaderString("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        return "unknown";
    }
}
