package org.pangrova.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.coordinator.http.RequestLimitConfig;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JAX-RS filter enforcing request body size limit and per-IP rate limiting.
 *
 * <p>Request body size: rejects with HTTP 413 (Payload Too Large) if the declared
 * Content-Length exceeds the configured maximum, or if the actual body stream
 * exceeds the limit during read.</p>
 *
 * <p>Rate limiting: uses a Caffeine cache to track request counts per IP within
 * a fixed 1-minute window. Rejects with HTTP 429 (Too Many Requests) when
 * the configured threshold is exceeded.</p>
 *
 * <p>Authenticated internal control is excluded from the public IP budget, not
 * the body limit. Health ({@code /v1/health}) is exempt from both checks.</p>
 *
 * @ThreadSafe
 */
@Provider
@jakarta.annotation.Priority(jakarta.ws.rs.Priorities.AUTHORIZATION)
public final class RequestLimitFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RequestLimitFilter.class);

    private static final String HEALTH_PATH = "v1/health";

    private static final int HTTP_STATUS_PAYLOAD_TOO_LARGE = 413;
    private static final int HTTP_STATUS_TOO_MANY_REQUESTS = 429;

    private final int maxRequestSizeBytes;
    private final int maxRequestsPerMinute;
    private final Cache<String, AtomicInteger> requestCountsByIp;

    /**
     * Creates a filter with the given configuration.
     *
     * @param config request limit configuration (must not be null)
     */
    public RequestLimitFilter(RequestLimitConfig config) {
        checkNotNull(config, "config must not be null");
        this.maxRequestSizeBytes = config.maxRequestSizeBytes();
        this.maxRequestsPerMinute = config.maxRequestsPerMinute();
        this.requestCountsByIp = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(1))
                .build();
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (isHealthCheck(requestContext)) {
            return;
        }

        String clientIp = resolveClientIp(requestContext);

        // Authenticated Worker control cannot share the user-facing IP quota:
        // starving terminal ACKs/heartbeats/deregistration prevents actual owner release.
        // Body limits still apply, and merely naming an internal route is not authentication.
        if (!AuthenticationFilter.hasInternalServiceIdentity(requestContext) && isRateLimitExceeded(clientIp)) {
            rejectTooManyRequests(requestContext, clientIp);
            return;
        }

        if (isContentLengthExceeded(requestContext)) {
            rejectPayloadTooLarge(requestContext, clientIp);
            return;
        }

        wrapInputStreamWithSizeLimit(requestContext);
    }

    private boolean isRateLimitExceeded(String clientIp) {
        AtomicInteger counter = requestCountsByIp.get(clientIp, k -> new AtomicInteger(0));
        int currentCount = counter.incrementAndGet();
        return currentCount > maxRequestsPerMinute;
    }

    private boolean isContentLengthExceeded(ContainerRequestContext requestContext) {
        int contentLength = requestContext.getLength();
        return contentLength > maxRequestSizeBytes;
    }

    private void wrapInputStreamWithSizeLimit(ContainerRequestContext requestContext) {
        InputStream original = requestContext.getEntityStream();
        if (original != null) {
            requestContext.setEntityStream(
                    new SizeLimitedInputStream(original, maxRequestSizeBytes));
        }
    }

    private static String resolveClientIp(ContainerRequestContext ctx) {
        return RequestPeerAddressFilter.resolve(ctx);
    }

    private static boolean isHealthCheck(ContainerRequestContext ctx) {
        String path = ctx.getUriInfo().getPath();
        return HEALTH_PATH.equals(path) || ("/" + HEALTH_PATH).equals(path);
    }

    private static void rejectTooManyRequests(ContainerRequestContext ctx, String ip) {
        LOG.warn("event=rate_limit_exceeded ip={}", ip);
        ErrorDetail error = new ErrorDetail(
                "TOO_MANY_REQUESTS", "TOO_MANY_REQUESTS",
                "Rate limit exceeded. Please try again later.");
        ctx.abortWith(Response.status(HTTP_STATUS_TOO_MANY_REQUESTS)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }

    private static void rejectPayloadTooLarge(ContainerRequestContext ctx, String ip) {
        LOG.warn("event=payload_too_large ip={} contentLength={}", ip, ctx.getLength());
        ErrorDetail error = new ErrorDetail(
                "PAYLOAD_TOO_LARGE", "PAYLOAD_TOO_LARGE",
                "Request body exceeds the maximum allowed size.");
        ctx.abortWith(Response.status(HTTP_STATUS_PAYLOAD_TOO_LARGE)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(error)
                .build());
    }

    /**
     * Returns the current request count for the given IP within the current window.
     * Primarily for testing and observability.
     */
    int getRequestCount(String ip) {
        AtomicInteger counter = requestCountsByIp.getIfPresent(ip);
        return counter != null ? counter.get() : 0;
    }

    /**
     * InputStream wrapper that enforces a byte-count limit during read.
     * Throws IOException if the stream exceeds the configured maximum.
     */
    static final class SizeLimitedInputStream extends InputStream {

        private final InputStream delegate;
        private final int maxBytes;
        private int bytesRead;
        private boolean limitExceeded;

        SizeLimitedInputStream(InputStream delegate, int maxBytes) {
            this.delegate = delegate;
            this.maxBytes = maxBytes;
        }

        @Override
        public int read() throws IOException {
            requireWithinLimit();
            int b = delegate.read();
            if (b != -1) {
                if (bytesRead == maxBytes) rejectLimit();
                bytesRead++;
            }
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            java.util.Objects.checkFromIndexSize(off, len, buf.length);
            if (len == 0) return 0;
            requireWithinLimit();
            // At the boundary, distinguish real EOF from an actual additional byte.
            if (bytesRead == maxBytes) return read();
            int allowedLen = Math.min(len, maxBytes - bytesRead);
            int n = delegate.read(buf, off, allowedLen);
            if (n > 0) {
                bytesRead += n;
            }
            return n;
        }

        private void requireWithinLimit() {
            if (limitExceeded) rejectLimit();
        }

        private void rejectLimit() {
            limitExceeded = true;
            throw new jakarta.ws.rs.WebApplicationException(
                    "Request body exceeds maximum size of " + maxBytes + " bytes",
                    HTTP_STATUS_PAYLOAD_TOO_LARGE);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
