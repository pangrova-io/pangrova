package org.pangrova.observability.metrics;

import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricAttributes;
import org.pangrova.spi.observability.MetricsProvider;

import java.util.List;

/**
 * Collects HTTP request metrics and exports them through the Metrics SPI.
 *
 * <p>Metrics collected:
 * <ul>
 *   <li>Request count: total number of HTTP requests processed</li>
 *   <li>Duration: request duration distribution (histogram, in seconds)</li>
 *   <li>Error count: total number of requests resulting in 4xx/5xx responses</li>
 * </ul>
 *
 * <p>All metrics are recorded with dimensional attributes (status code class,
 * HTTP method) enabling per-dimension aggregation without metric name
 * concatenation.
 *
 * <p>This collector is designed to be called from an HTTP interceptor/filter
 * at request completion time. The caller provides the response status code
 * and elapsed duration for each request.
 */
public final class HttpMetricsCollector {

    private static final List<Double> DURATION_BOUNDARIES_SECONDS =
            List.of(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0);

    private static final int CLIENT_ERROR_THRESHOLD = 400;

    private final LongCounter requestCount;
    private final DoubleHistogram requestDuration;
    private final LongCounter errorCount;

    /**
     * Creates the collector and registers its instruments with the given provider.
     */
    public HttpMetricsCollector(MetricsProvider provider) {
        this.requestCount = provider.createCounter(
                "http.server.request.total", "Total HTTP requests processed", "{request}");
        this.requestDuration = provider.createHistogram(
                "http.server.request.duration", "HTTP request duration", "s",
                DURATION_BOUNDARIES_SECONDS);
        this.errorCount = provider.createCounter(
                "http.server.error.total", "Total HTTP error responses (4xx/5xx)", "{error}");
    }

    /**
     * Records a completed HTTP request with dimensional attributes including route.
     *
     * @param statusCode      the HTTP response status code
     * @param durationSeconds  the request duration in seconds; must be non-negative
     * @param method          the HTTP method (GET, POST, etc.)
     * @param route           the matched HTTP route pattern (e.g., "/v1/query/:queryId")
     */
    public void recordRequest(int statusCode, double durationSeconds, String method, String route) {
        MetricAttributes attrs = MetricAttributes.of(
                "http.method", method,
                "http.route", route != null ? route : "unknown",
                "http.status_class", statusCodeClass(statusCode));
        requestCount.add(1, attrs);
        requestDuration.record(durationSeconds, attrs);
        if (statusCode >= CLIENT_ERROR_THRESHOLD) {
            errorCount.add(1, attrs);
        }
    }

    /**
     * Records a completed HTTP request with dimensional attributes (without route).
     *
     * @param statusCode   the HTTP response status code
     * @param durationSeconds the request duration in seconds; must be non-negative
     * @param method       the HTTP method (GET, POST, etc.)
     */
    public void recordRequest(int statusCode, double durationSeconds, String method) {
        recordRequest(statusCode, durationSeconds, method, null);
    }

    /**
     * Records a completed HTTP request without method information.
     *
     * @param statusCode   the HTTP response status code
     * @param durationSeconds the request duration in seconds; must be non-negative
     */
    public void recordRequest(int statusCode, double durationSeconds) {
        MetricAttributes attrs = MetricAttributes.of(
                "http.status_class", statusCodeClass(statusCode));
        requestCount.add(1, attrs);
        requestDuration.record(durationSeconds, attrs);
        if (statusCode >= CLIENT_ERROR_THRESHOLD) {
            errorCount.add(1, attrs);
        }
    }

    private static String statusCodeClass(int statusCode) {
        return (statusCode / 100) + "xx";
    }

    /**
     * Returns the request count counter for testing and introspection.
     */
    LongCounter getRequestCount() {
        return requestCount;
    }

    /**
     * Returns the request duration histogram for testing and introspection.
     */
    DoubleHistogram getRequestDuration() {
        return requestDuration;
    }

    /**
     * Returns the error count counter for testing and introspection.
     */
    LongCounter getErrorCount() {
        return errorCount;
    }
}
