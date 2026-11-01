package org.pangrova.observability.sdk.metrics;

import org.pangrova.spi.observability.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.observability.sdk.metrics.jvm.JvmMetricsCollector;

/**
 * Registers standard infrastructure metrics collectors at application startup.
 *
 * <p>Creates and holds references to:
 * <ul>
 *   <li>{@link JvmMetricsCollector} — heap, GC, thread metrics via MXBeans</li>
 *   <li>{@link HttpMetricsCollector} — request count, latency, error rate</li>
 * </ul>
 *
 * <p>Intended to be instantiated once during application bootstrap. The JVM collector
 * registers pull-based gauges that read MXBeans at collection time. The HTTP collector
 * exposes a push-based API for recording individual requests from an interceptor.
 */
public final class StandardMetricsRegistration {

    private static final Logger LOG = LoggerFactory.getLogger(StandardMetricsRegistration.class);

    private final JvmMetricsCollector jvmMetrics;
    private final HttpMetricsCollector httpMetrics;

    public StandardMetricsRegistration(MetricsProvider provider) {
        this.jvmMetrics = new JvmMetricsCollector(provider);
        this.httpMetrics = new HttpMetricsCollector(provider);
        LOG.info("Standard metrics collectors registered: JVM (heap/GC/threads), HTTP (requests/latency/errors)");
    }

    /**
     * Returns the JVM metrics collector for direct access to gauge values.
     */
    public JvmMetricsCollector getJvmMetrics() {
        return jvmMetrics;
    }

    /**
     * Returns the HTTP metrics collector for recording request completions.
     */
    public HttpMetricsCollector getHttpMetrics() {
        return httpMetrics;
    }
}
