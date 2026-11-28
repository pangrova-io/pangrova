package org.pangrova.worker.bootstrap.phase;

import org.pangrova.observability.metrics.MetricsScope;
import org.pangrova.observability.sdk.config.ObservabilityConfig;
import org.pangrova.observability.sdk.config.TelemetryInitializer;
import org.pangrova.observability.sdk.logging.otel.OtelLoggingProvider;
import org.pangrova.observability.sdk.metrics.otel.OtelMetricsProvider;
import org.pangrova.observability.noop.NoOpLoggingProvider;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.sdk.tracing.otel.OtelTracingProvider;
import org.pangrova.observability.api.logging.LoggingProvider;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes Worker observability providers from deployment configuration.
 */
public final class WorkerTelemetryInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerTelemetryInitializer.class);

    private TracingProvider tracingProvider;
    private MetricsScope metricsProvider;
    private LoggingProvider loggingProvider;
    private TelemetryInitializer otelInitializer;

    /** Initializes observability providers from the bootstrap configuration. */
    public void initialize(WorkerBootstrapConfig config) {
        if (!config.observabilityEnabled() || config.observabilityBootstrapConfig() == null) {
            LOG.info("event=phase_started phase=observability mode=noop");
            assignNoOpProviders();
            return;
        }
        initOtelProviders(config.observabilityBootstrapConfig());
        LOG.info("event=phase_started phase=observability mode=otel");
    }

    private void assignNoOpProviders() {
        tracingProvider = new NoOpTracingProvider();
        metricsProvider = new MetricsScope(new NoOpMetricsProvider());
        loggingProvider = new NoOpLoggingProvider();
    }

    private void initOtelProviders(ObservabilityConfig config) {
        otelInitializer = new TelemetryInitializer(config);
        tracingProvider = new OtelTracingProvider(otelInitializer.getTracer());
        metricsProvider = new MetricsScope(new OtelMetricsProvider(otelInitializer.getMeter()));
        loggingProvider = new OtelLoggingProvider();
    }

    public TracingProvider tracingProvider() { return tracingProvider; }
    public MetricsProvider metricsProvider() { return metricsProvider; }
    public LoggingProvider loggingProvider() { return loggingProvider; }
    public MetricsScope metricsScope() { return metricsProvider; }
    public TelemetryInitializer otelInitializer() { return otelInitializer; }
}
