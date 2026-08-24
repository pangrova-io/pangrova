package org.pangrova.coordinator.bootstrap;

import java.util.Map;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.observability.config.ObservabilityConfig;
import org.pangrova.observability.config.TelemetryInitializer;
import org.pangrova.observability.logging.OtelLoggingProvider;
import org.pangrova.observability.metrics.OtelMetricsProvider;
import org.pangrova.observability.noop.NoOpLoggingProvider;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.observability.tracing.OtelTracingProvider;
import org.pangrova.spi.observability.LoggingProvider;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstraps the observability subsystem (tracing, metrics, logging).
 * Falls back to NoOp providers when OTel is not configured.
 */
final class ObservabilityBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(ObservabilityBootstrap.class);

    private final CoordinatorBootstrapConfig config;

    private TracingProvider tracingProvider;
    private MetricsProvider metricsProvider;
    private LoggingProvider loggingProvider;
    private TelemetryInitializer otelInitializer;

    ObservabilityBootstrap(CoordinatorBootstrapConfig config) {
        this.config = config;
    }

    void start() {
        if (!config.observabilityEnabled()) {
            LOG.info("Phase skipped: observability not configured");
            assignNoOpProviders();
            return;
        }
        if (config.observabilityBootstrapConfig() == null) {
            LOG.warn("Phase: observability — enabled but no OTel config provided. "
                    + "Falling back to NoOp providers. Traces and metrics will NOT be collected. "
                    + "Set 'pangrova.observability.otlp-endpoint' to enable telemetry.");
            assignNoOpProviders();
            return;
        }
        initOtelProviders();
        LOG.info("Phase: observability — initialized (OTel)");
    }

    void shutdown() {
        if (otelInitializer != null) {
            otelInitializer.close();
        }
        LOG.info("Phase: observability — closed");
    }

    private void assignNoOpProviders() {
        tracingProvider = new NoOpTracingProvider();
        metricsProvider = new NoOpMetricsProvider();
        loggingProvider = new NoOpLoggingProvider();
    }

    private void initOtelProviders() {
        ObservabilityConfig obsCfg = buildObservabilityConfig(config.observabilityBootstrapConfig());
        otelInitializer = new TelemetryInitializer(obsCfg);
        tracingProvider = new OtelTracingProvider(otelInitializer.getTracer());
        metricsProvider = new OtelMetricsProvider(otelInitializer.getMeter());
        loggingProvider = new OtelLoggingProvider();
    }

    private static ObservabilityConfig buildObservabilityConfig(
            CoordinatorBootstrapConfig.ObservabilityBootstrapConfig bootCfg) {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", bootCfg.serviceName(),
                "pangrova.observability.otlp-endpoint", bootCfg.otlpEndpoint()));
        return new ObservabilityConfig(pangrovaConfig);
    }

    TracingProvider tracingProvider() { return tracingProvider; }
    MetricsProvider metricsProvider() { return metricsProvider; }
    LoggingProvider loggingProvider() { return loggingProvider; }
}
