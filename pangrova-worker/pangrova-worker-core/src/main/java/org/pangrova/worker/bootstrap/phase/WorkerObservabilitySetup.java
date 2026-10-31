package org.pangrova.worker.bootstrap.phase;

import java.util.Map;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.observability.sdk.config.ObservabilityConfig;
import org.pangrova.observability.sdk.config.TelemetryInitializer;
import org.pangrova.observability.sdk.logging.provider.otel.OtelLoggingProvider;
import org.pangrova.observability.sdk.metrics.provider.otel.OtelMetricsProvider;
import org.pangrova.observability.api.noop.NoOpLoggingProvider;
import org.pangrova.observability.api.noop.NoOpMetricsProvider;
import org.pangrova.observability.api.noop.NoOpTracingProvider;
import org.pangrova.observability.sdk.tracing.provider.otel.OtelTracingProvider;
import org.pangrova.observability.api.LoggingProvider;
import org.pangrova.observability.api.MetricsProvider;
import org.pangrova.observability.api.TracingProvider;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates Worker observability initialization (NoOp or OTel).
 * Extracted from {@link WorkerBootstrap} to keep class size under 500 lines.
 */
public final class WorkerObservabilitySetup {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerObservabilitySetup.class);

    private TracingProvider tracingProvider;
    private MetricsProvider metricsProvider;
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
        metricsProvider = new NoOpMetricsProvider();
        loggingProvider = new NoOpLoggingProvider();
    }

    private void initOtelProviders(WorkerBootstrapConfig.ObservabilityBootstrapConfig bootCfg) {
        ObservabilityConfig obsCfg = buildObservabilityConfig(bootCfg);
        otelInitializer = new TelemetryInitializer(obsCfg);
        tracingProvider = new OtelTracingProvider(otelInitializer.getTracer());
        metricsProvider = new OtelMetricsProvider(otelInitializer.getMeter());
        loggingProvider = new OtelLoggingProvider();
    }

    private static ObservabilityConfig buildObservabilityConfig(
            WorkerBootstrapConfig.ObservabilityBootstrapConfig bootCfg) {
        PangrovaConfiguration pangrovaConfig = PangrovaConfiguration.fromYaml(Map.of(
                "pangrova.observability.service-name", bootCfg.serviceName(),
                "pangrova.observability.otlp-endpoint", bootCfg.otlpEndpoint()));
        return new ObservabilityConfig(pangrovaConfig);
    }

    public TracingProvider tracingProvider() { return tracingProvider; }
    public MetricsProvider metricsProvider() { return metricsProvider; }
    public LoggingProvider loggingProvider() { return loggingProvider; }
    public TelemetryInitializer otelInitializer() { return otelInitializer; }
}
