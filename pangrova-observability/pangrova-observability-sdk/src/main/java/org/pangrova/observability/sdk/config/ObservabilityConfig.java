package org.pangrova.observability.sdk.config;

import org.pangrova.config.PangrovaConfiguration;

/**
 * Typed configuration for the OpenTelemetry observability layer.
 * Reads required and optional keys from {@link PangrovaConfiguration}.
 *
 * <p>Sampling probability defaults are environment-aware:
 * production defaults to 0.01 (1%) to avoid overwhelming trace backends;
 * non-production environments default to 1.0 for full observability.</p>
 */
public final class ObservabilityConfig {

    /** Default sampling probability for production environments (1%). */
    public static final double DEFAULT_PRODUCTION_SAMPLING_PROBABILITY = 0.01;

    /** Default sampling probability for non-production environments (100%). */
    public static final double DEFAULT_NON_PRODUCTION_SAMPLING_PROBABILITY = 1.0;

    /** Default metric export interval in milliseconds (60 seconds). */
    private static final long DEFAULT_METRICS_INTERVAL_MS = 60_000L;

    private static final String KEY_SERVICE_NAME = "pangrova.observability.service-name";
    private static final String KEY_SERVICE_VERSION = "pangrova.observability.service-version";
    private static final String KEY_SERVICE_NAMESPACE = "pangrova.observability.service-namespace";
    private static final String KEY_SERVICE_INSTANCE_ID = "pangrova.observability.service-instance-id";
    private static final String KEY_DEPLOYMENT_ENVIRONMENT = "pangrova.observability.deployment-environment";
    private static final String KEY_OTLP_ENDPOINT = "pangrova.observability.otlp-endpoint";
    private static final String KEY_SAMPLING_STRATEGY = "pangrova.observability.sampling.strategy";
    private static final String KEY_SAMPLING_PROBABILITY = "pangrova.observability.sampling.probability";
    private static final String KEY_METRICS_INTERVAL_MS = "pangrova.observability.metrics.interval-ms";
    private static final String KEY_OTLP_HEADERS = "pangrova.observability.otlp-headers";
    private static final String KEY_OTLP_COMPRESSION = "pangrova.observability.otlp-compression";
    private static final String KEY_OTLP_TLS_CERT_PATH = "pangrova.observability.otlp-tls-cert-path";

    private final String serviceName;
    private final String serviceVersion;
    private final String serviceNamespace;
    private final String serviceInstanceId;
    private final String deploymentEnvironment;
    private final String otlpEndpoint;
    private final SamplingStrategy samplingStrategy;
    private final double samplingProbability;
    private final long metricsIntervalMs;
    private final String otlpHeaders;
    private final String otlpCompression;
    private final String otlpTlsCertPath;

    /**
     * Builds the typed observability configuration from the given Pangrova configuration.
     */
    public ObservabilityConfig(PangrovaConfiguration config) {
        this.serviceName = getRequired(config, KEY_SERVICE_NAME);
        this.serviceVersion = getOrDefault(config, KEY_SERVICE_VERSION, "unknown");
        this.serviceNamespace = getOrDefault(config, KEY_SERVICE_NAMESPACE, "pangrova");
        this.serviceInstanceId = config.toMap().get(KEY_SERVICE_INSTANCE_ID);
        this.deploymentEnvironment = getOrDefault(config, KEY_DEPLOYMENT_ENVIRONMENT, "production");
        this.otlpEndpoint = getRequired(config, KEY_OTLP_ENDPOINT);
        this.samplingStrategy = parseSamplingStrategy(
                getOrDefault(config, KEY_SAMPLING_STRATEGY, "PROBABILITY"));
        this.samplingProbability = resolveSamplingProbability(config, this.deploymentEnvironment);
        this.metricsIntervalMs = getLongOrDefault(config, KEY_METRICS_INTERVAL_MS, DEFAULT_METRICS_INTERVAL_MS);
        this.otlpHeaders = config.toMap().get(KEY_OTLP_HEADERS);
        this.otlpCompression = getOrDefault(config, KEY_OTLP_COMPRESSION, "none");
        this.otlpTlsCertPath = config.toMap().get(KEY_OTLP_TLS_CERT_PATH);
        validateRanges();
    }

    /**
     * F-CONFIG-001: validate numeric ranges so a misconfiguration fails fast at startup
     * rather than silently producing an invalid trace sampler or metric export interval.
     */
    private void validateRanges() {
        if (Double.isNaN(samplingProbability) || samplingProbability < 0.0 || samplingProbability > 1.0) {
            throw new IllegalArgumentException(
                    "Sampling probability must be within [0.0, 1.0], got " + samplingProbability
                            + " (key '" + KEY_SAMPLING_PROBABILITY + "')");
        }
        if (metricsIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "Metrics interval must be positive, got " + metricsIntervalMs
                            + "ms (key '" + KEY_METRICS_INTERVAL_MS + "')");
        }
    }

    private static double resolveSamplingProbability(PangrovaConfiguration config, String environment) {
        String explicit = config.toMap().get(KEY_SAMPLING_PROBABILITY);
        if (explicit != null) {
            return Double.parseDouble(explicit);
        }
        if ("production".equalsIgnoreCase(environment)) {
            return DEFAULT_PRODUCTION_SAMPLING_PROBABILITY;
        }
        return DEFAULT_NON_PRODUCTION_SAMPLING_PROBABILITY;
    }

    private static String getRequired(PangrovaConfiguration config, String key) {
        String value = config.toMap().get(key);
        if (value == null) {
            throw new IllegalArgumentException("Required key missing: " + key);
        }
        return value;
    }

    private static String getOrDefault(PangrovaConfiguration config, String key, String defaultValue) {
        return config.toMap().getOrDefault(key, defaultValue);
    }

    private static long getLongOrDefault(PangrovaConfiguration config, String key, long defaultValue) {
        String value = config.toMap().get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cannot parse key '" + key + "' value '" + value + "' as long");
        }
    }

    private static SamplingStrategy parseSamplingStrategy(String value) {
        return SamplingStrategy.valueOf(value.toUpperCase());
    }

    public String getServiceName() { return serviceName; }

    public String getServiceVersion() { return serviceVersion; }

    public String getServiceNamespace() { return serviceNamespace; }

    /** Returns the configured instance ID, or null if not explicitly set (auto-generated at runtime). */
    public String getServiceInstanceId() { return serviceInstanceId; }

    public String getDeploymentEnvironment() { return deploymentEnvironment; }

    public String getOtlpEndpoint() { return otlpEndpoint; }

    public SamplingStrategy getSamplingStrategy() { return samplingStrategy; }

    public double getSamplingProbability() { return samplingProbability; }

    public long getMetricsIntervalMs() { return metricsIntervalMs; }

    /** Returns OTLP headers as comma-separated "key=value" pairs, or null if not configured. */
    public String getOtlpHeaders() { return otlpHeaders; }

    /** Returns OTLP compression algorithm ("gzip" or "none"). Defaults to "none". */
    public String getOtlpCompression() { return otlpCompression; }

    /** Returns the path to a TLS certificate for OTLP, or null if not configured. */
    public String getOtlpTlsCertPath() { return otlpTlsCertPath; }

    /** Sampling strategy for trace collection. */
    public enum SamplingStrategy {
        PROBABILITY,
        RATE_LIMITING
    }
}
