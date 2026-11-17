package org.pangrova.observability.sdk.config;

import org.pangrova.observability.sdk.enums.SamplingStrategy;
import org.pangrova.config.constants.CommonOptions;
import org.pangrova.config.PangrovaConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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

    private static final String KEY_SERVICE_VERSION = "observability.service-version";
    private static final String KEY_SERVICE_NAMESPACE = "observability.service-namespace";
    private static final String KEY_SERVICE_INSTANCE_ID = "observability.service-instance-id";
    private static final String KEY_DEPLOYMENT_ENVIRONMENT = "observability.deployment-environment";
    private static final String KEY_SAMPLING_STRATEGY = "observability.sampling.strategy";
    private static final String KEY_TRACES_PER_SECOND = "observability.sampling.traces-per-second";
    private static final String KEY_SAMPLING_PROBABILITY = "observability.sampling.probability";
    private static final String KEY_METRICS_INTERVAL_MS = "observability.metrics.interval-ms";
    private static final String KEY_OTLP_HEADERS = "observability.otlp-headers";
    private static final String KEY_OTLP_COMPRESSION = "observability.otlp-compression";
    private static final String KEY_OTLP_TLS_CERT_PATH = "observability.otlp-tls-cert-path";

    private final String serviceName;
    private final String serviceVersion;
    private final String serviceNamespace;
    private final String serviceInstanceId;
    private final String deploymentEnvironment;
    private final String otlpEndpoint;
    private final SamplingStrategy samplingStrategy;
    private final double samplingProbability;
    private final double tracesPerSecond;
    private final long metricsIntervalMs;
    private final Map<String, String> otlpHeaders;
    private final String otlpCompression;
    private final String otlpTlsCertPath;

    /** Parses exporter settings before either process starts allocating telemetry resources. */
    public static Optional<ObservabilityConfig> fromDeployment(PangrovaConfiguration config) {
        if (!config.getBoolean(CommonOptions.OBSERVABILITY_ENABLED)) {
            return Optional.empty();
        }
        boolean exporterConfigured = config.toMap().keySet().stream()
                .anyMatch(key -> key.startsWith("observability.") && !key.equals(
                        CommonOptions.OBSERVABILITY_ENABLED.key()));
        return exporterConfigured ? Optional.of(new ObservabilityConfig(config)) : Optional.empty();
    }

    /**
     * Builds the typed observability configuration from the given Pangrova configuration.
     */
    public ObservabilityConfig(PangrovaConfiguration config) {
        this.serviceName = getRequired(config, CommonOptions.OBSERVABILITY_SERVICE_NAME.key());
        this.serviceVersion = getOrDefault(config, KEY_SERVICE_VERSION, "unknown");
        this.serviceNamespace = getOrDefault(config, KEY_SERVICE_NAMESPACE, "pangrova");
        this.serviceInstanceId = config.toMap().get(KEY_SERVICE_INSTANCE_ID);
        this.deploymentEnvironment = getOrDefault(config, KEY_DEPLOYMENT_ENVIRONMENT, "production");
        this.otlpEndpoint = getRequired(config, CommonOptions.OBSERVABILITY_OTLP_ENDPOINT.key());
        this.samplingStrategy = parseSamplingStrategy(
                getOrDefault(config, KEY_SAMPLING_STRATEGY, "PROBABILITY"));
        this.samplingProbability = resolveSamplingProbability(config, this.deploymentEnvironment);
        this.tracesPerSecond = Double.parseDouble(getOrDefault(config, KEY_TRACES_PER_SECOND, "1"));
        this.metricsIntervalMs = getLongOrDefault(config, KEY_METRICS_INTERVAL_MS, DEFAULT_METRICS_INTERVAL_MS);
        this.otlpHeaders = parseHeaders(config.toMap().get(KEY_OTLP_HEADERS));
        this.otlpCompression = getOrDefault(config, KEY_OTLP_COMPRESSION, "none");
        this.otlpTlsCertPath = config.toMap().get(KEY_OTLP_TLS_CERT_PATH);
        validateRanges();
    }

    /**
     * F-CONFIG-001: validate numeric ranges so a misconfiguration fails fast at startup
     * rather than silently producing an invalid trace sampler or metric export interval.
     */
    private void validateRanges() {
        if (!Double.isFinite(tracesPerSecond) || tracesPerSecond <= 0) {
            throw new IllegalArgumentException(KEY_TRACES_PER_SECOND + " must be finite and positive");
        }
        if (!"gzip".equalsIgnoreCase(otlpCompression) && !"none".equalsIgnoreCase(otlpCompression)) {
            throw new IllegalArgumentException(KEY_OTLP_COMPRESSION + " must be gzip or none");
        }
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
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Required key missing or blank: " + key);
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

    private static Map<String, String> parseHeaders(String raw) {
        if (raw == null || raw.isEmpty()) return Map.of();
        var headers = new LinkedHashMap<String, String>();
        for (String entry : raw.split(",", -1)) {
            int separator = entry.indexOf('=');
            if (separator < 1) throw new IllegalArgumentException(KEY_OTLP_HEADERS + " requires name=value pairs");
            String name = entry.substring(0, separator).trim().toLowerCase(java.util.Locale.ROOT);
            if (name.isEmpty() || headers.putIfAbsent(name, entry.substring(separator + 1).trim()) != null) {
                throw new IllegalArgumentException(KEY_OTLP_HEADERS + " requires distinct nonempty names");
            }
        }
        return Map.copyOf(headers);
    }

    private static SamplingStrategy parseSamplingStrategy(String value) {
        return SamplingStrategy.valueOf(value.toUpperCase(java.util.Locale.ROOT));
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

    public double getTracesPerSecond() { return tracesPerSecond; }

    public long getMetricsIntervalMs() { return metricsIntervalMs; }

    /** Returns configured exporter headers, keyed by case-insensitive HTTP name. */
    public Map<String, String> getOtlpHeaders() { return otlpHeaders; }

    /** Returns OTLP compression algorithm ("gzip" or "none"). Defaults to "none". */
    public String getOtlpCompression() { return otlpCompression; }

    /** Returns the path to a TLS certificate for OTLP, or null if not configured. */
    public String getOtlpTlsCertPath() { return otlpTlsCertPath; }

}
