package org.pangrova.server.coordinator.bootstrap;

import java.nio.file.Path;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.ConfigValidator;
import org.pangrova.config.CoordinatorOptions;
import org.pangrova.scheduler.api.resource.SpillBudget;
import org.pangrova.server.bootstrap.ServerConfigurationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process entry point for the Pangrova Coordinator.
 *
 * <p>Loads configuration through the {@link PangrovaConfiguration} pipeline:
 * <ol>
 *   <li>{@code --config path/to/file.yml} CLI arg → YAML file merged with classpath defaults</li>
 *   <li>{@code PANGROVA_*} environment variable overrides</li>
 *   <li>Classpath default {@code coordinator.yml}</li>
 *   <li>{@link org.pangrova.config.ConfigOption#defaultValue()} fallback</li>
 * </ol>
 *
 * <p>If no {@code --config} argument is provided, defaults are used. Per R1.3, all feature
 * flags default to {@code true} except {@code security.enabled} which defaults to {@code false}.
 */
public final class PangrovaCoordinatorMain {
    private static final int MAX_TCP_PORT = 65535;

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaCoordinatorMain.class);
    private static final String CLASSPATH_DEFAULT = "coordinator.yml";

    private PangrovaCoordinatorMain() {}

    /** Starts the coordinator from its command-line configuration. */
    public static void main(String[] args) {
        try {
            PangrovaConfiguration pangrovaConfig = ServerConfigurationLoader.load(args, CLASSPATH_DEFAULT);
            coordinatorValidator().validate(pangrovaConfig);
            CoordinatorBootstrapConfig config = buildBootstrapConfig(pangrovaConfig);
            validateProductionInternalAuth(config);
            logSecurityPosture(pangrovaConfig);
            CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
            Runtime.getRuntime().addShutdownHook(new Thread(bootstrap::shutdown, "coordinator-shutdown"));
            bootstrap.start();
            LOG.info("event=coordinator_process_started coordinatorId={}", config.coordinatorId());
            try {
                bootstrap.awaitShutdown();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                bootstrap.shutdown();
            }
        } catch (Exception e) {
            LOG.error("event=coordinator_startup_failed error={}", e.getMessage(), e);
            System.exit(1);
        }
    }

    static void validateProductionInternalAuth(CoordinatorBootstrapConfig config) {
        String secret = config.internalAuthSecret();
        if (secret == null || secret.isBlank()
                || secret.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 32) {
            throw new IllegalStateException(
                    "Production Coordinator requires an internal auth secret of at least 32 bytes");
        }
    }

    /**
     * Validator for required coordinator configuration. Enforces:
     * <ul>
     *   <li>{@code coordinator.id} must be present and non-blank</li>
     *   <li>{@code coordinator.port} must be in 1..65535</li>
     *   <li>{@code metadata.cache.l1-max-size} must be positive when set</li>
     * </ul>
     */
    public static ConfigValidator coordinatorValidator() {
        return new ConfigValidator()
                .require(CoordinatorOptions.COORDINATOR_ID)
                .intRange(CoordinatorOptions.COORDINATOR_PORT, 1, MAX_TCP_PORT)
                .positiveInt(CoordinatorOptions.METADATA_CACHE_L1_MAX_SIZE)
                .positiveInt(CoordinatorOptions.QUERY_MAX_CONCURRENT);
    }

    /**
     * Translates an {@link PangrovaConfiguration} into the existing {@link CoordinatorBootstrapConfig}
     * record hierarchy, bridging the new typed config system with the current bootstrap flow.
     */
    public static CoordinatorBootstrapConfig buildBootstrapConfig(PangrovaConfiguration config) {
        String coordinatorId = config.getString(CoordinatorOptions.COORDINATOR_ID);
        int httpPort = config.getInt(CoordinatorOptions.COORDINATOR_PORT);
        org.pangrova.server.coordinator.enums.CoordinatorRole role = parseRole(
                config.getString(CoordinatorOptions.COORDINATOR_ROLE));
        var core = new CoordinatorBootstrapConfig.CoreConfig(
                coordinatorId, httpPort,
                CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                role, config.getString(CoordinatorOptions.COORDINATOR_HOST));
        var queryConfig = new CoordinatorBootstrapConfig.QueryConfig(
                config.getDuration(CoordinatorOptions.QUERY_TIMEOUT).toSeconds(),
                CoordinatorBootstrapConfig.QueryConfig.DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS,
                CoordinatorBootstrapConfig.QueryConfig.DEFAULT_TASK_TIMEOUT_SECONDS,
                CoordinatorBootstrapConfig.QueryConfig.DEFAULT_QUERY_RETENTION_SECONDS,
                CoordinatorBootstrapConfig.QueryConfig.DEFAULT_QUERY_CLEANUP_INTERVAL_SECONDS,
                new SpillBudget(
                        config.getLong(CoordinatorOptions.TASK_SPILL_MAX_BYTES)),
                config.getInt(CoordinatorOptions.WORKER_MAX_RETRY),
                config.getString(CoordinatorOptions.PLACEMENT_PROVIDER));
        var bootstrap = new CoordinatorBootstrapConfig(
                core, buildFeatureFlags(config), queryConfig,
                loadTransportConfig(config),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                loadMetadataJdbcConfig(config), SecurityConfigurationLoader.load(config),
                loadObservabilityConfig(config));
        try {
            CoordinatorShuffleProviders.validate(bootstrap);
        } catch (IllegalArgumentException invalid) {
            throw new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE, invalid.getMessage(), invalid);
        }
        return bootstrap;
    }

    private static CoordinatorBootstrapConfig.TransportConfig loadTransportConfig(
            PangrovaConfiguration config) {
        var transport = new CoordinatorBootstrapConfig.TransportConfig(
                config.getInt(CoordinatorOptions.TRANSPORT_PORT),
                config.getLong(CoordinatorOptions.TRANSPORT_ALLOCATOR_LIMIT_BYTES),
                config.getString(CoordinatorOptions.TRANSPORT_ADVERTISED_HOST), null);
        boolean enabled = config.getBoolean(CoordinatorOptions.TRANSPORT_TLS_ENABLED);
        String certificate = config.getString(
                CoordinatorOptions.TRANSPORT_TLS_CERTIFICATE_CHAIN_PATH);
        String privateKey = config.getString(
                CoordinatorOptions.TRANSPORT_TLS_PRIVATE_KEY_PATH);
        String rootCa = config.getString(CoordinatorOptions.TRANSPORT_TLS_ROOT_CA_PATH);
        String serverIdentity = config.getString(
                CoordinatorOptions.SHUFFLE_TLS_SERVER_IDENTITY);
        boolean materialConfigured = !certificate.isBlank()
                || !privateKey.isBlank() || !rootCa.isBlank()
                || !serverIdentity.isBlank();
        if (!enabled) {
            if (materialConfigured) {
                throw new PangrovaException(
                        PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "coordinator transport TLS material requires tls.enabled=true");
            }
            return transport;
        }
        if (certificate.isBlank() || privateKey.isBlank()
                || rootCa.isBlank() || serverIdentity.isBlank()) {
            throw new PangrovaException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "enabled coordinator transport TLS requires certificate, private key, "
                            + "root CA and shuffle server identity");
        }
        var tls = new org.pangrova.coordinator.transport.ShuffleTlsConfig(
                Path.of(certificate), Path.of(privateKey), Path.of(rootCa),
                serverIdentity);
        return new CoordinatorBootstrapConfig.TransportConfig(
                transport.port(), transport.allocatorLimitBytes(),
                transport.advertisedHost(), tls);
    }

    private static CoordinatorBootstrapConfig.FeatureFlags buildFeatureFlags(PangrovaConfiguration config) {
        if (config.getBoolean(CoordinatorOptions.TENANT_ENABLED)
                && !config.getBoolean(CoordinatorOptions.SECURITY_ENABLED)) {
            throw new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "tenant.enabled=true requires security.enabled=true and authenticated tenant membership");
        }
        if (config.getBoolean(CoordinatorOptions.TENANT_ENABLED)
                && "NOOP".equalsIgnoreCase(
                        config.getString(CoordinatorOptions.SECURITY_AUTH_TYPE))) {
            throw new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                    "tenant.enabled=true cannot use the NOOP authenticator");
        }
        var shuffleMode = config.get(CoordinatorOptions.SHUFFLE_MODE);
        var shuffleEndpoints = parseShuffleEndpoints(config.getString(CoordinatorOptions.SHUFFLE_ENDPOINTS));
        String provider = config.getString(CoordinatorOptions.SHUFFLE_PROVIDER);
        String prefix = "coordinator.shuffle.provider." + provider + ".";
        var settings = new java.util.HashMap<String, String>();
        config.toMap().forEach((key, value) -> {
            if (key.startsWith(prefix)) settings.put(key.substring(prefix.length()), value);
        });
        return new CoordinatorBootstrapConfig.FeatureFlags(
                config.getBoolean(CoordinatorOptions.OBSERVABILITY_ENABLED),
                config.getBoolean(CoordinatorOptions.SECURITY_ENABLED),
                config.getBoolean(CoordinatorOptions.METADATA_ENABLED),
                config.getBoolean(CoordinatorOptions.STREAMING_ENABLED),
                config.getBoolean(CoordinatorOptions.GOVERNANCE_ENABLED),
                config.getBoolean(CoordinatorOptions.RESILIENCE_ENABLED),
                config.getBoolean(CoordinatorOptions.TENANT_ENABLED),
                config.getBoolean(CoordinatorOptions.OPERATIONS_ENABLED),
                config.getBoolean(CoordinatorOptions.SPECULATION_ENABLED),
                false, false, false, false, false,
                shuffleMode, shuffleEndpoints, provider, settings);
    }

    private static java.util.List<String> parseShuffleEndpoints(String csv) {
        if (csv == null || csv.isBlank()) {
            return java.util.List.of();
        }
        return java.util.Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(entry -> !entry.isEmpty())
                .toList();
    }

    private static CoordinatorBootstrapConfig.MetadataJdbcConfig loadMetadataJdbcConfig(
            PangrovaConfiguration config) {
        String jdbcUrl = config.getString(CoordinatorOptions.METADATA_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return null;
        }
        return new CoordinatorBootstrapConfig.MetadataJdbcConfig(jdbcUrl,
                config.getString(CoordinatorOptions.METADATA_JDBC_USERNAME),
                config.getString(CoordinatorOptions.METADATA_JDBC_PASSWORD));
    }

    private static CoordinatorBootstrapConfig.ObservabilityBootstrapConfig loadObservabilityConfig(
            PangrovaConfiguration config) {
        if (!config.getBoolean(CoordinatorOptions.OBSERVABILITY_ENABLED)) {
            return null;
        }
        return null;
    }

    /**
     * Logs the effective security posture at startup so operators can immediately
     * see whether authentication, RLS, column masking, and tracing are enforced.
     * Emitted at WARN when security is disabled (high visibility to catch an
     * accidentally insecure production deployment), INFO otherwise.
     */
    private static void logSecurityPosture(PangrovaConfiguration config) {
        boolean securityEnabled = config.getBoolean(CoordinatorOptions.SECURITY_ENABLED);
        String authType = securityEnabled
                ? config.getString(CoordinatorOptions.SECURITY_AUTH_TYPE) : null;
        boolean tracingEnabled = config.getBoolean(CoordinatorOptions.OBSERVABILITY_ENABLED);
        for (String line : SecurityPostureSummary.format(securityEnabled, authType, tracingEnabled)) {
            if (securityEnabled) {
                LOG.info("{}", line);
            } else {
                LOG.warn("{}", line);
            }
        }
    }

    private static org.pangrova.server.coordinator.enums.CoordinatorRole parseRole(String value) {
        if (value == null || value.isBlank()) {
            return org.pangrova.server.coordinator.enums.CoordinatorRole.COMBINED;
        }
        try {
            return org.pangrova.server.coordinator.enums.CoordinatorRole.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("event=invalid_coordinator_role value={} fallback=COMBINED", value);
            return org.pangrova.server.coordinator.enums.CoordinatorRole.COMBINED;
        }
    }
}
