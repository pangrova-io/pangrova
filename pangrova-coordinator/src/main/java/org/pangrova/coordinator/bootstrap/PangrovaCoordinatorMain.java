package org.pangrova.coordinator.bootstrap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.ConfigValidator;
import org.pangrova.config.CoordinatorOptions;
import org.pangrova.config.YamlConfigLoader;
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

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaCoordinatorMain.class);
    private static final String CLASSPATH_DEFAULT = "coordinator.yml";

    private PangrovaCoordinatorMain() {}

    public static void main(String[] args) {
        try {
            PangrovaConfiguration pangrovaConfig = loadPangrovaConfiguration(args);
            coordinatorValidator().validate(pangrovaConfig);
            CoordinatorBootstrapConfig config = buildBootstrapConfig(pangrovaConfig);
            validateProductionInternalAuth(config);
            logSecurityPosture(pangrovaConfig);
            CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
            Runtime.getRuntime().addShutdownHook(new Thread(bootstrap::shutdown, "coordinator-shutdown"));
            bootstrap.start();
            LOG.info("event=coordinator_process_started coordinatorId={}", config.coordinatorId());
        } catch (Exception e) {
            LOG.error("event=coordinator_startup_failed error={}", e.getMessage(), e);
            System.exit(1);
        }
    }

    static void validateProductionInternalAuth(CoordinatorBootstrapConfig config) {
        String secret = config.internalAuthSecret();
        if (secret == null || secret.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 32) {
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
                .intRange(CoordinatorOptions.COORDINATOR_PORT, 1, 65535)
                .positiveInt(CoordinatorOptions.METADATA_CACHE_L1_MAX_SIZE)
                .positiveInt(CoordinatorOptions.QUERY_MAX_CONCURRENT);
    }

    /**
     * Parses CLI args for {@code --config <path>} and builds an {@link PangrovaConfiguration}.
     *
     * <p>Priority (highest → lowest):
     * <ol>
     *   <li>CLI YAML file (if {@code --config} provided)</li>
     *   <li>{@code PANGROVA_*} environment variables</li>
     *   <li>Classpath default {@code coordinator.yml}</li>
     *   <li>{@link org.pangrova.config.ConfigOption#defaultValue()}</li>
     * </ol>
     */
    static PangrovaConfiguration loadPangrovaConfiguration(String[] args) throws IOException {
        String configPath = parseConfigPath(args);
        Map<String, String> flatMap;
        if (configPath != null) {
            LOG.info("event=config_loading source=cli_yaml path={}", configPath);
            flatMap = YamlConfigLoader.loadWithDefaults(Path.of(configPath), CLASSPATH_DEFAULT);
        } else {
            LOG.info("event=config_loading source=classpath_default resource={}", CLASSPATH_DEFAULT);
            flatMap = YamlConfigLoader.loadFromClasspath(CLASSPATH_DEFAULT);
        }
        return PangrovaConfiguration.fromYamlWithEnvOverride(flatMap);
    }

    /**
     * Scans CLI args for {@code --config <path>}. Returns the path or {@code null} if absent.
     */
    static String parseConfigPath(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }

    /**
     * Translates an {@link PangrovaConfiguration} into the existing {@link CoordinatorBootstrapConfig}
     * record hierarchy, bridging the new typed config system with the current bootstrap flow.
     */
    public static CoordinatorBootstrapConfig buildBootstrapConfig(PangrovaConfiguration config) {
        String coordinatorId = config.getString(CoordinatorOptions.COORDINATOR_ID);
        int httpPort = config.getInt(CoordinatorOptions.COORDINATOR_PORT);
        org.pangrova.coordinator.bootstrap.CoordinatorRole role = parseRole(
                config.getString(CoordinatorOptions.COORDINATOR_ROLE));
        var core = new CoordinatorBootstrapConfig.CoreConfig(
                coordinatorId, httpPort,
                CoordinatorBootstrapConfig.CoreConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS,
                role);
        var queryConfig = new CoordinatorBootstrapConfig.QueryConfig(
                config.getDuration(CoordinatorOptions.QUERY_TIMEOUT).toSeconds(),
                CoordinatorBootstrapConfig.QueryConfig.DEFAULT_TIMEOUT_CHECK_INTERVAL_SECONDS,
                CoordinatorBootstrapConfig.QueryConfig.DEFAULT_TASK_TIMEOUT_SECONDS);
        return new CoordinatorBootstrapConfig(
                core, buildFeatureFlags(config), queryConfig,
                loadTransportConfig(config),
                CoordinatorBootstrapConfig.WorkerHealthConfig.defaults(),
                loadMetadataJdbcConfig(config), SecurityConfigurationLoader.load(config),
                loadObservabilityConfig(config));
    }

    private static CoordinatorBootstrapConfig.TransportConfig loadTransportConfig(
            PangrovaConfiguration config) {
        var defaults = CoordinatorBootstrapConfig.TransportConfig.defaults();
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
            return defaults;
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
                defaults.port(), defaults.allocatorLimitBytes(),
                defaults.advertisedHost(), tls);
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
        return new CoordinatorBootstrapConfig.FeatureFlags(
                config.getBoolean(CoordinatorOptions.OBSERVABILITY_ENABLED),
                config.getBoolean(CoordinatorOptions.SECURITY_ENABLED),
                config.getBoolean(CoordinatorOptions.METADATA_ENABLED),
                config.getBoolean(CoordinatorOptions.STREAMING_ENABLED),
                config.getBoolean(CoordinatorOptions.GOVERNANCE_ENABLED),
                config.getBoolean(CoordinatorOptions.RESILIENCE_ENABLED),
                config.getBoolean(CoordinatorOptions.TENANT_ENABLED),
                config.getBoolean(CoordinatorOptions.OPERATIONS_ENABLED));
    }

    private static CoordinatorBootstrapConfig.MetadataJdbcConfig loadMetadataJdbcConfig(
            PangrovaConfiguration config) {
        String jdbcUrl = config.getString(CoordinatorOptions.METADATA_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return null;
        }
        return new CoordinatorBootstrapConfig.MetadataJdbcConfig(jdbcUrl, "", "");
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

    private static org.pangrova.coordinator.bootstrap.CoordinatorRole parseRole(String value) {
        if (value == null || value.isBlank()) {
            return org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED;
        }
        try {
            return org.pangrova.coordinator.bootstrap.CoordinatorRole.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("event=invalid_coordinator_role value={} fallback=COMBINED", value);
            return org.pangrova.coordinator.bootstrap.CoordinatorRole.COMBINED;
        }
    }
}
