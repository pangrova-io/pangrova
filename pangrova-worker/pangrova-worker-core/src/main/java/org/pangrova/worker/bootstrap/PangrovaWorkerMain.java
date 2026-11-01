package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.AiFileOptions;
import org.pangrova.config.ConfigOption;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.WorkerOptions;
import org.pangrova.config.YamlConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process entry point for the Pangrova Worker.
 *
 * <p>Loads configuration through the {@link PangrovaConfiguration} pipeline:
 * <ol>
 *   <li>{@code --config path/to/file.yml} CLI arg → YAML file merged with classpath defaults</li>
 *   <li>{@code PANGROVA_*} environment variable overrides</li>
 *   <li>Classpath default {@code worker.yml}</li>
 *   <li>{@link org.pangrova.config.ConfigOption#defaultValue()} fallback</li>
 * </ol>
 *
 * <p>If no {@code --config} argument is provided, defaults are used.
 */
public final class PangrovaWorkerMain {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaWorkerMain.class);
    private static final String CLASSPATH_DEFAULT = "worker.yml";

    private PangrovaWorkerMain() {}

    /** Worker process entry point. */
    public static void main(String[] args) {
        try {
            PangrovaConfiguration pangrovaConfig = loadPangrovaConfiguration(args);
            WorkerBootstrapConfig config = buildBootstrapConfig(pangrovaConfig);
            validateProductionConfig(config);
            WorkerBootstrap bootstrap = new WorkerBootstrap(config);
            bootstrap.installProductionCatalogRuntime();
            Runtime.getRuntime().addShutdownHook(new Thread(bootstrap::shutdown, "worker-shutdown"));
            bootstrap.start();
            LOG.info("event=worker_process_started workerId={}", config.workerId());
            try {
                bootstrap.awaitShutdown();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                bootstrap.shutdown();
            }
        } catch (Exception e) {
            LOG.error("event=worker_startup_failed error={}", e.getMessage(), e);
            System.exit(1);
        }
    }

    static void validateProductionConfig(WorkerBootstrapConfig config) {
        if (config.coordinatorUrl() == null || config.coordinatorUrl().isBlank()) {
            throw new IllegalStateException("Production Worker requires worker.coordinator-url");
        }
        String secret = config.internalAuthSecret();
        if (secret == null || secret.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 32) {
            throw new IllegalStateException(
                    "Production Worker requires an internal auth secret of at least 32 bytes");
        }
    }

    /**
     * Parses CLI args for {@code --config <path>} and builds an {@link PangrovaConfiguration}.
     *
     * <p>Priority (highest → lowest):
     * <ol>
     *   <li>CLI YAML file (if {@code --config} provided)</li>
     *   <li>{@code PANGROVA_*} environment variables</li>
     *   <li>Classpath default {@code worker.yml}</li>
     *   <li>{@link org.pangrova.config.ConfigOption#defaultValue()}</li>
     * </ol>
     */
    static PangrovaConfiguration loadPangrovaConfiguration(String[] args) throws IOException {
        String configPath = parseConfigPath(args);
        Map<String, String> flatMap;
        if (configPath != null) {
            LOG.info("event=config_loading source=cli_yaml path={}",
                    configPath.replace('\r', '_').replace('\n', '_'));
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
     * Translates an {@link PangrovaConfiguration} into the existing {@link WorkerBootstrapConfig}
     * record hierarchy, bridging the new typed config system with the current bootstrap flow.
     */
    static WorkerBootstrapConfig buildBootstrapConfig(PangrovaConfiguration config) {
        String workerId = config.getString(WorkerOptions.WORKER_ID);
        int httpPort = config.getInt(WorkerOptions.WORKER_PORT);
        int transportPort = config.getInt(WorkerOptions.TRANSPORT_PORT);
        String coordinatorUrl = config.getString(WorkerOptions.COORDINATOR_URL);
        long heartbeatIntervalMs = config.getDuration(WorkerOptions.HEARTBEAT_INTERVAL).toMillis();
        boolean observabilityEnabled = config.getBoolean(WorkerOptions.OBSERVABILITY_ENABLED);
        boolean cdcEnabled = config.getBoolean(WorkerOptions.CDC_ENABLED);
        String httpHost = config.getString(WorkerOptions.WORKER_HOST);
        int resultRetentionSeconds = config.getInt(WorkerOptions.RESULT_RETENTION_SECONDS);

        var resourceConfig = loadResourceConfig(config);
        var obsConfig = loadObservabilityConfig(config, observabilityEnabled);
        var registrationRetryConfig = loadRegistrationRetryConfig(config);
        var cacheConfig = loadCacheConfig(config);

        return new WorkerBootstrapConfig(
                workerId, httpPort, transportPort,
                coordinatorUrl, heartbeatIntervalMs,
                observabilityEnabled, obsConfig,
                resourceConfig, resultRetentionSeconds,
                cdcEnabled, registrationRetryConfig, httpHost,
                config.getString(WorkerOptions.INTERNAL_AUTH_SECRET),
                null, cacheConfig,
                config.getString(WorkerOptions.WORKER_ADVERTISED_HOST),
                config.getString(WorkerOptions.SHUFFLE_ADVERTISED_HOST),
                loadNativeFunctionConfig(config),
                loadTransportSecurityConfig(config), config.getString(WorkerOptions.ADMISSION_STATE_DIRECTORY));
    }

    private static WorkerBootstrapConfig.TransportSecurityConfig loadTransportSecurityConfig(
            PangrovaConfiguration config) {
        return new WorkerBootstrapConfig.TransportSecurityConfig(
                config.getBoolean(WorkerOptions.TRANSPORT_TLS_ENABLED),
                config.getString(WorkerOptions.TRANSPORT_TLS_CERTIFICATE_CHAIN_PATH),
                config.getString(WorkerOptions.TRANSPORT_TLS_PRIVATE_KEY_PATH),
                config.getString(WorkerOptions.TRANSPORT_TLS_ROOT_CA_PATH),
                config.getString(WorkerOptions.TRANSPORT_TLS_IDENTITY),
                config.getInt(WorkerOptions.COPY_TASK_TLS_PORT));
    }

    private static org.pangrova.worker.nativebridge.jni.NativeFunctionConfig loadNativeFunctionConfig(
            PangrovaConfiguration config) {
        Map<String, String> aiConfig = new java.util.LinkedHashMap<>();
        aiConfig.put("authorization.mode", config.getString(WorkerOptions.AI_AUTHORIZATION_MODE));
        aiConfig.put(
                "authorization.allowed-models",
                config.getString(WorkerOptions.AI_ALLOWED_MODELS));
        aiConfig.put(
                "authorization.allowed-capabilities",
                config.getString(WorkerOptions.AI_ALLOWED_CAPABILITIES));
        aiConfig.put(
                "authorization.granted-permissions",
                config.getString(WorkerOptions.AI_GRANTED_PERMISSIONS));
        aiConfig.put(
                "authorization.max-batch-size",
                Integer.toString(config.getInt(WorkerOptions.AI_MAX_BATCH_SIZE)));
        aiConfig.put(
                "authorization.max-input-bytes",
                Long.toString(config.getLong(WorkerOptions.AI_MAX_INPUT_BYTES)));
        copyAiFileOptions(aiConfig, config);
        config.toMap().forEach((key, value) -> {
            String prefix = "worker.ai.models.";
            if (key.startsWith(prefix)) {
                aiConfig.put("models." + key.substring(prefix.length()), value);
            }
        });
        return new org.pangrova.worker.nativebridge.jni.NativeFunctionConfig(
                true, Map.of("ai-inference", aiConfig));
    }

    private static void copyAiFileOptions(
            Map<String, String> target, PangrovaConfiguration config) {
        copyAiOption(target, config, AiFileOptions.STORAGE_BACKEND);
        copyAiOption(target, config, AiFileOptions.STORAGE_STAGING_ROOT);
        copyAiOption(target, config, AiFileOptions.CAPABILITY_SECRET_REFERENCE);
        copyAiOption(target, config, AiFileOptions.S3_BUCKET);
        copyAiOption(target, config, AiFileOptions.S3_REGION);
        copyAiOption(target, config, AiFileOptions.S3_ENDPOINT);
        copyAiOption(target, config, AiFileOptions.S3_PREFIX);
        copyAiOption(target, config, AiFileOptions.S3_SSE);
        copyAiOption(target, config, AiFileOptions.S3_KMS_KEY_ID);
        copyAiOption(target, config, AiFileOptions.S3_EXPECTED_BUCKET_OWNER);
        copyAiOption(target, config, AiFileOptions.S3_PATH_STYLE_ACCESS);
        copyAiOption(target, config, AiFileOptions.MAX_BYTES);
        copyAiOption(target, config, AiFileOptions.ALLOWED_MIME_TYPES);
        copyAiOption(target, config, AiFileOptions.URL_ALLOWED_HOSTS);
        copyAiOption(target, config, AiFileOptions.URL_CONNECT_TIMEOUT_MS);
        copyAiOption(target, config, AiFileOptions.URL_READ_TIMEOUT_MS);
        copyAiOption(target, config, AiFileOptions.URL_MAX_WIRE_BYTES);
        copyAiOption(target, config, AiFileOptions.URL_MAX_DECODED_BYTES);
        copyAiOption(target, config, AiFileOptions.URL_MAX_CONNECTIONS);
        copyAiOption(target, config, AiFileOptions.URL_MAX_CONNECTIONS_PER_ROUTE);
        copyAiOption(target, config, AiFileOptions.URL_CONNECTION_TTL_MS);
        copyAiOption(target, config, AiFileOptions.URL_IDLE_EVICTION_MS);
    }

    private static <T> void copyAiOption(
            Map<String, String> target,
            PangrovaConfiguration config,
            ConfigOption<T> option) {
        config.getOptional(option).ifPresent(value -> target.put(
                option.key().substring("worker.ai.".length()), value.toString()));
    }

    private static WorkerBootstrapConfig.ResourceConfig loadResourceConfig(PangrovaConfiguration config) {
        return new WorkerBootstrapConfig.ResourceConfig(
                config.getLong(WorkerOptions.MEMORY_BUDGET_BYTES),
                config.getLong(WorkerOptions.MEMORY_SYSTEM_RESERVED_BYTES),
                config.getInt(WorkerOptions.THREADS_QUERY),
                config.getInt(WorkerOptions.THREADS_IO),
                config.getInt(WorkerOptions.THREADS_SCHEDULER),
                config.getString(WorkerOptions.SPILL_DIRECTORY),
                config.getLong(WorkerOptions.MEMORY_NODE_BYTES),
                config.getDouble(WorkerOptions.MEMORY_PRESSURE_THRESHOLD_LOW),
                config.getDouble(WorkerOptions.MEMORY_PRESSURE_THRESHOLD_MEDIUM),
                config.getDouble(WorkerOptions.MEMORY_PRESSURE_THRESHOLD_HIGH),
                config.getDouble(WorkerOptions.MEMORY_PRESSURE_THRESHOLD_CRITICAL),
                config.getLong(WorkerOptions.SHUFFLE_SERVICE_RETENTION_BYTES),
                config.getLong(WorkerOptions.SHUFFLE_SERVICE_TRANSIT_BYTES),
                config.getLong(WorkerOptions.SHUFFLE_SERVICE_SPILL_BYTES),
                config.getInt(WorkerOptions.SHUFFLE_SERVICE_SPILL_FILES),
                config.getString(WorkerOptions.SHUFFLE_SERVICE_SPILL_DIRECTORY),
                new org.pangrova.worker.exchange.remote.RemoteShuffleChannelAdmission.Limits(
                        config.getInt(WorkerOptions.SHUFFLE_CLIENT_MAX_OWNED_CHANNELS),
                        config.getInt(WorkerOptions.SHUFFLE_CLIENT_MAX_OWNED_CHANNELS_PER_ENDPOINT)));
    }

    private static WorkerBootstrapConfig.ObservabilityBootstrapConfig loadObservabilityConfig(
            PangrovaConfiguration config, boolean enabled) {
        if (!enabled) {
            return null;
        }
        String serviceName = config.getString(WorkerOptions.OBSERVABILITY_SERVICE_NAME);
        String otlpEndpoint = config.getString(WorkerOptions.OBSERVABILITY_OTLP_ENDPOINT);
        if (serviceName == null || otlpEndpoint == null) {
            return null;
        }
        return new WorkerBootstrapConfig.ObservabilityBootstrapConfig(serviceName, otlpEndpoint);
    }

    private static WorkerBootstrapConfig.RegistrationRetryConfig loadRegistrationRetryConfig(
            PangrovaConfiguration config) {
        return new WorkerBootstrapConfig.RegistrationRetryConfig(
                config.getInt(WorkerOptions.REGISTRATION_MAX_ATTEMPTS),
                config.getDuration(WorkerOptions.REGISTRATION_INITIAL_BACKOFF).toMillis(),
                config.getDuration(WorkerOptions.REGISTRATION_MAX_BACKOFF).toMillis());
    }

    private static WorkerBootstrapConfig.CacheConfig loadCacheConfig(PangrovaConfiguration config) {
        boolean enabled = config.getBoolean(WorkerOptions.CACHE_ENABLED);
        if (!enabled) {
            return WorkerBootstrapConfig.CacheConfig.defaults();
        }
        return new WorkerBootstrapConfig.CacheConfig(
                true,
                config.getLong(WorkerOptions.CACHE_MEMORY_BYTES),
                config.getString(WorkerOptions.CACHE_SSD_PATH),
                config.getLong(WorkerOptions.CACHE_SSD_BYTES),
                config.getInt(WorkerOptions.CACHE_SSD_SHARDS),
                config.getLong(WorkerOptions.CACHE_CHECKPOINT_INTERVAL_BYTES),
                config.getString(WorkerOptions.CACHE_ARTIFACT_PATH),
                config.getLong(WorkerOptions.CACHE_ARTIFACT_BYTES),
                config.getLong(WorkerOptions.CACHE_ARTIFACT_INDEX_MEMORY_BYTES));
    }
}
