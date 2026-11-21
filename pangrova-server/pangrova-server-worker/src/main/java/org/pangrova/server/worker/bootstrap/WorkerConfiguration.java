package org.pangrova.server.worker.bootstrap;

import org.pangrova.observability.sdk.config.ObservabilityConfig;

import java.util.Map;
import org.pangrova.config.constants.AiFileOptions;
import org.pangrova.config.ConfigOption;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.constants.WorkerOptions;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

/** Maps the resolved worker settings for both standalone and independent processes. */
public final class WorkerConfiguration {
    private WorkerConfiguration() {}

    /**
     * Builds the execution settings with the coordinator address resolved by the calling role.
     */
    public static WorkerBootstrapConfig from(PangrovaConfiguration config, String coordinatorUrl) {
        String workerId = config.getString(WorkerOptions.WORKER_ID);
        int httpPort = config.getInt(WorkerOptions.WORKER_PORT);
        int transportPort = config.getInt(WorkerOptions.TRANSPORT_PORT);
        long heartbeatIntervalMs = config.getDuration(WorkerOptions.HEARTBEAT_INTERVAL).toMillis();
        boolean observabilityEnabled = config.getBoolean(WorkerOptions.OBSERVABILITY_ENABLED);
        boolean cdcEnabled = config.getBoolean(WorkerOptions.CDC_ENABLED);
        String httpHost = config.getString(WorkerOptions.WORKER_HOST);
        int resultRetentionSeconds = config.getInt(WorkerOptions.RESULT_RETENTION_SECONDS);

        var resourceConfig = loadResourceConfig(config);
        var obsConfig = ObservabilityConfig.fromDeployment(config).orElse(null);
        var registrationRetryConfig = loadRegistrationRetryConfig(config);
        var cacheConfig = loadCacheConfig(config);

        return new WorkerBootstrapConfig(
                workerId, httpPort, transportPort,
                coordinatorUrl, heartbeatIntervalMs,
                observabilityEnabled, obsConfig,
                resourceConfig, resultRetentionSeconds,
                cdcEnabled, registrationRetryConfig, httpHost,
                config.getString(WorkerOptions.INTERNAL_AUTH_SECRET),
                config.get(WorkerOptions.SHUFFLE_MODE), cacheConfig,
                config.getString(WorkerOptions.WORKER_ADVERTISED_HOST),
                config.getString(WorkerOptions.SHUFFLE_ADVERTISED_HOST),
                loadNativeFunctionConfig(config),
                loadTransportSecurityConfig(config), config.getString(WorkerOptions.ADMISSION_STATE_DIRECTORY),
                org.pangrova.config.constants.ConnectorOptions.pluginDirectory(config));
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

    private static org.pangrova.worker.function.NativeFunctionConfig loadNativeFunctionConfig(
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
        return new org.pangrova.worker.function.NativeFunctionConfig(
                true, Map.of("ai-inference", aiConfig));
    }

    private static void copyAiFileOptions(
            Map<String, String> target, PangrovaConfiguration config) {
        copyAiOption(target, config, AiFileOptions.STORAGE_BACKEND);
        copyAiOption(target, config, AiFileOptions.STORAGE_STAGING_ROOT);
        copyAiOption(target, config, AiFileOptions.LOCAL_ROOT);
        copyAiOption(target, config, AiFileOptions.CAPABILITY_SECRET_REFERENCE);
        copyAiOption(target, config, AiFileOptions.S3_BUCKET);
        copyAiOption(target, config, AiFileOptions.S3_REGION);
        copyAiOption(target, config, AiFileOptions.S3_ENDPOINT);
        copyAiOption(target, config, AiFileOptions.S3_PREFIX);
        copyAiOption(target, config, AiFileOptions.S3_SSE);
        copyAiOption(target, config, AiFileOptions.S3_KMS_KEY_ID);
        copyAiOption(target, config, AiFileOptions.S3_EXPECTED_BUCKET_OWNER);
        copyAiOption(target, config, AiFileOptions.S3_PATH_STYLE_ACCESS);
        copyAiOption(target, config, AiFileOptions.OSS_BUCKET);
        copyAiOption(target, config, AiFileOptions.OSS_REGION);
        copyAiOption(target, config, AiFileOptions.OSS_ENDPOINT);
        copyAiOption(target, config, AiFileOptions.OSS_PREFIX);
        copyAiOption(target, config, AiFileOptions.OSS_SSE);
        copyAiOption(target, config, AiFileOptions.OSS_KMS_KEY_ID);
        copyAiOption(target, config, AiFileOptions.OSS_PATH_STYLE_ACCESS);
        copyAiOption(target, config, AiFileOptions.OSS_CONNECT_TIMEOUT_MS);
        copyAiOption(target, config, AiFileOptions.OSS_READ_TIMEOUT_MS);
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
        String provider = config.getString(WorkerOptions.SHUFFLE_PROVIDER);
        String prefix = "worker.shuffle.provider." + provider + ".";
        var properties = new java.util.HashMap<String, String>();
        config.toMap().forEach((key, value) -> {
            if (key.startsWith(prefix)) properties.put(key.substring(prefix.length()), value);
        });
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
                new org.pangrova.shuffle.api.worker.ShuffleChannelLimits(
                        config.getInt(WorkerOptions.SHUFFLE_CLIENT_MAX_OWNED_CHANNELS),
                        config.getInt(WorkerOptions.SHUFFLE_CLIENT_MAX_OWNED_CHANNELS_PER_ENDPOINT)),
                new WorkerBootstrapConfig.ShuffleProvider(provider, properties));
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
