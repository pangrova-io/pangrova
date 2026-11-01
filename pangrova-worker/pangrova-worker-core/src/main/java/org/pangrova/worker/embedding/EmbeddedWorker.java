package org.pangrova.worker.embedding;

import org.pangrova.config.ConfigValidator;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.WorkerOptions;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

/**
 * Public entry point for running a Worker inside another JVM process.
 *
 * <p>Owns worker configuration translation, validation, startup and shutdown so
 * embedders such as the standalone distribution do not depend on internals of
 * {@code org.pangrova.worker.bootstrap}.</p>
 */
public final class EmbeddedWorker implements AutoCloseable {

    private static final int MAX_PORT = 65535;

    private final WorkerBootstrapConfig config;
    private final WorkerBootstrap bootstrap;

    private EmbeddedWorker(WorkerBootstrapConfig config, WorkerBootstrap bootstrap) {
        this.config = config;
        this.bootstrap = bootstrap;
    }

    /**
     * Validates worker options and starts a worker that talks to the coordinator
     * listening on {@code coordinatorHttpPort} when no explicit URL is configured.
     */
    public static EmbeddedWorker start(PangrovaConfiguration configuration, int coordinatorHttpPort) {
        validator().validate(configuration);
        return start(buildConfig(configuration, coordinatorHttpPort));
    }

    /** Starts a worker from an already-built bootstrap configuration. */
    public static EmbeddedWorker start(WorkerBootstrapConfig config) {
        WorkerBootstrap bootstrap = new WorkerBootstrap(config);
        bootstrap.start();
        return new EmbeddedWorker(config, bootstrap);
    }

    /** Validator for worker-side configuration in embedded/standalone mode. */
    public static ConfigValidator validator() {
        return new ConfigValidator()
                .require(WorkerOptions.WORKER_ID)
                .intRange(WorkerOptions.WORKER_PORT, 1, MAX_PORT)
                .intRange(WorkerOptions.TRANSPORT_PORT, 0, MAX_PORT)
                .positiveLong(WorkerOptions.MEMORY_BUDGET_BYTES);
    }

    /**
     * Builds a {@link WorkerBootstrapConfig} from a unified configuration,
     * deriving the coordinator URL from the local coordinator port when absent.
     */
    public static WorkerBootstrapConfig buildConfig(PangrovaConfiguration config, int coordinatorPort) {
        String coordinatorUrl = config.getString(WorkerOptions.COORDINATOR_URL);
        if (coordinatorUrl == null || coordinatorUrl.isBlank()) {
            coordinatorUrl = "http://localhost:" + coordinatorPort;
        }
        return new WorkerBootstrapConfig(
                config.getString(WorkerOptions.WORKER_ID),
                config.getInt(WorkerOptions.WORKER_PORT),
                config.getInt(WorkerOptions.TRANSPORT_PORT),
                coordinatorUrl,
                config.getDuration(WorkerOptions.HEARTBEAT_INTERVAL).toMillis(),
                config.getBoolean(WorkerOptions.OBSERVABILITY_ENABLED),
                buildObsConfig(config),
                buildResourceConfig(config),
                config.getInt(WorkerOptions.RESULT_RETENTION_SECONDS),
                config.getBoolean(WorkerOptions.CDC_ENABLED),
                buildRegistrationRetryConfig(config),
                config.getString(WorkerOptions.WORKER_HOST),
                config.getString(WorkerOptions.INTERNAL_AUTH_SECRET),
                null, null,
                config.getString(WorkerOptions.WORKER_ADVERTISED_HOST),
                config.getString(WorkerOptions.SHUFFLE_ADVERTISED_HOST),
                null,
                buildTransportSecurityConfig(config), config.getString(WorkerOptions.ADMISSION_STATE_DIRECTORY));
    }

    public String workerId() {
        return config.workerId();
    }

    /** Actual bound HTTP port, or {@code -1} when the server is not listening. */
    public int httpPort() {
        return bootstrap.getHttpPort();
    }

    public int transportPort() {
        return bootstrap.getTransportPort();
    }

    public boolean isStarted() {
        return bootstrap.isStarted();
    }

    @Override
    public void close() {
        bootstrap.shutdown();
    }

    private static WorkerBootstrapConfig.TransportSecurityConfig buildTransportSecurityConfig(
            PangrovaConfiguration config) {
        return new WorkerBootstrapConfig.TransportSecurityConfig(
                config.getBoolean(WorkerOptions.TRANSPORT_TLS_ENABLED),
                config.getString(WorkerOptions.TRANSPORT_TLS_CERTIFICATE_CHAIN_PATH),
                config.getString(WorkerOptions.TRANSPORT_TLS_PRIVATE_KEY_PATH),
                config.getString(WorkerOptions.TRANSPORT_TLS_ROOT_CA_PATH),
                config.getString(WorkerOptions.TRANSPORT_TLS_IDENTITY));
    }

    private static WorkerBootstrapConfig.ResourceConfig buildResourceConfig(PangrovaConfiguration config) {
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

    private static WorkerBootstrapConfig.ObservabilityBootstrapConfig buildObsConfig(
            PangrovaConfiguration config) {
        if (!config.getBoolean(WorkerOptions.OBSERVABILITY_ENABLED)) {
            return null;
        }
        String serviceName = config.getString(WorkerOptions.OBSERVABILITY_SERVICE_NAME);
        String otlpEndpoint = config.getString(WorkerOptions.OBSERVABILITY_OTLP_ENDPOINT);
        return (serviceName != null && otlpEndpoint != null)
                ? new WorkerBootstrapConfig.ObservabilityBootstrapConfig(serviceName, otlpEndpoint)
                : null;
    }

    private static WorkerBootstrapConfig.RegistrationRetryConfig buildRegistrationRetryConfig(
            PangrovaConfiguration config) {
        return new WorkerBootstrapConfig.RegistrationRetryConfig(
                config.getInt(WorkerOptions.REGISTRATION_MAX_ATTEMPTS),
                config.getDuration(WorkerOptions.REGISTRATION_INITIAL_BACKOFF).toMillis(),
                config.getDuration(WorkerOptions.REGISTRATION_MAX_BACKOFF).toMillis());
    }
}
