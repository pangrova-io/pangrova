package org.pangrova.standalone;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.ConfigValidator;
import org.pangrova.config.WorkerOptions;
import org.pangrova.config.YamlConfigLoader;
import org.pangrova.coordinator.bootstrap.PangrovaCoordinatorMain;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-JVM entry point that starts both Coordinator and Worker for dev/test use.
 *
 * <p>Loads a unified YAML config file containing both {@code coordinator.*} and
 * {@code worker.*} sections. The worker's {@code coordinator-url} is automatically
 * derived from the coordinator's HTTP port when not explicitly set, defaulting to
 * {@code http://localhost:{coordinatorPort}}.
 *
 * <p>Shutdown order is LIFO: worker first, then coordinator — ensuring the worker
 * deregisters before the coordinator stops accepting requests.
 *
 * @see PangrovaCoordinatorMain
 * @see org.pangrova.worker.bootstrap.PangrovaWorkerMain
 */
public final class PangrovaStandaloneMain {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaStandaloneMain.class);
    private static final String CLASSPATH_DEFAULT = "coordinator.yml";

    private PangrovaStandaloneMain() {}

    public static void main(String[] args) {
        try {
            PangrovaConfiguration config = loadPangrovaConfiguration(args);
            // Validate coordinator + worker config before any module starts up.
            PangrovaCoordinatorMain.coordinatorValidator().validate(config);
            standaloneWorkerValidator().validate(config);
            CoordinatorBootstrapConfig coordConfig = PangrovaCoordinatorMain.buildBootstrapConfig(config);
            WorkerBootstrapConfig workerConfig = buildWorkerConfig(config, coordConfig.httpPort());

            CoordinatorBootstrap coordinator = new CoordinatorBootstrap(coordConfig);
            WorkerBootstrap worker = new WorkerBootstrap(workerConfig);

            coordinator.start();
            LOG.info("event=standalone_coordinator_started coordinatorId={} port={}",
                    coordConfig.coordinatorId(), coordConfig.httpPort());

            worker.start();
            LOG.info("event=standalone_worker_started workerId={} coordinatorUrl={}",
                    workerConfig.workerId(), workerConfig.coordinatorUrl());

            // Register shutdown hook AFTER both components start successfully.
            // If start() throws, there's nothing to shut down.
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> shutdownBoth(worker, coordinator),
                    "standalone-shutdown"));

            LOG.info("event=standalone_process_started mode=coordinator+worker");
        } catch (Exception e) {
            LOG.error("event=standalone_startup_failed error={}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * Parses CLI args and loads a unified {@link PangrovaConfiguration} containing
     * both coordinator and worker config sections.
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
     * Builds a {@link WorkerBootstrapConfig} from the unified config, auto-deriving
     * the coordinator URL from the coordinator port when not explicitly configured.
     */
    static WorkerBootstrapConfig buildWorkerConfig(PangrovaConfiguration config, int coordinatorPort) {
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
                config.getString(WorkerOptions.WORKER_ADVERTISED_HOST));
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
                config.getDouble(WorkerOptions.MEMORY_PRESSURE_THRESHOLD_CRITICAL));
    }

    private static WorkerBootstrapConfig.ObservabilityBootstrapConfig buildObsConfig(PangrovaConfiguration config) {
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

    /**
     * Validator for worker-side configuration in standalone mode. Enforces:
     * <ul>
     *   <li>{@code worker.id} must be present and non-blank</li>
     *   <li>{@code worker.port} must be in 1..65535</li>
     *   <li>{@code worker.transport.port} must be in 0..65535 (0 = auto-assign)</li>
     *   <li>{@code worker.memory.budget-bytes} must be positive</li>
     * </ul>
     */
    static ConfigValidator standaloneWorkerValidator() {
        return new ConfigValidator()
                .require(WorkerOptions.WORKER_ID)
                .intRange(WorkerOptions.WORKER_PORT, 1, 65535)
                .intRange(WorkerOptions.TRANSPORT_PORT, 0, 65535)
                .positiveLong(WorkerOptions.MEMORY_BUDGET_BYTES);
    }

    /**
     * Shuts down worker first, then coordinator (LIFO order).
     */
    static void shutdownBoth(WorkerBootstrap worker, CoordinatorBootstrap coordinator) {
        LOG.info("event=standalone_shutdown_start");
        try {
            worker.shutdown();
            LOG.info("event=standalone_worker_stopped");
        } catch (Exception e) {
            LOG.warn("event=standalone_worker_shutdown_error error={}", e.getMessage(), e);
        }
        try {
            coordinator.shutdown();
            LOG.info("event=standalone_coordinator_stopped");
        } catch (Exception e) {
            LOG.warn("event=standalone_coordinator_shutdown_error error={}", e.getMessage(), e);
        }
        LOG.info("event=standalone_shutdown_completed");
    }
}
