package org.pangrova.server.worker.embedding;

import org.pangrova.config.ConfigValidator;
import org.pangrova.server.worker.bootstrap.WorkerConfiguration;
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
        bootstrap.installProductionCatalogRuntime();
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
        return WorkerConfiguration.from(config, coordinatorUrl);
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
}
