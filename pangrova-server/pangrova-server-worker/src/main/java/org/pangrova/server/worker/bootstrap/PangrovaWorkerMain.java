package org.pangrova.server.worker.bootstrap;

import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.WorkerOptions;
import org.pangrova.server.bootstrap.ServerConfigurationLoader;
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
            PangrovaConfiguration pangrovaConfig = ServerConfigurationLoader.load(args, CLASSPATH_DEFAULT);
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

    static WorkerBootstrapConfig buildBootstrapConfig(PangrovaConfiguration config) {
        return WorkerConfiguration.from(config, config.getString(WorkerOptions.COORDINATOR_URL));
    }
}
