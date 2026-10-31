package org.pangrova.standalone;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.YamlConfigLoader;
import org.pangrova.coordinator.embedding.EmbeddedCoordinator;
import org.pangrova.worker.embedding.EmbeddedWorker;
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
 * @see EmbeddedCoordinator
 * @see EmbeddedWorker
 */
public final class PangrovaStandaloneMain {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaStandaloneMain.class);
    private static final String CLASSPATH_DEFAULT = "coordinator.yml";

    private PangrovaStandaloneMain() {}

    /** Starts a Coordinator and Worker from the standalone YAML configuration. */
    public static void main(String[] args) {
        try {
            PangrovaConfiguration config = loadPangrovaConfiguration(args);
            // Validate coordinator + worker config before any module starts up.
            EmbeddedCoordinator.validator().validate(config);
            EmbeddedWorker.validator().validate(config);

            EmbeddedCoordinator coordinator = EmbeddedCoordinator.start(config);
            LOG.info("event=standalone_coordinator_started coordinatorId={} port={}",
                    coordinator.coordinatorId(), coordinator.httpPort());

            EmbeddedWorker worker;
            try {
                worker = EmbeddedWorker.start(config, coordinator.httpPort());
            } catch (RuntimeException e) {
                coordinator.close();
                throw e;
            }
            LOG.info("event=standalone_worker_started workerId={}",
                    worker.workerId());

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
     * Shuts down worker first, then coordinator (LIFO order).
     */
    static void shutdownBoth(EmbeddedWorker worker, EmbeddedCoordinator coordinator) {
        LOG.info("event=standalone_shutdown_start");
        try {
            worker.close();
            LOG.info("event=standalone_worker_stopped");
        } catch (Exception e) {
            LOG.warn("event=standalone_worker_shutdown_error error={}", e.getMessage(), e);
        }
        try {
            coordinator.close();
            LOG.info("event=standalone_coordinator_stopped");
        } catch (Exception e) {
            LOG.warn("event=standalone_coordinator_shutdown_error error={}", e.getMessage(), e);
        }
        LOG.info("event=standalone_shutdown_completed");
    }
}
