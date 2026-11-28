package org.pangrova.standalone;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.server.bootstrap.ServerConfigurationLoader;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.server.coordinator.embedding.EmbeddedCoordinator;
import org.pangrova.server.worker.embedding.EmbeddedWorker;
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
            PangrovaConfiguration config = ServerConfigurationLoader.load(args, CLASSPATH_DEFAULT);
            // Validate coordinator + worker config before any module starts up.
            EmbeddedCoordinator.validator().validate(config);
            EmbeddedWorker.validator().validate(config);

            EmbeddedCoordinator coordinator = EmbeddedCoordinator.start(config);
            LOG.info("event=standalone_coordinator_started coordinatorId={} port={}",
                    coordinator.coordinatorId(), coordinator.httpPort());

            EmbeddedWorker worker;
            try {
                worker = EmbeddedWorker.start(config, coordinator.httpPort());
            } catch (RuntimeException | Error failure) {
                ResourceCloser.closeAll(failure, coordinator::close);
                throw failure;
            }
            LOG.info("event=standalone_worker_started workerId={}",
                    worker.workerId());

            try {
                Runtime.getRuntime().addShutdownHook(new Thread(
                        () -> shutdownBoth(worker, coordinator), "standalone-shutdown"));
            } catch (RuntimeException | Error failure) {
                ResourceCloser.closeAll(failure, worker::close, coordinator::close);
                throw failure;
            }

            LOG.info("event=standalone_process_started mode=coordinator+worker");
        } catch (Exception e) {
            LOG.error("event=standalone_startup_failed error={}", e.getMessage(), e);
            System.exit(1);
        }
    }

    /**
     * Shuts down worker first, then coordinator (LIFO order).
     */
    static void shutdownBoth(EmbeddedWorker worker, EmbeddedCoordinator coordinator) {
        LOG.info("event=standalone_shutdown_start");
        ResourceCloser.closeAll(null, worker::closeForProcessExit, coordinator::close);
        LOG.info("event=standalone_shutdown_completed");
    }
}
