package org.pangrova.coordinator.embedding;

import org.pangrova.config.ConfigValidator;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.coordinator.bootstrap.PangrovaCoordinatorMain;

/**
 * Public entry point for running a Coordinator inside another JVM process.
 *
 * <p>Owns the full coordinator lifecycle (configuration, validation, startup and
 * shutdown) so embedders such as the standalone distribution do not depend on
 * internals of {@code org.pangrova.coordinator.bootstrap}.</p>
 */
public final class EmbeddedCoordinator implements AutoCloseable {

    private final CoordinatorBootstrapConfig config;
    private final CoordinatorBootstrap bootstrap;

    private EmbeddedCoordinator(CoordinatorBootstrapConfig config, CoordinatorBootstrap bootstrap) {
        this.config = config;
        this.bootstrap = bootstrap;
    }

    /** Validates {@code configuration} and starts a coordinator. */
    public static EmbeddedCoordinator start(PangrovaConfiguration configuration) {
        validator().validate(configuration);
        return start(PangrovaCoordinatorMain.buildBootstrapConfig(configuration));
    }

    /** Starts a coordinator from an already-built bootstrap configuration. */
    public static EmbeddedCoordinator start(CoordinatorBootstrapConfig config) {
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();
        return new EmbeddedCoordinator(config, bootstrap);
    }

    /** Configuration validator for coordinator options. */
    public static ConfigValidator validator() {
        return PangrovaCoordinatorMain.coordinatorValidator();
    }

    public String coordinatorId() {
        return config.coordinatorId();
    }

    /** Actual bound HTTP port, or {@code -1} when the server is not listening. */
    public int httpPort() {
        return bootstrap.getHttpPort();
    }

    public boolean isStarted() {
        return bootstrap.isStarted();
    }

    @Override
    public void close() {
        bootstrap.shutdown();
    }
}
