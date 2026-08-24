package org.pangrova.testing;

import org.pangrova.common.lifecycle.ResourceCloser;

import java.io.Closeable;
import java.util.Objects;

/**
 * Manages lifecycle of test coordinator and worker instances.
 */
public final class TestCluster {

    @FunctionalInterface
    public interface CoordinatorFactory {
        Closeable startCoordinator();
    }

    @FunctionalInterface
    public interface WorkerFactory {
        Closeable startWorker();
    }

    private final CoordinatorFactory coordinatorFactory;
    private final WorkerFactory workerFactory;
    private Closeable coordinator;
    private Closeable worker;
    private volatile boolean running;

    public TestCluster(CoordinatorFactory coordinatorFactory, WorkerFactory workerFactory) {
        this.coordinatorFactory = Objects.requireNonNull(coordinatorFactory);
        this.workerFactory = Objects.requireNonNull(workerFactory);
    }

    public void start() {
        this.coordinator = coordinatorFactory.startCoordinator();
        this.worker = workerFactory.startWorker();
        this.running = true;
    }

    public void stop() {
        this.running = false;
        ResourceCloser.closeQuietly(worker);
        ResourceCloser.closeQuietly(coordinator);
    }

    public boolean isRunning() {
        return running;
    }
}
