package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for WorkerBootstrap startup/shutdown lifecycle.
 * Requires native engine — excluded from unit test runs via {@code native-required} tag.
 */
@Tag("native-required")
class WorkerBootstrapTest {

    private static StubCoordinatorServer stubCoordinator;
    private WorkerBootstrap bootstrap;

    @BeforeAll
    static void startStub() throws Exception {
        stubCoordinator = StubCoordinatorServer.start();
    }

    @AfterAll
    static void stopStub() {
        if (stubCoordinator != null) {
            stubCoordinator.close();
        }
    }

    @AfterEach
    void tearDown() {
        if (bootstrap != null && bootstrap.isStarted()) {
            bootstrap.shutdown();
        }
    }

    @Test
    void startCompletesWithoutNativeEngine() {
        bootstrap = new WorkerBootstrap(testConfig("test-worker-1"));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        assertThat(bootstrap.getTransportPort()).isGreaterThan(0);
    }

    @Test
    void shutdownCompletesCleanly() {
        bootstrap = new WorkerBootstrap(testConfig("test-worker-2"));
        bootstrap.start();
        assertThat(bootstrap.isStarted()).isTrue();

        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void startIsIdempotent() {
        bootstrap = new WorkerBootstrap(testConfig("test-worker-3"));
        bootstrap.start();

        // Second start should throw due to checkState
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> bootstrap.start());
        assertThat(bootstrap.isStarted()).isTrue();
    }

    @Test
    void shutdownIsIdempotent() {
        bootstrap = new WorkerBootstrap(testConfig("test-worker-4"));
        bootstrap.start();

        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();

        // Second shutdown is a no-op, should not throw
        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void shutdownWithoutStartIsNoOp() {
        bootstrap = new WorkerBootstrap(testConfig("test-worker-5"));
        // Never started — shutdown should be safe
        bootstrap.shutdown();
        assertThat(bootstrap.isStarted()).isFalse();
    }

    private static WorkerBootstrapConfig testConfig(String workerId) {
        return new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1),
                null, null,
                null, null,
                null);
    }
}
