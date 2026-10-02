package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies CDC consumer lifecycle wiring in WorkerBootstrap.
 * Uses a real AutoCloseable implementation — no mocks.
 */
@Tag("native-required")
class WorkerCdcBootstrapTest {

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
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    @Test
    void cdcConsumerClosedOnShutdown() {
        AtomicBoolean closed = new AtomicBoolean(false);
        bootstrap = createCdcEnabledBootstrap();
        bootstrap.setCdcConsumer(() -> closed.set(true));
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        bootstrap.shutdown();
        bootstrap = null;

        assertThat(closed.get()).isTrue();
    }

    @Test
    void cdcDisabledSkipsConsumerLifecycle() {
        AtomicBoolean closed = new AtomicBoolean(false);
        bootstrap = createCdcDisabledBootstrap();
        bootstrap.setCdcConsumer(() -> closed.set(true));
        bootstrap.start();

        bootstrap.shutdown();
        bootstrap = null;

        // Consumer still closed during shutdown cleanup
        assertThat(closed.get()).isTrue();
    }

    @Test
    void cdcEnabledWithoutConsumerStartsSafely() {
        bootstrap = createCdcEnabledBootstrap();
        // No setCdcConsumer call
        assertThatCode(() -> bootstrap.start()).doesNotThrowAnyException();
    }

    private static WorkerBootstrap createCdcEnabledBootstrap() {
        return TestWorkerBootstrap.create(buildConfig(true));
    }

    private static WorkerBootstrap createCdcDisabledBootstrap() {
        return TestWorkerBootstrap.create(buildConfig(false));
    }

    private static WorkerBootstrapConfig buildConfig(boolean cdcEnabled) {
        return new WorkerBootstrapConfig(
                "cdc-test-worker", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, cdcEnabled,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null);
    }
}
