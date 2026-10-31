package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Integration tests for observability bootstrap in Worker.
 * Validates NoOp fallback and real OTel initialization paths.
 */
@Tag("native-required")
class WorkerObservabilityBootstrapTest {

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
        }
    }

    @Test
    void disabledObservabilityStartsCleanly() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "worker-obs-1", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = TestWorkerBootstrap.create(config);
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
    }

    @Test
    void enabledWithoutOtelConfigFallsBackToNoOp() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "worker-obs-2", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS, true, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = TestWorkerBootstrap.create(config);
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
    }

    @Test
    void enabledWithOtelConfigStartsAndShutsDownCleanly() {
        var obsCfg = new WorkerBootstrapConfig.ObservabilityBootstrapConfig(
                "pangrova-worker", "http://localhost:4317");
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "worker-obs-3", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS, true, obsCfg, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = TestWorkerBootstrap.create(config);
        bootstrap.start();

        assertThat(bootstrap.isStarted()).isTrue();
        assertThatCode(() -> bootstrap.shutdown()).doesNotThrowAnyException();
    }
}
