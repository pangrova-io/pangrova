package org.pangrova.worker.property.bootstrap;

import org.pangrova.worker.bootstrap.StubCoordinatorServer;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Property tests for Worker graceful shutdown (Property 24).
 *
 * <p>Validates that WorkerBootstrap starts and shuts down cleanly
 * for any valid workerId, and that shutdown without prior start is safe.</p>
 *
 * <p><b>Validates: Requirements 48.2</b></p>
 */
@org.junit.jupiter.api.Tag("native-required")
class WorkerShutdownPropertyTest {

    private static StubCoordinatorServer stubCoordinator;

    @BeforeAll
    @BeforeContainer
    static void startStub() throws Exception {
        stubCoordinator = StubCoordinatorServer.start();
    }

    @AfterAll
    @AfterContainer
    static void stopStub() {
        if (stubCoordinator != null) {
            stubCoordinator.close();
        }
    }

    @Provide
    Arbitrary<String> workerIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(12)
                .map(s -> "worker-" + s);
    }

    /**
     * P24b: Starting and then shutting down WorkerBootstrap leaves
     * isStarted() == false with no exceptions.
     *
     * <p><b>Validates: Requirements 48.2</b></p>
     */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration, Property 24: Graceful shutdown completeness")
    void workerShutdownTerminatesCleanly(
            @ForAll("workerIds") String workerId) {

        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        WorkerBootstrap bootstrap = org.pangrova.worker.bootstrap.TestWorkerBootstrap.create(config);

        try {
            bootstrap.start();
            assertThat(bootstrap.isStarted()).isTrue();
        } finally {
            assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        }
        assertThat(bootstrap.isStarted()).isFalse();
    }

    /**
     * P24c (Worker): Calling shutdown() without start() is a no-op —
     * no exceptions, isStarted() remains false.
     *
     * <p><b>Validates: Requirements 48.2</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 24: Graceful shutdown completeness")
    void workerShutdownWithoutStartIsNoOp(
            @ForAll("workerIds") String workerId) {

        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        WorkerBootstrap bootstrap = new WorkerBootstrap(config);

        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }
}
