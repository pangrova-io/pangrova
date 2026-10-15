package org.pangrova.worker.bootstrap;

import org.pangrova.resource.memory.MemoryArbitrator;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that shutdown resource cleanup methods properly clear
 * ThreadPoolManager threads and MemoryArbitrator allocations.
 * Also verifies WorkerBootstrap shutdown completes without error.
 */
@Tag("native-required")
class WorkerShutdownResourceTest {

    private static StubCoordinatorServer stubCoordinator;

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

    @Test
    void threadPoolShutdown_releasesAllAllocatedThreads() {
        ThreadBudgetTracker pool = new ThreadBudgetTracker(4, 2, 1);
        pool.allocateThreads(3);
        assertThat(pool.getAllocatedThreads()).isEqualTo(3);

        pool.shutdown();

        assertThat(pool.getAllocatedThreads()).isZero();
        assertThat(pool.getAvailableThreads()).isEqualTo(7);
    }

    @Test
    void memoryClearAllocations_resetsUsedMemoryToZero() {
        MemoryArbitrator arbitrator = new MemoryArbitrator(
                512 * 1024 * 1024L, 64 * 1024 * 1024L);
        arbitrator.requestMemory("q-1", 1024);
        arbitrator.requestMemory("q-2", 2048);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(3072);

        arbitrator.clearAllocations();

        assertThat(arbitrator.getUsedMemory()).isZero();
        assertThat(arbitrator.getAvailableMemory())
                .isEqualTo(512 * 1024 * 1024L - 64 * 1024 * 1024L);
    }

    @Test
    void bootstrapShutdown_completesWithoutError() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-shutdown", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null);
        WorkerBootstrap bootstrap = TestWorkerBootstrap.create(config);
        bootstrap.start();
        assertThat(bootstrap.isStarted()).isTrue();

        bootstrap.shutdown();

        assertThat(bootstrap.isStarted()).isFalse();
    }

    @Test
    void bootstrapShutdown_idempotent() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-shutdown-idem", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, WorkerBootstrapConfig.ResourceConfig.defaults(),
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null);
        WorkerBootstrap bootstrap = TestWorkerBootstrap.create(config);
        bootstrap.start();

        bootstrap.shutdown();
        bootstrap.shutdown();

        assertThat(bootstrap.isStarted()).isFalse();
    }
}
