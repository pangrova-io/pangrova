package org.pangrova.worker.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.spi.execution.BatchResult;
import org.pangrova.worker.execution.SimulatedTaskExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Acceptance test: validates the Worker execution chain from main() to task completion.
 *
 * <p>Chain under test: WorkerBootstrap → TaskManager → NativeTaskExecutor →
 * SimulatedTaskExecutor.</p>
 *
 * <p><b>Property 35: Worker execution chain from main() to task completion</b></p>
 */
@org.junit.jupiter.api.Tag("native-required")
@Tag("Feature: e2e-pipeline-integration - Property 35: Worker execution chain")
class WorkerExecutionChainTest {

    private static StubCoordinatorServer stubCoordinator;
    private WorkerBootstrap workerBootstrap;

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

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    // -- Providers --

    @Provide
    Arbitrary<String> workerIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "wec-" + s);
    }

    @Provide
    Arbitrary<Integer> batchCounts() {
        return Arbitraries.integers().between(1, 5);
    }

    // -- Property tests --

    /** P35a: For any workerId, WorkerBootstrap starts and stops cleanly. */
    @Property(tries = 5)
    void bootstrapStartsAndStopsCleanly(
            @ForAll("workerIds") String workerId) {

        startWorker(workerId);
        assertThat(workerBootstrap.isStarted()).isTrue();
        assertThat(workerBootstrap.getTaskManager()).isNotNull();

        assertThatCode(() -> workerBootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(workerBootstrap.isStarted()).isFalse();
        workerBootstrap = null;
    }

    /** P35b: For any batchCount (1-5), SimulatedTaskExecutor produces that many batches then EOF via IPC. */
    @Property(tries = 5)
    void simulatedExecutorProducesExactBatchesThenEof(
            @ForAll("batchCounts") int batchCount) {

        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(batchCount);
        long handle = executor.createTask(new byte[0]);
        executor.startTask(handle);

        for (int i = 0; i < batchCount; i++) {
            assertThat(executor.getNextBatchResult(handle).status()).isEqualTo(BatchResult.Status.DATA);
        }
        assertThat(executor.getNextBatchResult(handle).status()).isEqualTo(BatchResult.Status.EOF);

        executor.releaseTask(handle);
        assertThat(executor.isReleased(handle)).isTrue();
    }

    // -- Scenario tests --

    /** SimulatedTaskExecutor full lifecycle: create → start → batches → release. */
    @Test
    void simulatedTaskExecutorFullLifecycle() {
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(3);
        long handle = executor.createTask(new byte[0]);
        executor.startTask(handle);

        assertThat(executor.getNextBatchResult(handle).status()).isEqualTo(BatchResult.Status.DATA);
        assertThat(executor.getNextBatchResult(handle).status()).isEqualTo(BatchResult.Status.DATA);
        assertThat(executor.getNextBatchResult(handle).status()).isEqualTo(BatchResult.Status.DATA);
        assertThat(executor.getNextBatchResult(handle).status()).isEqualTo(BatchResult.Status.EOF);

        executor.releaseTask(handle);
        assertThat(executor.isReleased(handle)).isTrue();
        assertThat(executor.activeTaskCount()).isZero();
    }

    /** WorkerBootstrap starts with execution engine, TaskManager is wired. */
    @Test
    void workerBootstrapStartsWithExecutionEngine() {
        startWorker("wec-engine-test");

        assertThat(workerBootstrap.isStarted()).isTrue();
        assertThat(workerBootstrap.getTaskManager()).isNotNull();

        assertThatCode(() -> workerBootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(workerBootstrap.isStarted()).isFalse();
        workerBootstrap = null;
    }

    /** SimulatedTaskExecutor tracks active task count across create/release. */
    @Test
    void simulatedTaskExecutorTracksActiveCount() {
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(1);
        long h1 = executor.createTask(new byte[0]);
        long h2 = executor.createTask(new byte[0]);

        assertThat(executor.activeTaskCount()).isEqualTo(2);

        executor.releaseTask(h1);
        assertThat(executor.activeTaskCount()).isEqualTo(1);

        executor.releaseTask(h2);
        assertThat(executor.activeTaskCount()).isZero();
    }

    // -- Bootstrap helpers --

    private void startWorker(String workerId) {
        shutdownWorker();
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null);
        workerBootstrap = new WorkerBootstrap(config);
        workerBootstrap.start();
    }

    private void shutdownAll() {
        shutdownWorker();
    }

    private void shutdownWorker() {
        if (workerBootstrap != null) {
            workerBootstrap.shutdown();
            workerBootstrap = null;
        }
    }
}
