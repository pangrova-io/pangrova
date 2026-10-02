package org.pangrova.testing.e2e.lifecycle;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * E2E test: Worker bootstrap against a real Coordinator.
 *
 * <p>Starts a real CoordinatorBootstrap on an ephemeral port, then starts
 * WorkerBootstrap pointing at that coordinator URL. Validates the full
 * registration chain: Worker HTTP POST /v1/workers/register → Coordinator
 * WorkerResource → WorkerManager.registerWorker.</p>
 *
 * <p>Requires native FlightResultServer command handler (START_RESULT_SERVER)
 * to be implemented in C++. Tagged {@code native-required} to exclude from
 * default test runs until the native command is available.</p>
 */
@Tag("native-required")
class WorkerBootstrapE2ETest {

    private CoordinatorBootstrap coordinator;
    private WorkerBootstrap worker;
    @TempDir
    Path shuffleSpillDirectory;

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.shutdown();
        }
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }

    @Test
    void workerRegistersWithRealCoordinator() {
        coordinator = startCoordinator("coord-e2e-reg");
        String coordinatorUrl = "http://localhost:" + coordinator.getHttpPort();

        worker = startWorker("worker-e2e-reg", coordinatorUrl, shuffleSpillDirectory);

        assertThat(worker.isStarted()).isTrue();
        assertThat(worker.getTaskManager()).isNotNull();
        assertThat(worker.getHttpPort()).isGreaterThan(0);

        // Coordinator should have the worker registered
        assertThat(coordinator.getWorkerManager()).isNotNull();
        assertThat(coordinator.getWorkerManager().getActiveWorkers()).hasSize(1);
    }

    @Test
    void workerShutdownDeregistersFromCoordinator() {
        coordinator = startCoordinator("coord-e2e-dereg");
        String coordinatorUrl = "http://localhost:" + coordinator.getHttpPort();

        worker = startWorker("worker-e2e-dereg", coordinatorUrl, shuffleSpillDirectory);
        assertThat(coordinator.getWorkerManager().getActiveWorkers()).hasSize(1);

        worker.shutdown();
        worker = null;

        // After deregistration, worker count should drop
        assertThat(coordinator.getWorkerManager().getActiveWorkers()).isEmpty();
    }

    @Test
    void workerBootstrapFullLifecycleWithRealCoordinator() {
        coordinator = startCoordinator("coord-e2e-lc");
        String coordinatorUrl = "http://localhost:" + coordinator.getHttpPort();

        worker = startWorker("worker-e2e-lc", coordinatorUrl, shuffleSpillDirectory);
        assertThat(worker.isStarted()).isTrue();

        assertThatCode(() -> {
            worker.shutdown();
            worker = null;
        }).doesNotThrowAnyException();
    }

    private static CoordinatorBootstrap startCoordinator(String id) {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(id, 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);
        bootstrap.start();
        assertThat(bootstrap.isStarted()).isTrue();
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
        return bootstrap;
    }

    private static WorkerBootstrap startWorker(
            String workerId, String coordinatorUrl, Path shuffleSpillDirectory) {
        var defaults = WorkerBootstrapConfig.ResourceConfig.defaults();
        var resources = new WorkerBootstrapConfig.ResourceConfig(
                defaults.memoryBudgetBytes(), defaults.systemReservedBytes(),
                defaults.queryThreads(), defaults.ioThreads(), defaults.schedulerThreads(),
                defaults.spillDirectory(), defaults.nodeMemoryBytes(),
                defaults.pressureThresholdLow(), defaults.pressureThresholdMedium(),
                defaults.pressureThresholdHigh(), defaults.pressureThresholdCritical(),
                defaults.shuffleRetentionBytes(), defaults.shuffleTransitBytes(),
                defaults.shuffleSpillBytes(), defaults.shuffleSpillFiles(),
                shuffleSpillDirectory.toAbsolutePath().toString());
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                workerId, 0, 0, coordinatorUrl,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, resources, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(3, 100, 1000),
                null, null,
                null, null,
                null,
                org.pangrova.worker.jni.NativeFunctionConfig.DEFAULT,
                WorkerBootstrapConfig.TransportSecurityConfig.disabled());
        WorkerBootstrap bootstrap = new WorkerBootstrap(config);
        bootstrap.start();
        return bootstrap;
    }
}
