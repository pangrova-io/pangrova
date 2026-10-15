package org.pangrova.testing.e2e.lifecycle;

import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.ContainerTestBase;

import java.nio.file.Path;
import java.util.UUID;

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
 * <p>Requires the latest packaged Native runtime and its real result server.
 * The {@code native-required} tag selects prerequisites, not a simulated
 * execution mode; full-regression must execute these tests.</p>
 */
@Tag("native-required")
@Tag("native-runtime-owner")
@ResourceLock("pangrova-packaged-native-runtime")
class WorkerBootstrapE2ETest extends ContainerTestBase {

    private CoordinatorBootstrap coordinator;
    private WorkerBootstrap worker;
    private BootstrapSecurityAuthority authority;
    @TempDir
    Path shuffleSpillDirectory;

    @AfterEach
    void tearDown() throws Exception {
        Exception failure = null;
        if (worker != null) {
            try { worker.shutdown(); } catch (RuntimeException cleanup) { failure = cleanup; }
            worker = null;
        }
        if (coordinator != null) {
            try { coordinator.shutdown(); } catch (RuntimeException cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            coordinator = null;
        }
        if (authority != null) {
            try { authority.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            authority = null;
        }
        if (failure != null) throw failure;
    }

    @Test
    void workerRegistersWithRealCoordinator() throws Exception {
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
    void workerShutdownDeregistersFromCoordinator() throws Exception {
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
    void workerBootstrapFullLifecycleWithRealCoordinator() throws Exception {
        coordinator = startCoordinator("coord-e2e-lc");
        String coordinatorUrl = "http://localhost:" + coordinator.getHttpPort();

        worker = startWorker("worker-e2e-lc", coordinatorUrl, shuffleSpillDirectory);
        assertThat(worker.isStarted()).isTrue();

        assertThatCode(() -> {
            worker.shutdown();
            worker = null;
        }).doesNotThrowAnyException();
    }

    @Test
    void bootstrapStartsAndStopsCleanlyForDistinctWorkerIds() throws Exception {
        // Preserve the original five property trials using actual registered Worker identities.
        for (int trial = 0; trial < 5; trial++) {
            coordinator = startCoordinator("coord-property-" + trial);
            String id = "wec-" + UUID.randomUUID();
            worker = startWorker(id, "http://localhost:" + coordinator.getHttpPort(), shuffleSpillDirectory);
            assertThat(worker.isStarted()).isTrue();
            assertThat(worker.getTaskManager()).isNotNull();
            assertThat(coordinator.getWorkerManager().getActiveWorkers()).hasSize(1);
            worker.shutdown();
            assertThat(worker.isStarted()).isFalse();
            assertThat(coordinator.getWorkerManager().getActiveWorkers()).isEmpty();
            tearDown();
        }
    }

    @Test
    void workerBootstrapStartsWithExecutionEngine() throws Exception {
        coordinator = startCoordinator("coord-engine");
        worker = startWorker("wec-engine-test", "http://localhost:" + coordinator.getHttpPort(),
                shuffleSpillDirectory);
        assertThat(worker.isStarted()).isTrue();
        assertThat(worker.getTaskManager()).isNotNull();
        assertThat(worker.getTaskManager().getSharedNativeExecutor()).isPresent();
        assertThatCode(worker::shutdown).doesNotThrowAnyException();
        assertThat(worker.isStarted()).isFalse();
    }

    private CoordinatorBootstrap startCoordinator(String id) throws Exception {
        authority = new BootstrapSecurityAuthority();
        CoordinatorBootstrapConfig config = authority.config(id);
        coordinator = new CoordinatorBootstrap(config);
        coordinator.start();
        assertThat(coordinator.isStarted()).isTrue();
        assertThat(coordinator.getHttpPort()).isGreaterThan(0);
        return coordinator;
    }

    private WorkerBootstrap startWorker(
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
                shuffleSpillDirectory.toAbsolutePath().toString(), defaults.shuffleChannelLimits());
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                workerId, 0, 0, coordinatorUrl,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, resources, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(3, 100, 1000),
                null, authority.secret(),
                null, null,
                null,
                null,
                org.pangrova.worker.jni.NativeFunctionConfig.DEFAULT,
                WorkerBootstrapConfig.TransportSecurityConfig.disabled());
        worker = new WorkerBootstrap(config);
        worker.installProductionCatalogRuntime();
        worker.start();
        return worker;
    }
}
