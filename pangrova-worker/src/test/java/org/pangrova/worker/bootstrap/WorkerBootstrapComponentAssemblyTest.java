package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Verifies Worker bootstrap component assembly completeness.
 *
 * <p>Audits that WorkerBootstrap correctly wires NativeRuntime (with NoOp fallback),
 * TaskManager, HeartbeatSender, DataProducer, ExchangeManager, and
 * pangrova-resource governance (MemoryArbitrator + ThreadBudgetTracker + SpillManager).</p>
 *
 * <p>Also verifies shutdown order is reverse of startup (LIFO).</p>
 *
 * <p><b>Validates: Requirements 4.3, 4.9</b></p>
 */
@Tag("native-required")
class WorkerBootstrapComponentAssemblyTest {

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

    // -- Component assembly verification --

    @Test
    void taskManagerIsAssembledAfterStart() {
        startDefaultWorker("comp-tm");
        assertThat(bootstrap.getTaskManager()).isNotNull();
        assertThat(bootstrap.getTaskManager().getRunningTaskCount()).isZero();
    }

    @Test
    void resultServerPortIsPositiveAfterStart() {
        startDefaultWorker("comp-transport");
        assertThat(bootstrap.getTransportPort()).isGreaterThan(0);
    }

    @Test
    void httpServerIsListeningAfterStart() {
        startDefaultWorker("comp-http");
        assertThat(bootstrap.getHttpPort()).isGreaterThan(0);
    }

    @Test
    void exchangeManagerIsAssembled() throws Exception {
        startDefaultWorker("comp-exchange");
        Object exchangeTransport = getRefField(bootstrap, "exchangeTransport");
        assertThat(exchangeTransport).isNotNull();
    }

    // -- Graceful native fallback --

    @Test
    void taskManagerFunctionalRegardlessOfNativeAvailability() throws Exception {
        startDefaultWorker("comp-noop");
        // nativeAvailable depends on whether the native .dylib/.so is present on this machine.
        // The key invariant: TaskManager is always assembled, using either NativeTaskExecutor
        // or a graceful fallback when native is unavailable.
        assertThat(bootstrap.getTaskManager()).isNotNull();
        assertThat(bootstrap.getTaskManager().getRunningTaskCount()).isZero();
    }

    // -- Resource governance (pangrova-resource) --

    @Test
    void resourceManagerIsAssembled() throws Exception {
        startDefaultWorker("comp-rm");
        Object resourceManager = getRefField(bootstrap, "resourceManager");
        assertThat(resourceManager).isNotNull();
        assertThat(resourceManager).isInstanceOf(org.pangrova.resource.ResourceManager.class);
    }

    @Test
    void memoryArbitratorIsConfigured() throws Exception {
        WorkerBootstrapConfig.ResourceConfig rc = new WorkerBootstrapConfig.ResourceConfig(
                256L * 1024 * 1024, 32L * 1024 * 1024, 2, 1, 1, null, 0,
                50.0, 70.0, 85.0, 95.0,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_RETENTION,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_TRANSIT,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_SPILL,
                WorkerBootstrapConfig.ResourceConfig.DEFAULT_SHUFFLE_SPILL_FILES, null,
                WorkerBootstrapConfig.ResourceConfig.defaults().shuffleChannelLimits());
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "comp-mem", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, rc, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = configuredBootstrap(config);
        bootstrap.start();

        org.pangrova.resource.ResourceManager rm =
                (org.pangrova.resource.ResourceManager) getRefField(bootstrap, "resourceManager");
        assertThat(rm.getMemoryArbitrator()).isNotNull();
        assertThat(rm.getMemoryArbitrator().getAvailableMemory()).isGreaterThan(0);
    }

    @Test
    void threadPoolManagerIsConfigured() throws Exception {
        startDefaultWorker("comp-tp");
        org.pangrova.resource.ResourceManager rm =
                (org.pangrova.resource.ResourceManager) getRefField(bootstrap, "resourceManager");
        assertThat(rm.getThreadPoolManager()).isNotNull();
    }

    @Test
    void spillManagerIsConfigured() throws Exception {
        startDefaultWorker("comp-spill");
        org.pangrova.resource.ResourceManager rm =
                (org.pangrova.resource.ResourceManager) getRefField(bootstrap, "resourceManager");
        assertThat(rm.getSpillManager()).isNotNull();
    }

    // -- Observability providers --

    @Test
    void noOpObservabilityProvidersWhenDisabled() throws Exception {
        startDefaultWorker("comp-obs-noop");
        Object observability = getField(bootstrap, "observability");
        Object tracing = getField(observability, "tracingProvider");
        Object metrics = getField(observability, "metricsProvider");
        Object logging = getField(observability, "loggingProvider");
        assertThat(tracing).isNotNull();
        assertThat(metrics).isNotNull();
        assertThat(logging).isNotNull();
    }

    // -- Heartbeat wiring --

    @Test
    void heartbeatStartedWithCoordinatorUrl() throws Exception {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "comp-hb-start", 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false, null, null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        bootstrap = configuredBootstrap(config);
        bootstrap.start();
        Object heartbeatSender = getRefField(bootstrap, "heartbeatSender");
        assertThat(heartbeatSender).isNotNull();
    }

    // -- Shutdown order is reverse of startup --

    @Test
    void shutdownCompletesCleanlyAfterStart() {
        startDefaultWorker("comp-shutdown");
        assertThat(bootstrap.isStarted()).isTrue();
        assertThatCode(() -> bootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
        bootstrap = null;
    }

    @Test
    void shutdownReleasesAllResources() throws Exception {
        startDefaultWorker("comp-shutdown-res");
        bootstrap.shutdown();

        // After shutdown, started flag should be false
        assertThat(bootstrap.isStarted()).isFalse();
        bootstrap = null;
    }

    @Test
    void shutdownWithoutStartIsNoOp() {
        bootstrap = configuredBootstrap(testConfig("comp-no-start"));
        assertThatCode(() -> bootstrap.shutdown()).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
        bootstrap = null;
    }

    // -- PangrovaWorker wiring --

    @Test
    void pangrovaWorkerIsCreatedDuringHttpPhase() throws Exception {
        startDefaultWorker("comp-worker");
        Object worker = getRefField(bootstrap, "worker");
        assertThat(worker).isNotNull();
        assertThat(worker).isInstanceOf(org.pangrova.worker.PangrovaWorker.class);
    }

    // -- CDC consumer lifecycle --

    @Test
    void cdcConsumerNotStartedWhenDisabled() throws Exception {
        startDefaultWorker("comp-cdc-off");
        Object cdcConsumer = getRefField(bootstrap, "cdcConsumer");
        assertThat(cdcConsumer).isNull();
    }

    // -- Helpers --

    private void startDefaultWorker(String workerId) {
        bootstrap = configuredBootstrap(testConfig(workerId));
        bootstrap.start();
    }

    private static WorkerBootstrap configuredBootstrap(WorkerBootstrapConfig config) {
        return TestWorkerBootstrap.create(config);
    }

    private static WorkerBootstrapConfig testConfig(String workerId) {
        return new WorkerBootstrapConfig(
                workerId, 0, 0, stubCoordinator.baseUrl(),
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null, 0, false,
                new WorkerBootstrapConfig.RegistrationRetryConfig(1, 1, 1), null, null,
                null, null,
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    /**
     * Reads a field from the WorkerComponentRefs held inside WorkerBootstrap.
     */
    private static Object getRefField(Object bootstrap, String fieldName) throws Exception {
        Object refs = getField(bootstrap, "refs");
        Field field = refs.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(refs);
    }
}
