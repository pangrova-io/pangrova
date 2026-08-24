package org.pangrova.worker.bootstrap.phase;

import org.pangrova.operations.bootstrap.BootstrapFailedException;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link WorkerTaskBootstrap} fails fast when coordinatorUrl is null
 * and that transport SPI selection uses native implementations.
 */
class WorkerTaskBootstrapTest {

    private static WorkerObservabilitySetup createNoOpObservability() {
        WorkerObservabilitySetup setup = new WorkerObservabilitySetup();
        setup.initialize(WorkerBootstrapConfig.withDefaults("obs-init"));
        return setup;
    }

    private static final WorkerObservabilitySetup NOOP_OBSERVABILITY = createNoOpObservability();

    @Test
    void createStatusReporter_failsFastWhenCoordinatorUrlNull() {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-failfast-1");

        assertThatThrownBy(() -> WorkerTaskBootstrap.createStatusReporter(config, NOOP_OBSERVABILITY))
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("coordinatorUrl must not be null or blank");
    }

    @Test
    void createStatusReporter_failsFastWhenCoordinatorUrlBlank() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-failfast-2", 0, 0, "   ",
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null,
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                null);

        assertThatThrownBy(() -> WorkerTaskBootstrap.createStatusReporter(config, NOOP_OBSERVABILITY))
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("coordinatorUrl must not be null or blank");
    }

    @Test
    void createStatusReporter_succeedsWithValidUrl() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-valid-1", 0, 0, "http://coordinator:8080",
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null,
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                null);

        var reporter = WorkerTaskBootstrap.createStatusReporter(config, NOOP_OBSERVABILITY);
        assertThat(reporter).isNotNull();
    }

    @Test
    void createTaskExecutor_throwsWhenNativeUnavailable() {
        assertThatThrownBy(() -> WorkerTaskBootstrap.createTaskExecutor(false))
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("Native library unavailable");
    }

    @Test
    void startTaskManager_throwsWhenNativeUnavailable() {
        WorkerBootstrapConfig config = new WorkerBootstrapConfig(
                "w-nonat-1", 0, 0, "http://coordinator:8080",
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null,
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                null);
        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setNativeAvailable(false);

        assertThatThrownBy(() -> WorkerTaskBootstrap.startTaskManager(config, refs, NOOP_OBSERVABILITY))
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("Native library unavailable");
    }

    @Test
    void warnIfSingleHostUrl_detectsLocalhost() {
        assertThat(WorkerTaskBootstrap.looksLikeSingleHost("localhost")).isTrue();
    }

    @Test
    void warnIfSingleHostUrl_detectsIpLiteral() {
        assertThat(WorkerTaskBootstrap.looksLikeSingleHost("10.0.1.5")).isTrue();
    }

    @Test
    void warnIfSingleHostUrl_allowsDnsName() {
        assertThat(WorkerTaskBootstrap.looksLikeSingleHost("coordinator-lb.internal")).isFalse();
    }
}
