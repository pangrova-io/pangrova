package org.pangrova.worker.bootstrap.phase;

import java.util.Arrays;

import org.pangrova.common.lifecycle.BootstrapFailedException;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
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
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());

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
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());

        var reporter = WorkerTaskBootstrap.createStatusReporter(config, NOOP_OBSERVABILITY);
        assertThat(reporter).isNotNull();
    }

    @Test
    void createTaskExecutor_throwsWhenNativeUnavailable() {
        assertThatThrownBy(() -> WorkerTaskBootstrap.createTaskExecutor(
                false, WorkerBootstrapConfig.withDefaults("worker-test"),
                new org.pangrova.worker.bootstrap.ownership.WorkerTaskOwnership(
                        WorkerBootstrapConfig.ResourceConfig.defaults().shuffleChannelLimits())))
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
                null, null, null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setNativeAvailable(false);

        assertThatThrownBy(() -> WorkerTaskBootstrap.startTaskManager(config, refs, NOOP_OBSERVABILITY))
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("Native library unavailable");
        assertThat(refs.taskOwnership()).isNotNull();
        assertThat(refs.taskOwnership().isDrained()).isFalse();
        assertThatThrownBy(() -> WorkerTaskBootstrap.startTaskManager(config, refs, NOOP_OBSERVABILITY))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Previous task bootstrap owners");
        refs.taskOwnership().close();
        assertThat(refs.taskOwnership().isDrained()).isTrue();
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

    @Test
    void shuffleAdvertisedHostUsesPeerRouteWithoutChangingResultRoute() {
        var config = new WorkerBootstrapConfig(
                "worker-1", 0, 0, null,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null,
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                "host.testcontainers.internal", "worker-1",
                null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());

        assertThat(WorkerHttpBootstrap.resolveAdvertisedHost(config))
                .isEqualTo("host.testcontainers.internal");
        assertThat(WorkerTaskBootstrap.resolveShuffleAdvertisedHost(config,
                WorkerHttpBootstrap.resolveAdvertisedHost(config)))
                .isEqualTo("worker-1");
    }

    @Test
    void shuffleAdvertisedHostDefaultsToResolvedWorkerRoute() {
        var config = WorkerBootstrapConfig.withDefaults("worker-1");
        assertThat(WorkerTaskBootstrap.resolveShuffleAdvertisedHost(config, "worker.internal"))
                .isEqualTo("worker.internal");
    }

    @Test
    void shuffleAdvertisedHostRejectsWildcardAddress() {
        var config = new WorkerBootstrapConfig(
                "worker-1", 0, 0, null,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null,
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, null, null,
                null, null,
                "host.testcontainers.internal", "0.0.0.0",
                null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());

        assertThatThrownBy(() -> WorkerTaskBootstrap.resolveShuffleAdvertisedHost(
                config, "host.testcontainers.internal"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("worker.shuffle.advertised-host");
    }

    @Test
    void resolveShuffleHmacKeys_generatesIndependentProcessLocalKeys() {
        var first = WorkerTaskBootstrap.resolveShuffleHmacKeys(null, null);
        var second = WorkerTaskBootstrap.resolveShuffleHmacKeys(null, null);

        assertThat(first.current()).hasSize(32);
        assertThat(first.previous()).isEmpty();
        assertThat(Arrays.equals(first.current(), second.current())).isFalse();
    }

    @Test
    void resolveShuffleHmacKeys_preservesManagedRotationWindow() {
        byte[] current = new byte[48];
        byte[] previous = new byte[32];
        Arrays.fill(current, (byte) 7);
        Arrays.fill(previous, (byte) 9);

        var keys = WorkerTaskBootstrap.resolveShuffleHmacKeys(current, previous);
        current[0] = 1;
        previous[0] = 1;

        assertThat(keys.current()).containsOnly((byte) 7);
        assertThat(keys.previous()).containsOnly((byte) 9);
    }

    @Test
    void resolveShuffleHmacKeys_rejectsWeakCurrentKey() {
        assertThatThrownBy(() -> WorkerTaskBootstrap.resolveShuffleHmacKeys(
                new byte[31], null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 32 bytes");
    }

    @Test
    void resolveShuffleHmacKeys_rejectsWeakPreviousKey() {
        assertThatThrownBy(() -> WorkerTaskBootstrap.resolveShuffleHmacKeys(
                new byte[32], new byte[31]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("previous shuffle HMAC key");
    }
}
