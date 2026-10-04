package org.pangrova.worker.bootstrap;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.NativeFunctionConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Bootstrap/JNI with controlled local owner exit; not SQL/provider E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerBootstrapOwnerDrainTest {
    @Test void failedCloseRetainsNativeRuntimeUntilTheActualOwnerClosesOnRetry() throws Exception {
        var bootstrap = bootstrap("native-owner-retry");
        var blocked = new AtomicBoolean(true);
        bootstrap.setCdcConsumer(() -> { if (blocked.get()) throw new IOException("owned CDC close failed"); });
        try {
            bootstrap.start();
            assertThatThrownBy(bootstrap::shutdown).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("CDC consumer");
            assertThat(bootstrap.isStarted()).isFalse();
            assertThatThrownBy(bootstrap::start).hasMessageContaining("CLEANUP_PENDING");
            assertThat(NativeCommandBuilder.requireSuccess(
                    NativeRuntime.nativeCommand(0, NativeCommandBuilder.getMetrics()), "retained Native runtime")
                    .hasPayload()).isTrue();
        } finally {
            blocked.set(false);
            bootstrap.shutdown();
        }
        bootstrap.awaitShutdown();
        assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPED");
        assertThat(bootstrap.getNodeMemoryPool().getUsedBytes()).isZero();
    }

    @Test void successfulShutdownIsOneShotAndCannotExposeAnOldCompletedLatchToNewStartup() throws Exception {
        var bootstrap = bootstrap("one-shot-worker");
        try { bootstrap.start(); }
        finally { bootstrap.shutdown(); }
        bootstrap.awaitShutdown();
        bootstrap.shutdown();
        assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPED");
        assertThatThrownBy(() -> bootstrap.setCdcConsumer(() -> {})).hasMessageContaining("STOPPED");
        assertThat(bootstrap.getNodeMemoryPool().getUsedBytes()).isZero();
    }

    @Test void actualFunctionConfigurationFailureKeepsFailedRollbackOwnersForShutdownRetry() throws Exception {
        var source = TestShuffleServiceConfig.config(WorkerBootstrapConfig.withDefaults("failed-start-owner"));
        var functions = new NativeFunctionConfig(true, java.util.Map.of("ai-inference",
                java.util.Map.of("file.storage-backend", "s3")));
        var config = new WorkerBootstrapConfig(source.workerId(), source.httpPort(), source.transportPort(),
                source.coordinatorUrl(), source.heartbeatIntervalMs(), source.observabilityEnabled(),
                source.observabilityBootstrapConfig(), source.resourceConfig(), source.resultRetentionSeconds(),
                source.cdcEnabled(), source.registrationRetryConfig(), source.httpHost(), source.internalAuthSecret(),
                source.shuffleMode(), source.cacheConfig(), source.advertisedHost(), functions, source.transportSecurityConfig());
        var bootstrap = new WorkerBootstrap(config);
        var blocked = new AtomicBoolean(true);
        bootstrap.setCdcConsumer(() -> { if (blocked.get()) throw new IOException("rollback owner still active"); });
        try {
            assertThatThrownBy(bootstrap::start).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("file.capability-secret-reference is required")
                    .satisfies(failure -> assertThat(failure.getSuppressed()).singleElement()
                            .satisfies(cleanup -> assertThat(cleanup).hasMessageContaining("CDC consumer")));
            assertThatThrownBy(bootstrap::start).hasMessageContaining("CLEANUP_PENDING");
            assertThat(NativeCommandBuilder.requireSuccess(
                    NativeRuntime.nativeCommand(0, NativeCommandBuilder.getMetrics()), "failed-start retained runtime")
                    .hasPayload()).isTrue();
        } finally { blocked.set(false); bootstrap.shutdown(); }
        bootstrap.awaitShutdown();
        assertThat(bootstrap.getNodeMemoryPool().getUsedBytes()).isZero();
        assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPED");
    }

    @Test void concurrentShutdownAndStartAreRejectedUntilTheActualCloseOwnerExits() throws Exception {
        var bootstrap = bootstrap("concurrent-drain-worker");
        var entered = new CountDownLatch(1);
        var resume = new CountDownLatch(1);
        bootstrap.setCdcConsumer(() -> {
            entered.countDown();
            if (!resume.await(5, TimeUnit.SECONDS)) throw new IOException("close owner was not released");
        });
        bootstrap.start();
        var close = CompletableFuture.runAsync(bootstrap::shutdown);
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPING");
            assertThatThrownBy(bootstrap::shutdown).hasMessageContaining("STOPPING");
            assertThat(close.isDone()).isFalse();
        } finally { resume.countDown(); }
        close.get(5, TimeUnit.SECONDS);
        bootstrap.awaitShutdown();
        assertThat(bootstrap.getNodeMemoryPool().getUsedBytes()).isZero();
    }

    private static WorkerBootstrap bootstrap(String workerId) {
        return new WorkerBootstrap(TestShuffleServiceConfig.config(WorkerBootstrapConfig.withDefaults(workerId)));
    }
}
