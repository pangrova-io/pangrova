package org.pangrova.coordinator.integration.bootstrap;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.common.lifecycle.BootstrapFailedException;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.nativebridge.jni.NativeCommandBuilder;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Bootstrap/JNI with controlled local owner exit; not SQL/provider E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerBootstrapOwnerDrainIntegrationTest {
    private WorkerBootstrapControlFixture control;

    @BeforeEach void startControl() throws Exception {
        control = new WorkerBootstrapControlFixture();
    }

    @AfterEach void closeControl() throws Exception {
        if (control != null) {
            control.close();
        }
    }
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
        try {
            bootstrap.start();
            var registered = control.registration("one-shot-worker");
            assertThat(registered).isNotNull();
            assertThat(registered.httpPort()).isEqualTo(bootstrap.getHttpPort()).isPositive();
            assertThat(registered.resultServerPort()).isEqualTo(bootstrap.getTransportPort()).isPositive();
            assertThat(registered.capabilityInventory().getCapabilitiesCount()).isPositive();
        }
        finally { bootstrap.shutdown(); }
        bootstrap.awaitShutdown();
        bootstrap.shutdown();
        assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPED");
        assertThatThrownBy(() -> bootstrap.setCdcConsumer(() -> {})).hasMessageContaining("STOPPED");
        assertThat(bootstrap.getNodeMemoryPool().getUsedBytes()).isZero();
        assertThat(control.registration("one-shot-worker")).isNull();
    }

    @Test void nativeWriterConfigurationFailureKeepsFailedRollbackOwnersForShutdownRetry() throws Exception {
        var config = control.config("failed-start-owner");
        var bootstrap = new WorkerBootstrap(config);
        var blocked = new AtomicBoolean(true);
        bootstrap.setCdcConsumer(() -> { if (blocked.get()) throw new IOException("rollback owner still active"); });
        try {
            assertThatThrownBy(bootstrap::start).isInstanceOf(BootstrapFailedException.class)
                    .hasMessageContaining("Worker catalog runtime is not configured")
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
        CompletableFuture<Void> close = null;
        try {
            bootstrap.start();
            close = CompletableFuture.runAsync(bootstrap::shutdown);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPING");
            assertThatThrownBy(bootstrap::shutdown).hasMessageContaining("STOPPING");
            assertThat(close.isDone()).isFalse();
        } finally {
            resume.countDown();
            try {
                if (close != null) {
                    close.get(5, TimeUnit.SECONDS);
                }
            } finally {
                bootstrap.shutdown();
            }
        }
        bootstrap.awaitShutdown();
        assertThat(bootstrap.getNodeMemoryPool().getUsedBytes()).isZero();
    }

    private WorkerBootstrap bootstrap(String workerId) {
        return control.bootstrap(workerId);
    }
}
