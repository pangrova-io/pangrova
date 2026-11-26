package org.pangrova.worker.bootstrap;

import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.execution.WorkerExecutedTaskBackend;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual pre-start local owners; does not initialize or simulate Native/business execution. */
class WorkerBootstrapPrestartOwnershipTest {
    @Test void shutdownBeforeStartClosesAnInstalledActualPipeAndSealsTheInstance() throws Exception {
        var bootstrap = new WorkerBootstrap(WorkerBootstrapConfig.withDefaults("prestart-pipe-owner"), event -> {});
        var pipe = Pipe.open();
        try (var owned = pipe.source(); var sink = pipe.sink()) {
            bootstrap.setCdcConsumer(owned);
            bootstrap.shutdown();
            assertThat(owned.isOpen()).isFalse();
            assertThat(bootstrap.isStarted()).isFalse();
            assertShutdownCompleted(bootstrap);
            bootstrap.shutdown();
            assertThatThrownBy(bootstrap::start).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("STOPPED");
        }
    }

    @Test void failedPrestartOwnerRemainsOpenUntilActualCloseSucceedsOnRetry() throws Exception {
        var bootstrap = new WorkerBootstrap(WorkerBootstrapConfig.withDefaults("prestart-retry-owner"), event -> {});
        var pipe = Pipe.open();
        var blocked = new AtomicBoolean(true);
        var attempts = new AtomicInteger();
        try (var owned = pipe.source(); var sink = pipe.sink()) {
            bootstrap.setCdcConsumer(() -> {
                attempts.incrementAndGet();
                if (blocked.get()) throw new IOException("prestart pipe owner has not drained");
                owned.close();
            });
            try {
                assertThatThrownBy(bootstrap::shutdown).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("CDC consumer");
                assertThat(owned.isOpen()).isTrue();
                assertThat(attempts).hasValue(1);
                assertThatThrownBy(bootstrap::start).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("CLEANUP_PENDING");
                assertThatThrownBy(() -> bootstrap.setCdcConsumer(sink)).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("CLEANUP_PENDING");
            } finally { blocked.set(false); bootstrap.shutdown(); }
            assertThat(attempts).hasValue(2);
            assertThat(owned.isOpen()).isFalse();
            assertShutdownCompleted(bootstrap);
            assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPED");
        }
    }

    @Test void concurrentPrestartShutdownSealsAdmissionWhileTheActualOwnerIsStillClosing() throws Exception {
        var bootstrap = new WorkerBootstrap(WorkerBootstrapConfig.withDefaults("prestart-active-owner"), event -> {});
        var pipe = Pipe.open();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var owned = pipe.source(); var sink = pipe.sink()) {
            bootstrap.setCdcConsumer(() -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("prestart close was not released");
                owned.close();
            });
            var close = CompletableFuture.runAsync(bootstrap::shutdown);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(owned.isOpen()).isTrue();
                assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPING");
                assertThatThrownBy(bootstrap::shutdown).hasMessageContaining("STOPPING");
            } finally { release.countDown(); }
            close.get(5, TimeUnit.SECONDS);
            assertThat(owned.isOpen()).isFalse();
            assertShutdownCompleted(bootstrap);
        }
    }

    @Test
    void shutdownBeforeStartClosesThePendingWorkerBackendExactlyOnce() throws Exception {
        var bootstrap = new WorkerBootstrap(WorkerBootstrapConfig.withDefaults("pending-backend"), event -> {});
        var closes = new AtomicInteger();
        bootstrap.setWorkerExecutedTaskBackend(new CloseOnlyBackend(closes::incrementAndGet));

        bootstrap.shutdown();
        assertShutdownCompleted(bootstrap);
        bootstrap.shutdown();

        assertThat(closes).hasValue(1);
        assertThatThrownBy(bootstrap::start).hasMessageContaining("STOPPED");
    }

    @Test
    void failedPendingBackendCloseRetainsOwnershipForShutdownRetry() throws Exception {
        var bootstrap = new WorkerBootstrap(WorkerBootstrapConfig.withDefaults("pending-backend-retry"), event -> {});
        var failing = new AtomicBoolean(true);
        var closes = new AtomicInteger();
        bootstrap.setWorkerExecutedTaskBackend(new CloseOnlyBackend(() -> {
            closes.incrementAndGet();
            if (failing.get()) {
                throw new IllegalStateException("backend has not drained");
            }
        }));

        assertThatThrownBy(bootstrap::shutdown).hasMessageContaining("backend has not drained");
        assertThatThrownBy(bootstrap::start).hasMessageContaining("CLEANUP_PENDING");
        failing.set(false);
        bootstrap.shutdown();

        assertShutdownCompleted(bootstrap);
        assertThat(closes).hasValue(2);
    }

    record CloseOnlyBackend(Runnable onClose) implements WorkerExecutedTaskBackend {
        @Override
        public boolean supports(TaskKind kind) {
            return kind == TaskKind.TASK_KIND_CDC_INGEST;
        }

        @Override
        public long create(TaskRequest request) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public void start(long handle) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public boolean isFinished(long handle) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public boolean awaitFinished(long handle, int waitMillis) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public byte[] getTaskStatus(long handle) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public void cancel(long handle) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public List<byte[]> getCommitFragments(long handle) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public Optional<byte[]> release(long handle) {
            throw new AssertionError("No task was submitted");
        }

        @Override
        public void close() {
            onClose.run();
        }
    }

    private static void assertShutdownCompleted(WorkerBootstrap bootstrap) throws Exception {
        CompletableFuture.runAsync(() -> {
            try { bootstrap.awaitShutdown(); }
            catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
        }).get(5, TimeUnit.SECONDS);
    }
}
