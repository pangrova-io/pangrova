package org.pangrova.worker.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.NativeMetricsSnapshot;
import org.pangrova.resource.ResourceManager;
import org.pangrova.resource.spill.SpillManager;
import org.pangrova.resource.thread.ThreadBudgetTracker;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.task.memory.MemoryRefs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual JNI control owner and executor exit barriers; no simulated business or task success. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerDependencyDrainTest {
    @Test void interruptedMonitorJoinPreservesNativeControlForAnActualCleanupRetry() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "interrupted-owner-drain"));
            var entered = new CountDownLatch(1);
            var resume = new CountDownLatch(1);
            var exited = new CountDownLatch(1);
            try (var manager = manager(runtime, monitor)) {
                try {
                    monitor.execute(() -> awaitExplicitRelease(entered, resume, exited));
                    assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                    Thread.currentThread().interrupt();
                    try {
                        assertThatThrownBy(manager::close).isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("completion monitors");
                        assertThat(Thread.currentThread().isInterrupted()).isTrue();
                    } finally { Thread.interrupted(); }
                    assertActualNativeControl(runtime);
                } finally { resume.countDown(); }
                assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
                manager.close();
                assertThat(monitor.isTerminated()).isTrue();
                assertThatThrownBy(() -> runtime.executor().executeCommand(0, NativeCommandBuilder.getMetrics()))
                        .isInstanceOf(IllegalStateException.class);
            }
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
        }
    }

    @Test void unfinishedActualMonitorKeepsControlUntilFinallyExitThenRetryClosesIt() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var monitor = Executors.newSingleThreadExecutor(task -> new Thread(task, "unfinished-owner-drain"));
            var entered = new CountDownLatch(1);
            var resume = new CountDownLatch(1);
            var exited = new CountDownLatch(1);
            try (var manager = manager(runtime, monitor)) {
                try {
                    monitor.execute(() -> awaitExplicitRelease(entered, resume, exited));
                    assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(manager::close).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("completion monitors");
                    assertThat(exited.getCount()).isEqualTo(1);
                    assertThat(monitor.isTerminated()).isFalse();
                    assertActualNativeControl(runtime);
                } finally { resume.countDown(); }
                assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
                manager.close();
                assertThat(monitor.isTerminated()).isTrue();
                assertThatThrownBy(() -> runtime.executor().executeCommand(0, NativeCommandBuilder.getMetrics()))
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    private static TaskManager manager(ActualNativeTaskFixture runtime, ExecutorService monitor) {
        var resources = new ResourceManager(runtime.arbitrator(), new ThreadBudgetTracker(8, 4, 2), new SpillManager());
        return new TaskManager(new TaskManagerContext(new RecordingTaskStatusReporter(), runtime.executor(),
                new MemoryRefs(resources, runtime.nodePool(), null), runtime.transports(), monitor),
                "actual-native-owner-drain", 0.0);
    }

    private static void assertActualNativeControl(ActualNativeTaskFixture runtime) throws Exception {
        var actual = NativeCommandBuilder.requireSuccess(
                runtime.executor().executeCommand(0, NativeCommandBuilder.getMetrics()), "retained control owner");
        assertThat(actual.hasPayload()).isTrue();
        assertThat(actual.getPayload().is(NativeMetricsSnapshot.class)).isTrue();
        assertThat(actual.getPayload().unpack(NativeMetricsSnapshot.class).getSchemaVersion()).isEqualTo(1);
    }

    private static void awaitExplicitRelease(CountDownLatch entered, CountDownLatch resume, CountDownLatch exited) {
        boolean interrupted = false;
        entered.countDown();
        try {
            while (resume.getCount() != 0) {
                try { resume.await(); } catch (InterruptedException actualInterrupt) { interrupted = true; }
            }
        } finally {
            exited.countDown();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
