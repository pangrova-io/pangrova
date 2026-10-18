package org.pangrova.coordinator.bootstrap.lifecycle;

import java.nio.channels.Pipe;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Pure process-control failures; no task execution, ACK, or database durability is simulated. */
class CoordinatorLifecycleAdmissionTest {
    @Test
    void processWaitRetainsOwnershipAcrossPreStartCloseAndFailedCleanup() throws Exception {
        var lifecycle = new CoordinatorLifecycleAdmission();
        var pipe = Pipe.open();
        var returned = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                lifecycle.awaitShutdown();
                returned.countDown();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }, "coordinator-process-wait-test");
        waiter.start();
        try {
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> waiter.getState() == Thread.State.WAITING);
            lifecycle.close(() -> { throw new AssertionError("Pre-start close must not claim cleanup"); });
            lifecycle.beginStart();
            lifecycle.endStart(true);
            var failure = new IllegalStateException("real owner still open");
            assertThatThrownBy(() -> lifecycle.close(() -> { throw failure; })).isSameAs(failure);
            assertThat(returned.getCount()).isEqualTo(1);
            assertThat(pipe.source().isOpen()).isTrue();
            lifecycle.close(() -> {
                try { pipe.source().close(); pipe.sink().close(); }
                catch (java.io.IOException cleanup) { throw new IllegalStateException(cleanup); }
            });
            assertThat(returned.await(5, TimeUnit.SECONDS)).isTrue();
            waiter.join(5000);
            assertThat(waiter.isAlive()).isFalse();
            assertThat(pipe.source().isOpen()).isFalse();
            assertThat(pipe.sink().isOpen()).isFalse();
            lifecycle.awaitShutdown();
        } finally {
            waiter.interrupt();
            waiter.join(5000);
            pipe.source().close();
            pipe.sink().close();
        }
    }

    @Test
    void processWaitIsInterruptibleWithoutClaimingShutdown() throws Exception {
        var lifecycle = new CoordinatorLifecycleAdmission();
        lifecycle.beginStart();
        lifecycle.endStart(true);
        var interrupted = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try { lifecycle.awaitShutdown(); }
            catch (InterruptedException expected) { interrupted.countDown(); }
        }, "coordinator-process-interrupt-test");
        waiter.start();
        try {
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> waiter.getState() == Thread.State.WAITING);
            waiter.interrupt();
            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
            waiter.join(5000);
            assertThat(waiter.isAlive()).isFalse();
            assertThat(lifecycle.isRunning()).isTrue();
            lifecycle.close(() -> { });
            lifecycle.awaitShutdown();
        } finally {
            waiter.interrupt();
            waiter.join(5000);
            lifecycle.close(() -> { });
        }
    }

    @Test
    void failedDependencyRetainsCloseObligationAndSuccessfulExitIsNotRepeated() throws Exception {
        var lifecycle = new CoordinatorLifecycleAdmission();
        var pipe = Pipe.open();
        var attempts = new AtomicInteger();
        var exits = new AtomicInteger();
        lifecycle.beginStart();
        lifecycle.endStart(true);
        Runnable cleanup = () -> {
            lifecycle.step("sink", () -> {
                try { pipe.sink().close(); }
                catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
                exits.incrementAndGet();
            });
            lifecycle.step("source", () -> {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("control close boundary failure");
                try { pipe.source().close(); }
                catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
            });
        };
        try {
            assertThatThrownBy(() -> lifecycle.close(cleanup)).isInstanceOf(IllegalStateException.class);
            assertThat(lifecycle.isRunning()).isFalse();
            assertThat(pipe.source().isOpen()).isTrue();
            assertThat(pipe.sink().isOpen()).isFalse();
            lifecycle.close(cleanup);
            lifecycle.close(cleanup);
            assertThat(pipe.source().isOpen()).isFalse();
            assertThat(exits).hasValue(1);
            assertThat(attempts).hasValue(2);
            assertThatThrownBy(lifecycle::beginStart).isInstanceOf(IllegalStateException.class);
        } finally { pipe.sink().close(); pipe.source().close(); }
    }

    @Test
    void dependencyCannotJoinItsOwnUnfinishedStep() {
        var lifecycle = new CoordinatorLifecycleAdmission();
        assertThatThrownBy(() -> lifecycle.step("owner", () -> lifecycle.step("owner", () -> { })))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("join itself");
        lifecycle.step("owner", () -> { });
    }

    @Test
    void invalidCallbacksAreRejectedWithoutChangingAdmission() {
        var lifecycle = new CoordinatorLifecycleAdmission();
        assertThatThrownBy(() -> lifecycle.close(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> lifecycle.step("owner", null)).isInstanceOf(NullPointerException.class);
        lifecycle.beginStart();
        lifecycle.endStart(true);
        assertThat(lifecycle.isRunning()).isTrue();
        lifecycle.close(() -> { });
    }
}
