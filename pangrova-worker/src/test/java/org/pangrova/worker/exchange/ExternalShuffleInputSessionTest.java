package org.pangrova.worker.exchange;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual runnable/binding exit boundaries only; not a Shuffle service or Native E2E. */
class ExternalShuffleInputSessionTest {
    @Test
    void interruptedBindingJoinRemainsRetryableUntilTheActualBindingExits() {
        var session = new ExternalShuffleInputSession();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(session::close).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Interrupted while draining exchange readers");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(session.isClosed()).isTrue();
        } finally {
            Thread.interrupted();
        }
        session.bindingFinished();
        session.close();
    }

    @Test
    void cancellationDoesNotPretendAStillRunningPumpHasExited() throws Exception {
        var entered = new CountDownLatch(1);
        var leave = new CountDownLatch(1);
        var pump = new ExternalShuffleInputPump(() -> {
            entered.countDown();
            awaitRelease(leave);
        });
        var runner = new Thread(pump, "exchange-pump-exit-boundary");
        runner.start();
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            pump.cancel();
            assertThatThrownBy(() -> pump.awaitExit(System.nanoTime()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("has not exited");
            assertThat(runner.isAlive()).isTrue();
        } finally {
            leave.countDown();
            runner.join(5_000);
        }
        assertThat(runner.isAlive()).isFalse();
        pump.awaitExit(System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
    }

    @Test
    void cancellationBeforeStartPreventsFutureWorkRatherThanCancellingOnlyAFuture() throws Exception {
        var entries = new AtomicInteger();
        var pump = new ExternalShuffleInputPump(entries::incrementAndGet);
        pump.cancel();
        pump.run();
        pump.awaitExit(System.nanoTime());
        assertThat(entries.get()).isZero();
    }

    private static void awaitRelease(CountDownLatch leave) {
        boolean interrupted = false;
        while (true) {
            try {
                leave.await();
                break;
            } catch (InterruptedException failure) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}
