package org.pangrova.worker.heartbeat;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.net.http.HttpClient;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual owned HTTP executor cleanup; does not send or fabricate heartbeat business traffic. */
class HeartbeatSenderOwnershipTest {
    @Test void interruptedOwnedHttpJoinCanRetryAfterRunningWasAlreadyCleared() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var httpExecutor = Executors.newSingleThreadExecutor(work -> new Thread(work, "owned-heartbeat-http"));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        var httpOwner = new org.pangrova.transport.http.client.ManagedHttpClient(
                HttpClient.newBuilder().executor(httpExecutor).build(), httpExecutor);
        var sender = new HeartbeatSender(new HeartbeatConfig("http://localhost:1", "owned-heartbeat", 3_600_000),
                () -> { throw new AssertionError("No heartbeat is scheduled inside this ownership boundary"); },
                scheduler, httpOwner, null);
        try {
            httpExecutor.execute(() -> holdActualOwner(entered, release, exited));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            sender.start();
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(sender::stop).isInstanceOf(IllegalStateException.class)
                        .hasCauseInstanceOf(InterruptedException.class);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally { Thread.interrupted(); }
            assertThat(sender.isRunning()).isFalse();
            assertThat(exited.getCount()).isEqualTo(1);
            assertThat(httpExecutor.isTerminated()).isFalse();
            release.countDown();
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            sender.stop();
            sender.stop();
            assertThat(httpExecutor.isTerminated()).isTrue();
            assertThat(scheduler.isShutdown()).isFalse();
        } finally {
            release.countDown();
            sender.stop();
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void holdActualOwner(CountDownLatch entered, CountDownLatch release, CountDownLatch exited) {
        boolean interrupted = false;
        entered.countDown();
        try {
            while (release.getCount() != 0) {
                try { release.await(); } catch (InterruptedException failure) { interrupted = true; }
            }
        } finally {
            exited.countDown();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
