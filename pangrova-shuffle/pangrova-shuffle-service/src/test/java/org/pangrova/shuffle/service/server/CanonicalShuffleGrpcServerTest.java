package org.pangrova.shuffle.service.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.pangrova.shuffle.service.config.ShuffleServiceConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalShuffleGrpcServerTest {

    @Test
    void rejectsWorkBeyondBoundedQueue() throws Exception {
        ShuffleServiceConfig.Limits limits = new ShuffleServiceConfig.Limits(
                1, 1, 3, 1, 1, 1);
        ExecutorService executor = CanonicalShuffleGrpcServer.boundedExecutor(limits);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();
        try {
            executor.execute(() -> {
                threadName.set(Thread.currentThread().getName());
                started.countDown();
                await(release);
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            executor.execute(() -> {});

            assertThatThrownBy(() -> executor.execute(() -> {}))
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(threadName.get()).startsWith("shuffle-io-");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void reportsAdmissionOverloadWithoutDowngrade() {
        assertThat(CanonicalShuffleGrpcServer.overloaded().getStatus().getCode())
                .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
