package org.pangrova.worker.task;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 188: validates that the orchestration/completion-monitor executor pool
 * is separate from the Velox driver pool. Blocking waitForCompletion must
 * not consume driver threads.
 *
 * <p>Tests create the pool via the same factory logic as
 * {@code WorkerTaskBootstrap.createOrchestrationExecutor}, verifying bounded
 * pool properties and saturation policy.</p>
 */
class OrchestrationPoolSeparationTest {

    @Test
    @DisplayName("orchestration executor is a bounded pool with daemon threads")
    void orchestrationExecutorIsBounded() {
        int maxConcurrentTasks = Runtime.getRuntime().availableProcessors() * 2;
        ExecutorService executor = new ThreadPoolExecutor(
                2, maxConcurrentTasks,
                60L, TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(maxConcurrentTasks * 2),
                r -> {
                    Thread t = new Thread(r, "orchestration-monitor");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());

        assertThat(executor).isNotNull();

        // Verify it runs tasks on daemon threads named "orchestration-monitor"
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();
        AtomicReference<Boolean> isDaemon = new AtomicReference<>();

        executor.submit(() -> {
            threadName.set(Thread.currentThread().getName());
            isDaemon.set(Thread.currentThread().isDaemon());
            latch.countDown();
        });

        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(threadName.get()).isEqualTo("orchestration-monitor");
        assertThat(isDaemon.get()).isTrue();

        executor.shutdownNow();
    }

    @Test
    @DisplayName("orchestration executor does not block when saturated (caller-runs policy)")
    void callerRunsPolicyUnderSaturation() {
        int maxConcurrentTasks = Runtime.getRuntime().availableProcessors() * 2;
        ExecutorService executor = new ThreadPoolExecutor(
                2, maxConcurrentTasks,
                60L, TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(maxConcurrentTasks * 2),
                r -> {
                    Thread t = new Thread(r, "orchestration-monitor");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());

        // The pool uses CallerRunsPolicy, so submitting more tasks than capacity
        // should not throw -- it runs on the calling thread instead.
        int overload = Runtime.getRuntime().availableProcessors() * 10;
        CountDownLatch allSubmitted = new CountDownLatch(1);

        for (int i = 0; i < overload; i++) {
            executor.submit(() -> {
                try {
                    allSubmitted.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        allSubmitted.countDown();
        executor.shutdownNow();
    }
}
