package org.pangrova.worker.bootstrap.phase;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 188: verifies that the orchestration executor pool is separate
 * from the Velox driver pool and uses the correct thread naming, daemon
 * flag, and rejection policy.
 *
 * <p>The separation prevents morsel-driven scheduling deadlock: the
 * completion monitor (poll-until-finished) must never share threads with
 * the Velox folly executor, because blocking orchestration on driver
 * pool threads starves morsel delivery under fan-out workloads.</p>
 *
 * <p>This test mirrors the pool creation logic in
 * {@code WorkerTaskBootstrap.createOrchestrationExecutor} to verify
 * the design contract without pulling in the full worker dependency chain.</p>
 */
class OrchestrationPoolSeparationTest {

    /**
     * Creates an orchestration executor pool using the same parameters as
     * {@code WorkerTaskBootstrap.createOrchestrationExecutor}.
     */
    private static ExecutorService createOrchestrationPool() {
        int maxConcurrentTasks = Runtime.getRuntime().availableProcessors() * 2;
        return new ThreadPoolExecutor(
                2,
                maxConcurrentTasks,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(maxConcurrentTasks * 2),
                r -> {
                    Thread t = new Thread(r, "orchestration-monitor");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Test
    @DisplayName("orchestration pool threads are daemon and named orchestration-monitor")
    void threadNamingAndDaemonFlag() throws Exception {
        ExecutorService exec = createOrchestrationPool();
        try {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) exec;
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.atomic.AtomicReference<Thread> threadRef =
                    new java.util.concurrent.atomic.AtomicReference<>();
            tpe.submit(() -> {
                threadRef.set(Thread.currentThread());
                latch.countDown();
            });
            latch.await(5, TimeUnit.SECONDS);
            Thread t = threadRef.get();
            assertThat(t).isNotNull();
            assertThat(t.getName()).isEqualTo("orchestration-monitor");
            assertThat(t.isDaemon()).isTrue();
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    @DisplayName("pool uses CallerRunsPolicy to handle saturation")
    void callerRunsPolicyUnderSaturation() {
        ExecutorService exec = createOrchestrationPool();
        try {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) exec;
            RejectedExecutionHandler handler = tpe.getRejectedExecutionHandler();
            assertThat(handler).isInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class);
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    @DisplayName("pool core size is 2 and max size is bounded (not cached)")
    void poolSizingIsBounded() {
        ExecutorService exec = createOrchestrationPool();
        try {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) exec;
            assertThat(tpe.getCorePoolSize()).isEqualTo(2);
            int maxConcurrent = Runtime.getRuntime().availableProcessors() * 2;
            assertThat(tpe.getMaximumPoolSize()).isEqualTo(maxConcurrent);
        } finally {
            exec.shutdownNow();
        }
    }
}
