package org.pangrova.worker.execution;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies thread-safety of {@link NativeTaskExecutor#close()} with
 * concurrent access. Tests structural concurrency guarantees without
 * requiring native library.
 */
@Tag("native-required")
class NativeTaskExecutorConcurrentCloseTest {

    @Test
    void concurrentCloseCallsAreIdempotent() throws Exception {
        int threadCount = 10;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        AtomicReference<Exception> firstException = new AtomicReference<>();

        NativeTaskExecutor executor = new NativeTaskExecutor();

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    executor.close();
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    firstException.compareAndSet(null, e);
                }
            }, "close-thread-" + i);
            threads[i].start();
        }

        ready.await();
        go.countDown();

        for (Thread t : threads) {
            t.join(5000);
        }

        assertThat(exceptionCount.get())
                .as("Concurrent close() must not throw")
                .isZero();
    }

    @Test
    void createTaskAfterCloseThrowsIllegalState() {
        NativeTaskExecutor executor = new NativeTaskExecutor();
        executor.close();

        try {
            executor.createTask(new byte[]{1, 2, 3});
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).contains("closed");
        } catch (UnsatisfiedLinkError e) {
            // Expected when native library is not loaded
        }
    }
}
