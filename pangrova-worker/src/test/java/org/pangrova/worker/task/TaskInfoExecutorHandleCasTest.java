package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies TaskInfo.compareAndSetExecutorHandle prevents concurrent double-release (R7.8).
 *
 * <p>The old code used {@code setExecutorHandle(0)} which is a plain write — two threads
 * could both read the non-zero handle, both set it to 0, and both call releaseTask.
 * The CAS fix ensures only one thread wins the race.</p>
 */
class TaskInfoExecutorHandleCasTest {

    @Test
    void compareAndSetSucceedsWhenExpectedMatches() {
        TaskInfo info = createTaskInfo();
        info.setExecutorHandle(42L);

        boolean result = info.compareAndSetExecutorHandle(42L, 0L);

        assertThat(result).isTrue();
        assertThat(info.executorHandle()).isEqualTo(0L);
    }

    @Test
    void compareAndSetFailsWhenExpectedDoesNotMatch() {
        TaskInfo info = createTaskInfo();
        info.setExecutorHandle(42L);

        boolean result = info.compareAndSetExecutorHandle(99L, 0L);

        assertThat(result).isFalse();
        assertThat(info.executorHandle()).isEqualTo(42L);
    }

    @Test
    void compareAndSetFailsAfterAlreadyCleared() {
        TaskInfo info = createTaskInfo();
        info.setExecutorHandle(42L);

        // First CAS succeeds
        assertThat(info.compareAndSetExecutorHandle(42L, 0L)).isTrue();
        // Second CAS with stale expected value fails
        assertThat(info.compareAndSetExecutorHandle(42L, 0L)).isFalse();
        assertThat(info.executorHandle()).isEqualTo(0L);
    }

    @Test
    void onlyOneThreadWinsCasUnderConcurrency() throws InterruptedException {
        TaskInfo info = createTaskInfo();
        info.setExecutorHandle(100L);

        int threadCount = 8;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger casWinners = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    if (info.compareAndSetExecutorHandle(100L, 0L)) {
                        casWinners.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        doneLatch.await();

        // Exactly one thread should win the CAS
        assertThat(casWinners.get()).isEqualTo(1);
        assertThat(info.executorHandle()).isEqualTo(0L);
    }

    private static TaskInfo createTaskInfo() {
        return new TaskInfo("task-1", "query-1", null, Instant.now());
    }
}
