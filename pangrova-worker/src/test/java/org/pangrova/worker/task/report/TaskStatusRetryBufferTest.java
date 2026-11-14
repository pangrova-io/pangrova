package org.pangrova.worker.task.report;

import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TaskStatusRetryBuffer}.
 *
 * <p>Uses real functional implementations (no mocks) to verify:
 * <ul>
 *   <li>Failed reports are buffered</li>
 *   <li>Buffered reports are retried with backoff</li>
 *   <li>Buffer size is bounded</li>
 *   <li>Successful retry removes the report from the buffer</li>
 * </ul>
 */
class TaskStatusRetryBufferTest {

    private TaskStatusRetryBuffer buffer;

    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.close();
        }
    }

    @Test
    void failedReportsAreBuffered() {
        AtomicBoolean senderCalled = new AtomicBoolean(false);
        buffer = new TaskStatusRetryBuffer(
                report -> { senderCalled.set(true); return false; },
                64, 10_000L, 30_000L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));
        buffer.buffer(report("task-2", "q-1", "FAILED"));

        assertThat(buffer.size()).isEqualTo(2);
    }

    @Test
    void successfulRetryRemovesReportFromBuffer() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        List<TaskStatusReport> sent = new CopyOnWriteArrayList<>();

        buffer = new TaskStatusRetryBuffer(
                report -> { sent.add(report); delivered.countDown(); return true; },
                64, 50L, 200L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));

        boolean completed = delivered.await(5, TimeUnit.SECONDS);
        assertThat(completed).as("report should be delivered within timeout").isTrue();
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).taskId()).isEqualTo("task-1");
        // After successful delivery, buffer should be empty
        // Give a small window for the drain to complete
        Thread.sleep(100);
        assertThat(buffer.size()).isZero();
    }

    @Test
    void bufferedReportsAreRetriedAfterInitialFailure() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch successLatch = new CountDownLatch(1);

        // Fail first 2 attempts, succeed on 3rd
        buffer = new TaskStatusRetryBuffer(
                report -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt >= 3) {
                        successLatch.countDown();
                        return true;
                    }
                    return false;
                },
                64, 50L, 200L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));

        boolean completed = successLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).as("report should eventually be delivered").isTrue();
        assertThat(attempts.get()).isGreaterThanOrEqualTo(3);
        Thread.sleep(100);
        assertThat(buffer.size()).isZero();
    }

    @Test
    void bufferSizeIsBounded() {
        buffer = new TaskStatusRetryBuffer(
                report -> false,
                4, 10_000L, 30_000L);

        // Fill beyond capacity
        for (int i = 0; i < 6; i++) {
            buffer.buffer(report("task-" + i, "q-1", "FINISHED"));
        }

        assertThat(buffer.size()).isLessThanOrEqualTo(4);
        assertThat(buffer.maxBufferSize()).isEqualTo(4);
    }

    @Test
    void drainAndRetryKeepsFailedReportsInQueue() {
        AtomicInteger callCount = new AtomicInteger(0);
        buffer = new TaskStatusRetryBuffer(
                report -> {
                    callCount.incrementAndGet();
                    return false;
                },
                64, 10_000L, 30_000L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));
        buffer.buffer(report("task-2", "q-1", "FAILED"));

        // Manually trigger drain
        buffer.drainAndRetry();

        assertThat(callCount.get()).isEqualTo(2);
        assertThat(buffer.size()).isEqualTo(2);
    }

    @Test
    void drainAndRetryRemovesSuccessfulReports() {
        List<String> delivered = new CopyOnWriteArrayList<>();
        buffer = new TaskStatusRetryBuffer(
                report -> {
                    // Only succeed for task-1
                    if ("task-1".equals(report.taskId())) {
                        delivered.add(report.taskId());
                        return true;
                    }
                    return false;
                },
                64, 10_000L, 30_000L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));
        buffer.buffer(report("task-2", "q-1", "FAILED"));

        buffer.drainAndRetry();

        assertThat(delivered).containsExactly("task-1");
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void closeIsIdempotent() {
        buffer = new TaskStatusRetryBuffer(report -> true, 64, 10_000L, 30_000L);
        buffer.close();
        buffer.close(); // second close should not throw
    }

    @Test
    void bufferRejectsAfterClose() {
        buffer = new TaskStatusRetryBuffer(report -> true, 64, 10_000L, 30_000L);
        buffer.close();

        buffer.buffer(report("task-1", "q-1", "FINISHED"));
        assertThat(buffer.size()).isZero();
    }

    @Test
    void constructorRejectsNullSender() {
        assertThatThrownBy(() -> new TaskStatusRetryBuffer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sender");
    }

    @Test
    void constructorRejectsNonPositiveBufferSize() {
        assertThatThrownBy(() -> new TaskStatusRetryBuffer(report -> true, 0, 1000L, 30_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBufferSize");
    }

    @Test
    void senderExceptionDoesNotLoseReport() {
        AtomicInteger callCount = new AtomicInteger(0);
        buffer = new TaskStatusRetryBuffer(
                report -> {
                    callCount.incrementAndGet();
                    throw new RuntimeException("connection refused");
                },
                64, 10_000L, 30_000L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));
        buffer.drainAndRetry();

        assertThat(callCount.get()).isEqualTo(1);
        assertThat(buffer.size()).isEqualTo(1);
    }

    @Test
    void multipleReportsDeliveredInOrder() throws Exception {
        List<String> deliveryOrder = new CopyOnWriteArrayList<>();
        CountDownLatch allDelivered = new CountDownLatch(3);

        buffer = new TaskStatusRetryBuffer(
                report -> {
                    deliveryOrder.add(report.taskId());
                    allDelivered.countDown();
                    return true;
                },
                64, 50L, 200L);

        buffer.buffer(report("task-1", "q-1", "FINISHED"));
        buffer.buffer(report("task-2", "q-1", "FINISHED"));
        buffer.buffer(report("task-3", "q-1", "FINISHED"));

        boolean completed = allDelivered.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(deliveryOrder).containsExactly("task-1", "task-2", "task-3");
    }

    private static TaskStatusReport report(String taskId, String queryId, String state) {
        return new TaskStatusReport(
                taskId, queryId, 0, 0, "COMPUTE",
                "worker-1", state, null, null, null, 1, System.currentTimeMillis(),
                null, null,
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1, 0));
    }
}
