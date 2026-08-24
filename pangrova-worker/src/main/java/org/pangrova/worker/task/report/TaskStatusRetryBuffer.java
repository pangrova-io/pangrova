package org.pangrova.worker.task.report;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * Buffers failed task status reports and retries delivery with exponential backoff.
 *
 * <p>When the Coordinator is unreachable, callers submit failed reports via
 * {@link #buffer(TaskStatusReport)}. A background scheduler periodically drains
 * the buffer and re-sends each report through the provided {@link StatusReportSender}.
 * Successfully delivered reports are removed; failures remain for the next cycle.
 *
 * <p><strong>Backoff strategy</strong>: initial 1 s, doubling up to 30 s max.
 * On any successful delivery the backoff resets to the initial value.
 *
 * <p><strong>Bounded queue</strong>: the buffer holds at most {@code maxBufferSize}
 * reports. When full, the oldest report is evicted to make room (log + drop).
 *
 * <p><strong>Thread safety</strong>: all public methods are safe for concurrent use.
 * The internal queue is a {@link LinkedBlockingQueue}; the scheduler runs on a
 * single named daemon thread owned by this class.
 *
 * @see StatusReportSender
 */
public final class TaskStatusRetryBuffer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TaskStatusRetryBuffer.class);

    static final int DEFAULT_MAX_BUFFER_SIZE = 1024;
    static final long DEFAULT_INITIAL_BACKOFF_MS = 1_000L;
    static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;

    private final StatusReportSender sender;
    private final LinkedBlockingQueue<TaskStatusReport> queue;
    private final int maxBufferSize;
    private final ScheduledExecutorService scheduler;
    private final BackoffStrategy backoff;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> retryFuture;

    /**
     * Creates a retry buffer with default settings (1024 max, 1 s → 30 s backoff).
     *
     * @param sender the delegate that performs the actual HTTP POST
     */
    public TaskStatusRetryBuffer(StatusReportSender sender) {
        this(sender, DEFAULT_MAX_BUFFER_SIZE, DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAX_BACKOFF_MS);
    }

    /**
     * Creates a retry buffer with custom capacity and backoff parameters.
     *
     * @param sender          the delegate that performs the actual HTTP POST
     * @param maxBufferSize   maximum number of buffered reports (prevents OOM)
     * @param initialBackoffMs initial retry interval in milliseconds
     * @param maxBackoffMs    maximum retry interval in milliseconds
     */
    public TaskStatusRetryBuffer(StatusReportSender sender, int maxBufferSize,
                                 long initialBackoffMs, long maxBackoffMs) {
        this.sender = requireNonNull(sender, "sender must not be null");
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive, got " + maxBufferSize);
        }
        this.maxBufferSize = maxBufferSize;
        this.queue = new LinkedBlockingQueue<>(maxBufferSize);
        this.backoff = BackoffStrategy.retry(initialBackoffMs, maxBackoffMs);
        this.scheduler = createScheduler();
        scheduleNextRetry(initialBackoffMs);
        LOG.info("event=retry_buffer_created maxBufferSize={} initialBackoffMs={} maxBackoffMs={}",
                maxBufferSize, initialBackoffMs, maxBackoffMs);
    }

    /**
     * Buffers a failed report for later retry.
     *
     * <p>If the buffer is full, the oldest report is evicted to make room.
     *
     * @param report the report that failed to send
     */
    public void buffer(TaskStatusReport report) {
        requireNonNull(report, "report must not be null");
        if (closed.get()) {
            LOG.warn("event=retry_buffer_rejected_closed taskId={} queryId={}",
                    report.taskId(), report.queryId());
            return;
        }
        if (!queue.offer(report)) {
            TaskStatusReport evicted = queue.poll();
            if (evicted != null) {
                LOG.warn("event=retry_buffer_evicted taskId={} queryId={} state={}",
                        evicted.taskId(), evicted.queryId(), evicted.state());
            }
            queue.offer(report);
        }
        LOG.debug("event=retry_buffer_enqueued taskId={} queryId={} state={} bufferSize={}",
                report.taskId(), report.queryId(), report.state(), queue.size());
    }

    /**
     * Returns the current number of buffered reports.
     */
    public int size() {
        return queue.size();
    }

    /**
     * Returns the maximum buffer capacity.
     */
    public int maxBufferSize() {
        return maxBufferSize;
    }

    /**
     * Drains the buffer and attempts to re-send each report.
     * Successfully sent reports are removed; failures are re-queued.
     */
    // Package-private for testability
    void drainAndRetry() {
        if (queue.isEmpty()) {
            return;
        }
        List<TaskStatusReport> batch = new ArrayList<>(queue.size());
        queue.drainTo(batch);
        if (batch.isEmpty()) {
            return;
        }

        LOG.info("event=retry_buffer_drain_start count={}", batch.size());
        boolean[] results = sendBatch(batch);
        boolean anySuccess = results[0];
        boolean anyFailure = results[1];

        if (anySuccess) {
            backoff.reset();
        }
        long nextDelay = anyFailure ? backoff.next() : DEFAULT_INITIAL_BACKOFF_MS;
        scheduleNextRetry(nextDelay);
        LOG.info("event=retry_buffer_drain_complete delivered={} remaining={} nextRetryMs={}",
                batch.size() - queue.size(), queue.size(), nextDelay);
    }

    private boolean[] sendBatch(List<TaskStatusReport> batch) {
        boolean anySuccess = false;
        boolean anyFailure = false;
        for (TaskStatusReport report : batch) {
            boolean sent = trySendReport(report);
            if (sent) {
                anySuccess = true;
            } else {
                anyFailure = true;
                requeue(report);
            }
        }
        return new boolean[]{anySuccess, anyFailure};
    }

    private boolean trySendReport(TaskStatusReport report) {
        try {
            boolean sent = sender.send(report);
            if (sent) {
                LOG.info("event=retry_buffer_delivered taskId={} queryId={} state={}",
                        report.taskId(), report.queryId(), report.state());
            }
            return sent;
        } catch (Exception e) {
            LOG.warn("event=retry_buffer_send_error taskId={} queryId={} error={}",
                    report.taskId(), report.queryId(), e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        cancelRetryFuture();
        shutdownScheduler();
        int remaining = queue.size();
        if (remaining > 0) {
            LOG.warn("event=retry_buffer_closed_with_pending remaining={}", remaining);
        }
        LOG.info("event=retry_buffer_closed");
    }

    private void cancelRetryFuture() {
        ScheduledFuture<?> future = retryFuture;
        if (future != null) {
            future.cancel(false);
        }
    }

    private void shutdownScheduler() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void requeue(TaskStatusReport report) {
        if (!queue.offer(report)) {
            LOG.warn("event=retry_buffer_requeue_dropped taskId={} queryId={}",
                    report.taskId(), report.queryId());
        }
    }

    private void scheduleNextRetry(long delayMs) {
        if (closed.get()) {
            return;
        }
        retryFuture = scheduler.schedule(this::drainAndRetry, delayMs, TimeUnit.MILLISECONDS);
    }

    private static ScheduledExecutorService createScheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "task-status-retry");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }
}
