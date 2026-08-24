package org.pangrova.coordinator.commit;

import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Resilient wrapper around a {@link CommitListener} that adds bounded queuing,
 * retry with exponential backoff, and observability counters.
 */
public final class ResilientCommitListenerDispatcher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ResilientCommitListenerDispatcher.class);

    private static final int DEFAULT_QUEUE_CAPACITY = 1024;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_INITIAL_BACKOFF_MS = 50;
    private static final long DEFAULT_MAX_BACKOFF_MS = 2000;
    private static final long CLOSE_JOIN_TIMEOUT_MS = 5000;

    /** Shared daemon scheduler used for retry backoff delays instead of Thread.sleep. */
    private static final ScheduledExecutorService RETRY_BACKOFF_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "commit-listener-backoff");
                thread.setDaemon(true);
                return thread;
            });

    private final CommitListener delegate;
    private final LinkedBlockingQueue<CommitEvent> queue;
    private final int maxRetries;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final Thread dispatchThread;
    private volatile boolean closed;

    // Observability counters
    private final LongAdder dispatchedEvents = new LongAdder();
    private final LongAdder droppedEvents = new LongAdder();
    private final LongAdder failedEvents = new LongAdder();
    private final LongAdder retriedEvents = new LongAdder();

    public ResilientCommitListenerDispatcher(CommitListener delegate) {
        this(delegate, DEFAULT_QUEUE_CAPACITY, DEFAULT_MAX_RETRIES,
                DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAX_BACKOFF_MS);
    }

    /** Creates a dispatcher with custom queue capacity, retry budget and backoff bounds. */
    public ResilientCommitListenerDispatcher(CommitListener delegate,
                                             int queueCapacity,
                                             int maxRetries,
                                             long initialBackoffMs,
                                             long maxBackoffMs) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be >= 1, got " + queueCapacity);
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
        }
        if (initialBackoffMs <= 0) {
            throw new IllegalArgumentException("initialBackoffMs must be > 0, got " + initialBackoffMs);
        }
        if (maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException("maxBackoffMs must be >= initialBackoffMs");
        }
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.dispatchThread = new Thread(this::dispatchLoop, "commit-listener-dispatch");
        this.dispatchThread.setDaemon(true);
        this.dispatchThread.start();
    }

    /** Enqueues a commit event for asynchronous delivery, evicting the oldest entry when full. */
    public void dispatch(CommitEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (closed) {
            LOG.warn("action=dispatch_after_close table={}.{}.{} eventType={}",
                    event.catalogName(), event.schemaName(), event.tableName(), event.eventType());
            droppedEvents.increment();
            return;
        }
        if (!queue.offer(event)) {
            CommitEvent evicted = queue.poll();
            if (evicted != null) {
                droppedEvents.increment();
                LOG.warn("action=commit_event_evicted table={}.{}.{} eventType={} "
                                + "queueSize={} droppedTotal={}",
                        evicted.catalogName(), evicted.schemaName(), evicted.tableName(),
                        evicted.eventType(), queue.size(), droppedEvents.sum());
            }
            if (!queue.offer(event)) {
                droppedEvents.increment();
                LOG.warn("action=commit_event_dropped table={}.{}.{} eventType={} droppedTotal={}",
                        event.catalogName(), event.schemaName(), event.tableName(),
                        event.eventType(), droppedEvents.sum());
            }
        }
    }

    public CommitListener asListener() {
        return this::dispatch;
    }

    public long dispatchedCount() {
        return dispatchedEvents.sum();
    }

    public long droppedCount() {
        return droppedEvents.sum();
    }

    public long failedCount() {
        return failedEvents.sum();
    }

    public long retriedCount() {
        return retriedEvents.sum();
    }

    public int pendingCount() {
        return queue.size();
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        dispatchThread.interrupt();
        try {
            dispatchThread.join(CLOSE_JOIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        int remaining = queue.size();
        if (remaining > 0) {
            LOG.warn("action=dispatcher_closed_with_pending pendingEvents={}", remaining);
        }
    }

    private void dispatchLoop() {
        while (!closed) {
            try {
                CommitEvent event = queue.take();
                deliverWithRetry(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        CommitEvent event;
        while ((event = queue.poll()) != null) {
            deliverWithRetry(event);
        }
    }

    private void deliverWithRetry(CommitEvent event) {
        long backoffMs = initialBackoffMs;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                delegate.onCommit(event);
                dispatchedEvents.increment();
                return;
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    failedEvents.increment();
                    LOG.error("action=commit_listener_failed_permanently table={}.{}.{} "
                                    + "eventType={} attempts={} failedTotal={} error={}",
                            event.catalogName(), event.schemaName(), event.tableName(),
                            event.eventType(), attempt + 1, failedEvents.sum(),
                            e.getMessage(), e);
                    return;
                }
                retriedEvents.increment();
                LOG.warn("action=commit_listener_retry table={}.{}.{} eventType={} "
                                + "attempt={} nextBackoffMs={} error={}",
                        event.catalogName(), event.schemaName(), event.tableName(),
                        event.eventType(), attempt + 1, backoffMs, e.getMessage());
                if (!awaitRetryBackoff(backoffMs)) {
                    failedEvents.increment();
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
            }
        }
    }

    /**
     * Waits for the given backoff duration on the shared scheduler; returns false when interrupted.
     */
    private boolean awaitRetryBackoff(long backoffMs) {
        try {
            RETRY_BACKOFF_SCHEDULER.schedule(() -> { }, backoffMs, TimeUnit.MILLISECONDS).get();
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            // The no-op backoff task cannot fail; treat any such anomaly as retriable.
            return true;
        }
    }
}
