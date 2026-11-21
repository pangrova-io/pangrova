package org.pangrova.security.audit;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventWriter;

/**
 * Asynchronous emitter that delegates audit writes to a background thread.
 * Write failures are logged but never propagated — fault isolation for audit path.
 *
 * <p>Concurrency strategy:
 * <ul>
 *   <li>{@code closed} — AtomicBoolean for idempotent shutdown signaling</li>
 *   <li>{@code eventsRejectedAfterClose} — AtomicLong for lock-free rejected event counting</li>
 *   <li>{@code executor} — single-thread executor serializes all writes;
 *       {@link #emit} can be called from any thread</li>
 * </ul>
 * </p>
 */
@ThreadSafe
public class AuditEventEmitter {

    private static final Logger LOG = LoggerFactory.getLogger(AuditEventEmitter.class);
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

    private final ExecutorService executor;
    private final AuditEventWriter writer;
    private final long shutdownTimeoutMs;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong eventsRejectedAfterClose = new AtomicLong(0);
    private final AtomicLong eventsLostDuringShutdownCount = new AtomicLong(0);
    private volatile boolean shutdownTimedOut = false;

    /**
     * Creates an emitter backed by a single-thread executor with default shutdown timeout (5s).
     *
     * @param writer the audit event writer to delegate to
     * @throws NullPointerException if writer is null
     */
    public AuditEventEmitter(AuditEventWriter writer) {
        this(writer, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Creates an emitter with configurable shutdown timeout.
     *
     * @param writer            the audit event writer to delegate to
     * @param shutdownTimeoutMs max time in ms to wait for pending events during shutdown
     * @throws NullPointerException     if writer is null
     * @throws IllegalArgumentException if shutdownTimeoutMs is not positive
     */
    public AuditEventEmitter(AuditEventWriter writer, long shutdownTimeoutMs) {
        this.writer = Objects.requireNonNull(writer, "writer must not be null");
        if (shutdownTimeoutMs <= 0) {
            throw new IllegalArgumentException("shutdownTimeoutMs must be positive");
        }
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.executor = ManagedExecutorFactory.newSingleThreadExecutor("audit-emitter");
    }

    /**
     * Submits an audit event for asynchronous writing.
     * Events submitted after {@link #close()} are rejected and counted.
     *
     * @param event the audit event to emit
     * @throws NullPointerException if event is null
     */
    public void emit(AuditEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        if (closed.get()) {
            eventsRejectedAfterClose.incrementAndGet();
            LOG.warn("action=audit_emit_rejected eventId={} reason=emitter_closed",
                    event.eventId());
            return;
        }
        executor.submit(() -> writeQuietly(event));
    }

    /**
     * Graceful shutdown: allows pending tasks to complete within timeout,
     * then forces shutdown if necessary.
     *
     * <p>Idempotent: calling close() multiple times is safe.</p>
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                shutdownTimedOut = true;
                java.util.List<Runnable> cancelled = executor.shutdownNow();
                eventsLostDuringShutdownCount.set(cancelled.size());
            }
        } catch (InterruptedException e) {
            shutdownTimedOut = true;
            java.util.List<Runnable> cancelled = executor.shutdownNow();
            eventsLostDuringShutdownCount.set(cancelled.size());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns whether the last shutdown timed out waiting for pending events.
     */
    public boolean shutdownTimedOut() {
        return shutdownTimedOut;
    }

    /**
     * Returns the number of events cancelled during forced shutdown.
     * Counts tasks that were queued in the executor but never executed
     * because {@code shutdownNow()} was called after the timeout expired.
     */
    public long eventsLostDuringShutdown() {
        return eventsLostDuringShutdownCount.get();
    }

    /**
     * Returns the number of events rejected after close was called.
     */
    public long eventsRejectedAfterClose() {
        return eventsRejectedAfterClose.get();
    }

    private void writeQuietly(AuditEvent event) {
        try {
            writer.write(event);
        } catch (Exception e) {
            LOG.error("action=audit_write status=failed eventId={} error={}",
                    event.eventId(), e.getMessage());
        }
    }
}
