package org.pangrova.governance.core.lineage;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.spi.query.lineage.QueryLineageFact;
import org.pangrova.persistence.api.query.lineage.QueryLineageOutbox;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Non-blocking lineage event emitter with bounded queue, retry, and graceful shutdown.
 * Durable mode commits a frozen fact before returning; HTTP delivery remains asynchronous.
 *
 * <p>Concurrency strategy:
 * <ul>
 *   <li>{@code closed} — AtomicBoolean for idempotent shutdown signaling</li>
 *   <li>{@code running} — volatile flag for consumer loop termination</li>
 *   <li>{@code eventsDroppedTotal} — AtomicLong for lock-free dropped event counting</li>
 *   <li>{@code retryQueue} — ArrayBlockingQueue for thread-safe event handoff
 *       between producer (query threads) and consumer (emitter thread)</li>
 * </ul>
 * </p>
 */
@ThreadSafe
public final class LineageEventEmitter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LineageEventEmitter.class);
    private static final int QUEUE_CAPACITY = 1000;
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;
    private static final long DEFAULT_DROPPED_EVENTS_ALERT_THRESHOLD = 100;
    private static final long EMPTY_POLL_SLEEP_MS = 500;
    private static final long BACKOFF_SLEEP_CHUNK_MS = 500;
    private static final long MAX_RETRY_BACKOFF_MS = 60_000;

    private final LineageBackend backend;
    private final QueryLineageOutbox outbox;
    private final String namespace;
    private final Object producerGate = new Object();
    @org.pangrova.common.concurrent.GuardedBy("producerGate")
    private int enteredProducers;
    private static final long DURABLE_DELIVERY_LEASE_SECONDS = 30;
    private final BlockingQueue<LineageEvent> retryQueue;
    private final AtomicLong eventsDroppedTotal = new AtomicLong(0);
    private final ExecutorService executor;
    private final long shutdownTimeoutMs;
    private final long droppedEventsAlertThreshold;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean running = true;
    private volatile boolean shutdownTimedOut = false;
    private volatile Thread consumerThread;
    private volatile Throwable backendCloseFailure;

    /**
     * Creates a new emitter with default shutdown timeout (5s) and alert threshold (100).
     *
     * @param backend destination for lineage events (must not be null)
     */
    public LineageEventEmitter(LineageBackend backend) {
        this(backend, DEFAULT_SHUTDOWN_TIMEOUT_MS, DEFAULT_DROPPED_EVENTS_ALERT_THRESHOLD);
    }

    /**
     * Creates a new emitter with configurable shutdown and alerting parameters.
     *
     * @param backend                     destination for lineage events (must not be null)
     * @param shutdownTimeoutMs           max time to wait for consumer thread termination
     * @param droppedEventsAlertThreshold threshold for dropped events WARN log
     */
    public LineageEventEmitter(LineageBackend backend,
                               long shutdownTimeoutMs,
                               long droppedEventsAlertThreshold) {
        this(backend, shutdownTimeoutMs, droppedEventsAlertThreshold, null, "pangrova");
    }

    /** Uses the same single HTTP owner with durable facts instead of an in-memory queue. */
    public LineageEventEmitter(LineageBackend backend, QueryLineageOutbox outbox, String namespace) {
        this(backend, DEFAULT_SHUTDOWN_TIMEOUT_MS, DEFAULT_DROPPED_EVENTS_ALERT_THRESHOLD,
                java.util.Objects.requireNonNull(outbox, "outbox"), namespace);
    }

    /** Configures a finite drain budget while retaining the same durable delivery owner. */
    public LineageEventEmitter(LineageBackend backend, QueryLineageOutbox outbox,
            String namespace, long shutdownTimeoutMs) {
        this(backend, shutdownTimeoutMs, DEFAULT_DROPPED_EVENTS_ALERT_THRESHOLD,
                java.util.Objects.requireNonNull(outbox, "outbox"), namespace);
    }

    private LineageEventEmitter(LineageBackend backend, long shutdownTimeoutMs,
            long droppedEventsAlertThreshold, QueryLineageOutbox outbox, String namespace) {
        if (backend == null) {
            throw new IllegalArgumentException("backend must not be null");
        }
        if (shutdownTimeoutMs <= 0) {
            throw new IllegalArgumentException("shutdownTimeoutMs must be positive");
        }
        if (droppedEventsAlertThreshold <= 0) {
            throw new IllegalArgumentException("droppedEventsAlertThreshold must be positive");
        }
        this.backend = backend;
        this.outbox = outbox;
        this.namespace = java.util.Objects.requireNonNull(namespace, "namespace");
        if (namespace.isBlank() || namespace.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Lineage requires an unambiguous deployment namespace");
        }
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.droppedEventsAlertThreshold = droppedEventsAlertThreshold;
        this.retryQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        this.executor = ManagedExecutorFactory.newSingleThreadExecutor("lineage-emitter");
        this.executor.submit(this::processRetryQueue);
    }

    public void emitStartEvent(String queryId, String sql,
                                List<DatasetRef> inputs, List<DatasetRef> outputs) {
        emit(buildEvent(LineageEvent.EventType.START, queryId, sql, inputs, outputs, Map.of()));
    }

    public void emitCompleteEvent(String queryId) {
        emit(buildEvent(LineageEvent.EventType.COMPLETE, queryId, null, List.of(), List.of(), Map.of()));
    }

    public void emitFailEvent(String queryId, String errorMessage) {
        emit(buildEvent(LineageEvent.EventType.FAIL, queryId, null,
            List.of(), List.of(), Map.of("error", errorMessage)));
    }

    /** Enqueues the terminal type with its actual logical dataset lineage and facets. */
    public void emitTerminalEvent(LineageEvent.EventType type, String queryId, String sql,
            List<DatasetRef> inputs, List<DatasetRef> outputs, Map<String, String> facets) {
        if (type == LineageEvent.EventType.START) {
            throw new IllegalArgumentException("Terminal lineage cannot emit START");
        }
        emit(buildEvent(type, queryId, sql, inputs, outputs, facets));
    }

    /**
     * Returns the total number of events dropped due to queue overflow.
     */
    public long eventsDroppedTotal() {
        return eventsDroppedTotal.get();
    }

    /**
     * Returns whether the last shutdown timed out waiting for the consumer thread.
     */
    public boolean shutdownTimedOut() {
        return shutdownTimedOut;
    }

    /**
     * Graceful shutdown: signals consumer loop to stop, lets the executor drain
     * remaining in-memory events within the timeout. Durable facts remain in the outbox.
     * Repeated close calls may wait again for an owner that previously exceeded the budget.
     *
     * <p>Never calls {@code backend.send()} on the caller's thread — all drain
     * work happens on the consumer thread within the executor's shutdown window.</p>
     *
     * <p>Idempotent: calling close() multiple times is safe.</p>
     */
    @Override
    public void close() {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMs);
        synchronized (producerGate) {
            closed.set(true);
            running = false;
        }
        if (Thread.currentThread() == consumerThread) {
            executor.shutdown();
            shutdownTimedOut = true;
            return;
        }
        boolean producersExited = awaitProducers(deadline);
        long remaining = Math.max(1, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
        ResourceCloser.shutdownExecutor(executor, Duration.ofMillis(remaining));
        shutdownTimedOut = !producersExited || !executor.isTerminated();
        LOG.info("action=lineage_emitter_shutdown durableOutbox={} remainingInMemoryQueue={} timedOut={}",
                outbox != null, retryQueue.size(), shutdownTimedOut);
        if (!shutdownTimedOut && backendCloseFailure != null) {
            throw new IllegalStateException("Lineage HTTP owner failed to close", backendCloseFailure);
        }
    }

    /** Commits an exact pre-frozen query fact; journal replay uses the same identity and payload. */
    public void record(QueryLineageFact fact) {
        java.util.Objects.requireNonNull(fact, "fact");
        if (!namespace.equals(fact.namespace())) throw new IllegalArgumentException("Lineage namespace changed");
        enterProducer();
        try {
            if (outbox == null) enqueue(LineageFacts.event(fact));
            else outbox.append(fact);
        } finally {
            exitProducer();
        }
    }

    /** Returns the frozen deployment namespace used in durable event identity. */
    public String namespace() { return namespace; }
    /** True only when emission first commits to the actual metadata outbox. */
    public boolean isDurable() { return outbox != null; }

    private void emit(LineageEvent event) {
        if (outbox != null) {
            record(LineageFacts.fact(namespace, event));
            return;
        }
        enterProducer();
        try {
            enqueue(event);
        } finally {
            exitProducer();
        }
    }

    private void enqueue(LineageEvent event) {
        // BUG-G01: Drop the incoming event on queue full instead of evicting the oldest.
        // Preserves FIFO ordering — a START event already in the queue won't be dropped
        // in favor of a later COMPLETE event.
        if (!retryQueue.offer(event)) {
            long dropped = eventsDroppedTotal.incrementAndGet();
            checkDroppedEventsThreshold(dropped);
        }
    }

    private void enterProducer() {
        synchronized (producerGate) {
            if (closed.get()) throw new IllegalStateException("Lineage producer admission is closed");
            enteredProducers++;
        }
    }

    private void exitProducer() {
        synchronized (producerGate) {
            enteredProducers--;
            producerGate.notifyAll();
        }
    }

    private boolean awaitProducers(long deadline) {
        synchronized (producerGate) {
            while (enteredProducers > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                try {
                    java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(producerGate, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    private void checkDroppedEventsThreshold(long dropped) {
        if (dropped == droppedEventsAlertThreshold || dropped % droppedEventsAlertThreshold == 0) {
            LOG.warn("action=lineage_events_dropped_alert, droppedEventsTotal={}, threshold={}",
                dropped, droppedEventsAlertThreshold);
        }
    }

    private void processRetryQueue() {
        consumerThread = Thread.currentThread();
        try {
            if (outbox == null) processUntilShutdown();
            else processDurableUntilShutdown();
        } finally {
            if (backend instanceof AutoCloseable owner) {
                try {
                    owner.close();
                } catch (Exception failure) {
                    backendCloseFailure = failure;
                    LOG.error("event=lineage_backend_close_failed error={}", failure.getMessage(), failure);
                }
            }
        }
    }

    private void processDurableUntilShutdown() {
        long backoffMs = 1000;
        while (running) {
            QueryLineageOutbox.Delivery delivery = null;
            try {
                var claimed = outbox.claim(namespace, Duration.ofSeconds(DURABLE_DELIVERY_LEASE_SECONDS));
                if (claimed.isEmpty()) {
                    ThreadUtils.sleepQuietly(EMPTY_POLL_SLEEP_MS);
                    if (Thread.currentThread().isInterrupted()) break;
                    continue;
                }
                delivery = claimed.orElseThrow();
                backend.send(LineageFacts.event(delivery.fact()));
                outbox.acknowledge(delivery);
                backoffMs = 1000;
            } catch (RuntimeException failure) {
                LOG.warn("event=lineage_durable_delivery_pending admitted_fact_retained=true", failure);
                if (delivery != null) {
                    try {
                        outbox.release(delivery);
                    } catch (RuntimeException release) {
                        failure.addSuppressed(release);
                    }
                }
                interruptibleSleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_RETRY_BACKOFF_MS);
            }
        }
        // Persisted unacknowledged rows remain available to a successor; no failed drain deletes them.
    }

    private void processUntilShutdown() {
        long backoffMs = 1000;
        while (running) {
            // BUG-G02: Peek-based head-of-line retry. The event stays at the head
            // until successfully sent, preserving FIFO order. Only poll() after
            // successful send.
            LineageEvent event = retryQueue.peek();
            if (event == null) {
                ThreadUtils.sleepQuietly(EMPTY_POLL_SLEEP_MS);
                // sleepQuietly restores the interrupt flag; treat interruption as shutdown
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                continue;
            }
            backoffMs = trySend(event, backoffMs);
        }
        drainOnConsumerThread();
    }

    private void drainOnConsumerThread() {
        LineageEvent event;
        while ((event = retryQueue.poll()) != null) {
            try {
                backend.send(event);
            } catch (Exception e) {
                LOG.debug("action=lineage_shutdown_drain_fail, runId={}, error={}",
                    event.runId(), e.getMessage());
            }
        }
    }

    private long trySend(LineageEvent event, long currentBackoff) {
        try {
            backend.send(event);
            retryQueue.poll(); // Safe: single consumer thread — no concurrent poll() possible
            return 1000;
        } catch (Exception e) {
            LOG.warn("action=lineage_emit_retry, runId={}, error={}",
                event.runId(), e.getMessage());
            // Event stays at head of queue — no re-enqueue needed
            interruptibleSleep(currentBackoff);
            return Math.min(currentBackoff * 2, MAX_RETRY_BACKOFF_MS);
        }
    }

    /**
     * Sleeps in small chunks, checking the {@code running} flag between chunks
     * so that shutdown is not blocked by a long backoff period.
     */
    private void interruptibleSleep(long ms) {
        long remaining = ms;
        while (remaining > 0 && running) {
            long chunk = Math.min(remaining, BACKOFF_SLEEP_CHUNK_MS);
            ThreadUtils.sleepQuietly(chunk);
            remaining -= chunk;
        }
    }

    private LineageEvent buildEvent(LineageEvent.EventType type, String queryId,
                                    String sql, List<DatasetRef> inputs,
                                    List<DatasetRef> outputs, Map<String, String> facets) {
        return new LineageEvent(type, queryId, "pangrova-query", sql,
            inputs, outputs, facets, Instant.now());
    }
}
