package org.pangrova.query.retention;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.pangrova.query.api.retention.ReaderPinLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Renews physically live query pins independently of terminal cleanup I/O.
 * The authoritative query registry and provider are borrowed, never copied or closed.
 * Server supplies a timer and a separate bounded renewal executor. Closing prevents
 * new calls but does not interrupt in-flight storage I/O; Server must drain that
 * executor before closing the provider or its storage.
 */
public final class ReaderPinRenewal implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderPinRenewal.class);
    private final ReaderPinLifecycle lifecycle;
    private final Supplier<? extends Collection<String>> activeQueries;
    private final LongSupplier clock;
    private final long intervalNanos;
    private final AtomicBoolean inFlight = new AtomicBoolean();
    private volatile boolean closed;
    // Accessed only while holding inFlight.
    private long nextRenewal;
    private ScheduledFuture<?> scheduled;

    /** Borrows the provider and registry; the supplied clock measures monotonic nanoseconds. */
    public ReaderPinRenewal(ReaderPinLifecycle lifecycle,
            Supplier<? extends Collection<String>> activeQueries, LongSupplier clock) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.activeQueries = Objects.requireNonNull(activeQueries, "activeQueries");
        this.clock = Objects.requireNonNull(clock, "clock");
        intervalNanos = lifecycle.renewIntervalNanos();
        if (intervalNanos <= 0) throw new IllegalArgumentException("Reader pin renewal interval must be positive");
        nextRenewal = clock.getAsLong();
    }

    /** Starts once; timer callbacks only enqueue bounded work, never perform storage I/O. */
    public synchronized void start(ScheduledExecutorService timer, Executor worker) {
        Objects.requireNonNull(timer, "timer");
        Objects.requireNonNull(worker, "worker");
        if (closed) throw new IllegalStateException("Reader pin renewal is closed");
        if (scheduled != null) throw new IllegalStateException("Reader pin renewal is already started");
        scheduled = timer.scheduleWithFixedDelay(() -> dispatch(worker), 0, intervalNanos, TimeUnit.NANOSECONDS);
    }

    /** May also be driven by a cleanup/recovery tick; concurrent requests are coalesced. */
    public void renewDue() {
        if (closed || !inFlight.compareAndSet(false, true)) return;
        try {
            renewOwned();
        } finally {
            inFlight.set(false);
        }
    }

    private void dispatch(Executor worker) {
        if (closed || !inFlight.compareAndSet(false, true)) return;
        try {
            worker.execute(() -> {
                try {
                    renewOwned();
                } finally {
                    inFlight.set(false);
                }
            });
        } catch (RejectedExecutionException rejected) {
            inFlight.set(false);
            LOG.warn("event=reader_pin_renewal_dispatch_rejected", rejected);
        }
    }

    private void renewOwned() {
        if (closed) return;
        long now = clock.getAsLong();
        if (now - nextRenewal < 0) return;
        nextRenewal = now + intervalNanos;
        try {
            List<String> queries = List.copyOf(activeQueries.get());
            if (!queries.isEmpty()) lifecycle.renew(queries);
        } catch (RuntimeException failure) {
            // Lease loss/outage is not converted to successful renewal or a different provider.
            LOG.warn("event=reader_pin_renewal_failed", failure);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (scheduled != null) scheduled.cancel(false);
    }
}
