package org.pangrova.coordinator.coordination;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.ExecutionAnchorStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Process-scoped composition binding shared by query HA and maintenance-only publication. */
public final class CoordinatorExecutionAuthority implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CoordinatorExecutionAuthority.class);
    private final ExecutionAnchorStore backend;
    private final String coordinatorId;
    private final String incarnation;
    private final Duration leaseDuration;
    private final ScheduledExecutorService renewal;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycle = new Object();

    /** Binds a real backend to one actual Coordinator process, independent of its enabled roles. */
    public CoordinatorExecutionAuthority(ExecutionAnchorStore backend, String coordinatorId, String incarnation,
            Duration leaseDuration) {
        this.backend = Objects.requireNonNull(backend, "durable execution backend");
        if (coordinatorId == null || coordinatorId.isBlank() || incarnation == null || incarnation.isBlank()
                || leaseDuration == null || leaseDuration.toMillis() < 3) {
            throw new IllegalArgumentException("execution authority requires an actual process identity and lease");
        }
        this.coordinatorId = coordinatorId;
        this.incarnation = incarnation;
        this.leaseDuration = leaseDuration;
        renewal = ManagedExecutorFactory.newSingleThreadScheduledExecutor("execution-authority-renewal");
        renewal.scheduleWithFixedDelay(this::renewSafely, leaseDuration.toMillis() / 3,
                leaseDuration.toMillis() / 3, TimeUnit.MILLISECONDS);
    }

    /** Acquires only through the authoritative backend CAS and backend clock. */
    public Optional<ExecutionAnchor> acquire(String executionId) {
        synchronized (lifecycle) {
            if (closed.get()) throw new IllegalStateException("Coordinator execution authority is closed");
            var current = backend.current(executionId).filter(anchor -> coordinatorId.equals(anchor.ownerId())
                    && incarnation.equals(anchor.ownerIncarnation()));
            if (current.isPresent()) return current;
            return backend.tryAcquire(executionId, coordinatorId, incarnation, leaseDuration);
        }
    }

    /** Completes only an anchor actually issued to this process. */
    public boolean release(ExecutionAnchor anchor) {
        requireProcess(anchor);
        return backend.release(anchor);
    }

    /** Makes only this exact process token eligible for recovery. */
    public boolean yield(ExecutionAnchor anchor) {
        requireProcess(anchor);
        return backend.yield(anchor);
    }

    /** Shared bootstrap backend used by the query HA registry. */
    public ExecutionAnchorStore backend() { return backend; }
    /** Actual configured node identifier. */
    public String coordinatorId() { return coordinatorId; }
    /** Actual single startup identity shared across roles. */
    public String incarnation() { return incarnation; }

    private void requireProcess(ExecutionAnchor anchor) {
        if (!coordinatorId.equals(anchor.ownerId()) || !incarnation.equals(anchor.ownerIncarnation())) {
            throw new IllegalArgumentException("anchor belongs to another Coordinator process");
        }
    }

    private void renewSafely() {
        synchronized (lifecycle) {
            if (closed.get()) return;
            try {
                backend.renewOwned(coordinatorId, incarnation, leaseDuration);
            } catch (RuntimeException failure) {
                LOG.error("event=execution_authority_renewal_failed coordinatorId={}", coordinatorId, failure);
            }
        }
    }

    /** Stops renewal and yields, rather than deletes, unfinished executions for exact-token recovery. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        renewal.shutdownNow();
        synchronized (lifecycle) {
            // An in-flight JDBC renewal must finish before the final yield; it cannot revive this process afterwards.
            backend.yieldOwned(coordinatorId, incarnation);
        }
    }
}
