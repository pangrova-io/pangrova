package org.pangrova.coordinator.coordination;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.query.coordination.ExecutionAnchor;
import org.pangrova.persistence.api.query.coordination.ExecutionAnchorStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;

/** Process-scoped composition binding shared by query HA and maintenance-only publication. */
@ThreadSafe
public final class CoordinatorExecutionAuthority implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CoordinatorExecutionAuthority.class);
    private static final long SHUTDOWN_TIMEOUT_MS = 5000;
    private final ExecutionAnchorStore backend;
    private final String coordinatorId;
    private final String incarnation;
    private final Duration leaseDuration;
    private final ScheduledExecutorService renewal;
    private final Object lifecycle = new Object();
    private final ExecutionAuthorityOperations operations = new ExecutionAuthorityOperations();
    private final AtomicReference<QueryOwnershipRegistry> queryOwnership = new AtomicReference<>();
    @GuardedBy("lifecycle")
    private CompletableFuture<Void> closeExit;
    @GuardedBy("lifecycle")
    private Thread closer;

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
        try {
            renewal.scheduleWithFixedDelay(() -> ExecutorDrain.runOwned(renewal, this::renewSafely),
                    leaseDuration.toMillis() / 3, leaseDuration.toMillis() / 3, TimeUnit.MILLISECONDS);
        } catch (RuntimeException | Error failure) {
            try {
                ExecutorDrain.shutdownAndAwait(renewal, SHUTDOWN_TIMEOUT_MS);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Acquires only through the authoritative backend CAS and backend clock. */
    public Optional<ExecutionAnchor> acquire(String executionId) {
        var operation = operations.enter();
        try {
            var current = backend.current(executionId).filter(anchor -> coordinatorId.equals(anchor.ownerId())
                    && incarnation.equals(anchor.ownerIncarnation()));
            if (current.isPresent()) return current;
            return backend.tryAcquire(executionId, coordinatorId, incarnation, leaseDuration);
        } finally {
            operation.close();
        }
    }

    /** Completes only an anchor actually issued to this process. */
    public boolean release(ExecutionAnchor anchor) {
        requireProcess(anchor);
        var operation = operations.enter();
        try {
            return backend.release(anchor);
        } finally {
            operation.close();
        }
    }

    /** Makes only this exact process token eligible for recovery. */
    public boolean yield(ExecutionAnchor anchor) {
        requireProcess(anchor);
        var operation = operations.enter();
        try {
            return backend.yield(anchor);
        } finally {
            operation.close();
        }
    }

    /** Shared bootstrap backend used by the query HA registry. */
    public ExecutionAnchorStore backend() { return backend; }
    /** Actual configured node identifier. */
    public String coordinatorId() { return coordinatorId; }
    /** Actual single startup identity shared across roles. */
    public String incarnation() { return incarnation; }

    Duration leaseDuration() { return leaseDuration; }

    void bindQueryOwnership(QueryOwnershipRegistry registry) {
        if (!registry.usesStore(backend)) {
            throw new IllegalArgumentException("Query ownership requires the exact process backend");
        }
        if (!queryOwnership.compareAndSet(null, registry) && queryOwnership.get() != registry) {
            throw new IllegalStateException("Process authority already has a query ownership registry");
        }
    }

    void unbindQueryOwnership(QueryOwnershipRegistry registry) {
        queryOwnership.compareAndSet(registry, null);
    }

    private void requireProcess(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor");
        if (!coordinatorId.equals(anchor.ownerId()) || !incarnation.equals(anchor.ownerIncarnation())) {
            throw new IllegalArgumentException("anchor belongs to another Coordinator process");
        }
    }

    private void renewSafely() {
        ExecutionAuthorityOperations.Operation operation;
        try {
            operation = operations.enter();
        } catch (IllegalStateException sealed) {
            return;
        }
        try {
            QueryOwnershipRegistry registry = queryOwnership.get();
            var expected = registry == null ? java.util.List.<ExecutionAnchor>of()
                    : registry.renewalSnapshot(coordinatorId, incarnation);
            var receipt = backend.renewOwned(coordinatorId, incarnation, leaseDuration);
            if (registry != null && !registry.applyRenewal(expected, receipt)) {
                throw new IllegalStateException("Process renewal lost one or more exact query fences");
            }
        } catch (RuntimeException failure) {
            LOG.error("event=execution_authority_renewal_failed coordinatorId={}", coordinatorId, failure);
        } finally {
            operation.close();
        }
    }

    /** Stops renewal and yields, rather than deletes, unfinished executions for exact-token recovery. */
    @Override
    public void close() {
        operations.seal();
        CompletableFuture<Void> completion;
        boolean claimed;
        synchronized (lifecycle) {
            if (closer == Thread.currentThread()) throw new IllegalStateException("Authority close cannot join itself");
            claimed = closer == null && (closeExit == null || closeExit.isCompletedExceptionally());
            if (claimed) {
                closeExit = new CompletableFuture<>();
                closer = Thread.currentThread();
            }
            completion = closeExit;
        }
        if (!claimed) {
            completion.join();
            return;
        }
        try {
            // Actual scheduler and synchronous backend operations exit before the final exact-process yield.
            ExecutorDrain.shutdownAndAwait(renewal, SHUTDOWN_TIMEOUT_MS);
            operations.awaitExit(SHUTDOWN_TIMEOUT_MS);
            backend.yieldOwned(coordinatorId, incarnation);
            completion.complete(null);
        } catch (RuntimeException | Error failure) {
            completion.completeExceptionally(failure);
            throw failure;
        } finally {
            synchronized (lifecycle) {
                closer = null;
            }
        }
    }
}
