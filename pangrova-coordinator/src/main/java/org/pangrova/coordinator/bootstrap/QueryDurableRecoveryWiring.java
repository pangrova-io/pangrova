package org.pangrova.coordinator.bootstrap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.pangrova.config.ShuffleMode;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.coordinator.query.task.write.WriteCommitRecoveryService;
import org.pangrova.shuffle.api.ownership.ShuffleExecutionOwner;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns durable shuffle cleanup and write-commit recovery wiring for queries. */
final class QueryDurableRecoveryWiring implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(QueryDurableRecoveryWiring.class);
    private static final int CLEANUP_PAGE_SIZE = 256;
    private static final long CALLBACK_DRAIN_TIMEOUT_MS = 5_000;

    private final CoordinatorBootstrapConfig config;
    private final DataSource metadataDataSource;
    private final AtomicReference<HighAvailabilityCoordinationService> liveCoordination =
            new AtomicReference<>();
    private final AtomicBoolean scanRunning = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final org.pangrova.coordinator.bootstrap.lifecycle.CoordinatorCallbackLifetime callbacks =
            new org.pangrova.coordinator.bootstrap.lifecycle.CoordinatorCallbackLifetime();
    private final Set<String> cleanupInFlight = ConcurrentHashMap.newKeySet();
    private final Object schedulingAdmission = new Object();

    private JdbcShuffleCleanupIntentStore shuffleCleanupIntents;
    private QueryDispatcher queryDispatcher;
    private volatile ScheduledFuture<?> cleanupFuture;
    private Supplier<List<String>> cleanupBatchProvider;
    private Consumer<String> cleanupAction;
    /** Only the single-flight scan worker advances this keyset cursor. */
    private String cleanupCursor;

    QueryDurableRecoveryWiring(
            CoordinatorBootstrapConfig config,
            DataSource metadataDataSource,
            HighAvailabilityCoordinationService initialCoordination) {
        this.config = config;
        this.metadataDataSource = metadataDataSource;
        liveCoordination.set(initialCoordination);
    }

    static QueryDurableRecoveryWiring withCleanupSource(
            CoordinatorBootstrapConfig config,
            Supplier<List<String>> cleanupBatchProvider,
            Consumer<String> cleanupAction) {
        QueryDurableRecoveryWiring wiring = new QueryDurableRecoveryWiring(
                config, null, null);
        wiring.cleanupBatchProvider = java.util.Objects.requireNonNull(
                cleanupBatchProvider, "cleanupBatchProvider");
        wiring.cleanupAction = java.util.Objects.requireNonNull(cleanupAction, "cleanupAction");
        return wiring;
    }

    void installShuffleCleanup(QueryManager queryManager, QueryDispatcher dispatcher) {
        if (config.shuffleMode() != ShuffleMode.REMOTE) {
            return;
        }
        if (metadataDataSource == null) {
            throw new IllegalStateException(
                    "remote Shuffle requires shared PostgreSQL execution authority");
        }
        shuffleCleanupIntents = new JdbcShuffleCleanupIntentStore(
                metadataDataSource, this::resolveShuffleOwner);
        queryManager.setShuffleCleanupPending(shuffleCleanupIntents::markPending);
        dispatcher.setShuffleCleanupIntents(shuffleCleanupIntents);
        queryDispatcher = dispatcher;
        cleanupBatchProvider = () -> {
            List<String> batch = shuffleCleanupIntents.pendingForOwnerAfter(
                    config.coordinatorId(), cleanupCursor, CLEANUP_PAGE_SIZE);
            if (batch.isEmpty() && cleanupCursor != null) {
                cleanupCursor = null;
                batch = shuffleCleanupIntents.pendingForOwnerAfter(
                        config.coordinatorId(), null, CLEANUP_PAGE_SIZE);
            }
            if (!batch.isEmpty()) {
                cleanupCursor = batch.get(batch.size() - 1);
            }
            return batch;
        };
        cleanupAction = queryId -> {
            queryDispatcher.retryPendingShuffleCleanup(queryId);
            if (!shuffleCleanupIntents.isPending(queryId)) {
                queryDispatcher.cleanupQuery(queryId);
            }
        };
    }

    void wireHighAvailability(
            HighAvailabilityCoordinationService service,
            Supplier<ScheduledExecutorService> scheduler,
            Executor cleanupExecutor,
            long workerHealthCheckIntervalMs) {
        try (var operation = callbacks.enter()) {
            operation.run(() -> installSchedule(service, scheduler, cleanupExecutor, workerHealthCheckIntervalMs));
        }
    }

    private void installSchedule(HighAvailabilityCoordinationService service,
            Supplier<ScheduledExecutorService> scheduler, Executor cleanupExecutor, long intervalMs) {
        if (closed.get()) throw new IllegalStateException("durable recovery is closed");
        ScheduledFuture<?> candidate = cleanupBatchProvider == null ? null : scheduler.get().scheduleWithFixedDelay(
                () -> submitShuffleCleanupScan(cleanupExecutor), 0, intervalMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> previous;
        boolean rejected;
        synchronized (schedulingAdmission) {
            rejected = closed.get();
            previous = cleanupFuture;
            if (!rejected) {
                liveCoordination.set(service);
                cleanupFuture = candidate;
            }
        }
        if (rejected) {
            if (candidate != null) candidate.cancel(false);
            throw new IllegalStateException("durable recovery closed during schedule installation");
        }
        if (previous != null) previous.cancel(false);
    }

    @Override
    public void close() {
        ScheduledFuture<?> future;
        synchronized (schedulingAdmission) {
            closed.set(true);
            future = cleanupFuture;
        }
        if (future != null) {
            future.cancel(false);
        }
        callbacks.close(CALLBACK_DRAIN_TIMEOUT_MS);
    }

    ShuffleExecutionOwner resolveShuffleOwner(String queryId) {
        HighAvailabilityCoordinationService service = liveCoordination.get();
        if (service == null) {
            throw new IllegalStateException("shuffle execution authority is not initialized");
        }
        var anchor = service.requireExecutionAnchor(queryId);
        return new ShuffleExecutionOwner(
                anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
    }

    Set<String> recover(
            List<ExecutionAnchor> claimed,
            HighAvailabilityCoordinationService service,
            WriteCommitRecoveryService writeCommitRecoveryService) {
        if (claimed.isEmpty()) {
            return Set.of();
        }
        try (var operation = callbacks.enter()) {
            java.util.concurrent.atomic.AtomicReference<Set<String>> result =
                    new java.util.concurrent.atomic.AtomicReference<>();
            operation.run(() -> result.set(recoverEntered(claimed, service, writeCommitRecoveryService)));
            return result.get();
        }
    }

    private Set<String> recoverEntered(List<ExecutionAnchor> claimed, HighAvailabilityCoordinationService service,
            WriteCommitRecoveryService writeCommitRecoveryService) {
        liveCoordination.set(service);
        if (writeCommitRecoveryService != null) {
            Map<String, Long> tokens = claimed.stream()
                    .collect(Collectors.toUnmodifiableMap(
                            ExecutionAnchor::executionId,
                            ExecutionAnchor::fencingToken));
            writeCommitRecoveryService.recover(tokens,
                    service::validateDurableExecutionToken);
        }
        return recoverShuffleCleanupIntents(claimed);
    }

    private Set<String> recoverShuffleCleanupIntents(List<ExecutionAnchor> claimed) {
        if (shuffleCleanupIntents == null) {
            return Set.of();
        }
        Set<String> cleaned = new HashSet<>();
        for (ExecutionAnchor anchor : claimed) {
            String queryId = anchor.executionId();
            if (!shuffleCleanupIntents.isPending(queryId)) {
                continue;
            }
            queryDispatcher.retryPendingShuffleCleanup(queryId);
            if (shuffleCleanupIntents.isPending(queryId)) {
                throw new IllegalStateException(
                        "recovered shuffle cleanup is still pending: " + queryId);
            }
            cleaned.add(queryId);
        }
        return Set.copyOf(cleaned);
    }

    private void submitShuffleCleanupScan(Executor cleanupExecutor) {
        if (closed.get() || !scanRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            var operation = callbacks.enter();
            try {
                cleanupExecutor.execute(() -> operation.run(() -> {
                    try {
                        retryOwnedShuffleCleanups(cleanupExecutor);
                    } finally {
                        scanRunning.set(false);
                    }
                }));
            } catch (RuntimeException | Error failure) {
                operation.close();
                throw failure;
            }
        } catch (RejectedExecutionException rejected) {
            scanRunning.set(false);
            LOG.warn("event=shuffle_cleanup_scan_rejected error={}", rejected.getMessage());
        } catch (RuntimeException | Error failure) {
            scanRunning.set(false);
            if (!closed.get()) throw failure;
        }
    }

    private void retryOwnedShuffleCleanups(Executor cleanupExecutor) {
        try {
            for (String queryId : cleanupBatchProvider.get()) {
                if (closed.get()) {
                    return;
                }
                if (!submitShuffleCleanup(cleanupExecutor, queryId)) {
                    return;
                }
            }
        } catch (RuntimeException failure) {
            LOG.error("event=shuffle_cleanup_scan_failed error={}",
                    failure.getMessage(), failure);
        }
    }

    private boolean submitShuffleCleanup(Executor cleanupExecutor, String queryId) {
        if (!cleanupInFlight.add(queryId)) {
            return true;
        }
        try {
            var operation = callbacks.enter();
            try {
                cleanupExecutor.execute(() -> operation.run(() -> runShuffleCleanup(queryId)));
            } catch (RuntimeException | Error failure) {
                operation.close();
                throw failure;
            }
            return true;
        } catch (RejectedExecutionException rejected) {
            cleanupInFlight.remove(queryId);
            // The intent remains durable; retry next tick instead of growing
            // an unbounded in-memory queue.
            LOG.warn("event=shuffle_cleanup_queue_full queryId={}", queryId);
            return false;
        } catch (RuntimeException | Error failure) {
            cleanupInFlight.remove(queryId);
            if (!closed.get()) throw failure;
            return false;
        }
    }

    private void runShuffleCleanup(String queryId) {
        try {
            if (!closed.get()) {
                cleanupAction.accept(queryId);
            }
        } catch (RuntimeException failure) {
            LOG.warn("event=shuffle_cleanup_retry_failed queryId={} error={}",
                    queryId, failure.getMessage(), failure);
        } finally {
            cleanupInFlight.remove(queryId);
        }
    }
}
