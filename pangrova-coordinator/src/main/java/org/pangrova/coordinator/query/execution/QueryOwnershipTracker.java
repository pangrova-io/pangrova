package org.pangrova.coordinator.query.execution;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Tracks query ownership via HA coordination and handles query lifecycle
 * cleanup: ownership release, split assignment cleanup, connector notification,
 * and dispatched task cancellation.
 *
 * <p>Thread-safe: {@code ownershipSnapshot} uses AtomicReference for atomic reads.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class QueryOwnershipTracker {

    private static final Logger LOG = LoggerFactory.getLogger(QueryOwnershipTracker.class);

    private final CoordinatorTaskTracker taskTracker;
    private final WorkerManager workerManager;
    private final CatalogRegistry catalogRegistry;
    private final SplitAssignmentManager splitAssignmentManager;
    private final TaskCancelRetrier cancelRetrier;
    private final AtomicReference<OwnershipSnapshot> ownershipSnapshot = new AtomicReference<>();
    private final ConcurrentHashMap<String, List<ConnectorMetadata>> queryMetadata = new ConcurrentHashMap<>();
    private volatile JdbcShuffleCleanupIntentStore shuffleCleanupIntents;
    private volatile ShuffleLifecycleManager shuffleLifecycleManager;

    /** Immutable snapshot for atomic HA ownership reads. */
    record OwnershipSnapshot(
            HighAvailabilityCoordinationService service, String coordinatorId) {}

    public QueryOwnershipTracker(CoordinatorTaskTracker taskTracker,
                                 WorkerManager workerManager,
                                 CatalogRegistry catalogRegistry,
                                 SplitAssignmentManager splitAssignmentManager,
                                 TaskCancelRetrier cancelRetrier) {
        this.taskTracker = checkNotNull(taskTracker, "taskTracker must not be null");
        this.workerManager = checkNotNull(workerManager, "workerManager must not be null");
        this.catalogRegistry = catalogRegistry;
        this.splitAssignmentManager = splitAssignmentManager;
        this.cancelRetrier = checkNotNull(cancelRetrier, "cancelRetrier must not be null");
    }

    /**
     * Late-binds the HA ownership tracking.
     * Called after HA bootstrap completes, since QueryDispatcher is constructed
     * before the HA service is available. Atomically updates the snapshot so
     * concurrent queries see a consistent view.
     */
    public void setOwnershipTracking(HighAvailabilityCoordinationService haService,
                                     String coordinatorId) {
        checkNotNull(haService, "haService must not be null");
        checkNotNull(coordinatorId, "coordinatorId must not be null");
        ownershipSnapshot.set(new OwnershipSnapshot(haService, coordinatorId));
        LOG.info("event=ownership_tracking_wired coordinatorId={}", coordinatorId);
    }

    /** Seeds the HA snapshot from constructor-time deps (if available). */
    void initFromContext(HighAvailabilityCoordinationService haService, String coordinatorId) {
        if (haService != null && coordinatorId != null) {
            ownershipSnapshot.set(new OwnershipSnapshot(haService, coordinatorId));
        }
    }

    /** Wires durable remote Shuffle cleanup before execution ownership release. */
    public void setShuffleCleanup(
            JdbcShuffleCleanupIntentStore intents,
            ShuffleLifecycleManager lifecycleManager) {
        shuffleCleanupIntents = checkNotNull(intents, "intents must not be null");
        shuffleLifecycleManager = checkNotNull(
                lifecycleManager, "lifecycleManager must not be null");
    }

    /**
     * Acquires query ownership via the HA coordination service.
     * Returns true if ownership was acquired or if ownership tracking is not configured.
     */
    public boolean acquireQueryOwnership(String queryId) {
        OwnershipSnapshot snapshot = ownershipSnapshot.get();
        if (snapshot == null) {
            return true;
        }
        try {
            boolean acquired = snapshot.service().acquireQueryOwnership(queryId, snapshot.coordinatorId());
            if (!acquired) {
                LOG.warn("event=query_ownership_denied queryId={} coordinatorId={}",
                        queryId, snapshot.coordinatorId());
            }
            return acquired;
        } catch (Exception e) {
            LOG.warn("event=query_ownership_acquire_failed queryId={} coordinatorId={} error={}",
                    queryId, snapshot.coordinatorId(), e.getMessage());
            return false;
        }
    }

    /** Cleans up split assignment state, releases ownership, and notifies connectors. */
    public void cleanupQuery(String queryId) {
        JdbcShuffleCleanupIntentStore intents = shuffleCleanupIntents;
        if (intents != null) {
            // QueryStateMachine writes the durable intent before its terminal
            // transition. A retry must only consume that intent: recreating it
            // after remote cleanup atomically deleted the scope and intent
            // would make cleanup loop forever.
            if (intents.isPending(queryId)) {
                if (shuffleLifecycleManager instanceof
                        org.pangrova.coordinator.query.execution.dispatch.shuffle
                                .CoordinatorShuffleLifecycleManager remote) {
                    remote.recoverCleanupShuffle(queryId);
                } else {
                    shuffleLifecycleManager.cleanupShuffle(queryId);
                }
                if (intents.isPending(queryId)) {
                    throw new IllegalStateException(
                            "shuffle cleanup is still pending: " + queryId);
                }
            }
        }
        OwnershipSnapshot snapshot = ownershipSnapshot.get();
        if (snapshot != null) {
            try {
                snapshot.service().releaseQueryOwnership(queryId, snapshot.coordinatorId());
            } catch (Exception e) {
                LOG.debug("event=query_ownership_release_failed queryId={} coordinatorId={} error={}",
                        queryId, snapshot.coordinatorId(), e.getMessage());
            }
        }
        if (splitAssignmentManager != null) {
            splitAssignmentManager.cleanup(queryId);
        }
        notifyConnectorsCleanupQuery(queryId);
    }

    /** Cancels all dispatched tasks for a query. */
    public void cancelDispatchedTasks(String queryId) {
        for (String taskId : taskTracker.getTaskIds(queryId)) {
            String workerId = taskTracker.getWorkerForTask(taskId);
            if (workerId == null) {
                continue;
            }
            WorkerInfo worker = workerManager.getWorker(workerId);
            if (worker == null) {
                continue;
            }
            cancelRetrier.cancelWithRetry(queryId, taskId, worker.host(), worker.httpPort());
        }
    }

    public TaskCancelRetrier cancelRetrier() {
        return cancelRetrier;
    }

    void registerConnectorMetadata(String queryId, ConnectorMetadata metadata) {
        queryMetadata.computeIfAbsent(queryId, ignored -> new CopyOnWriteArrayList<>()).add(metadata);
    }

    private void notifyConnectorsCleanupQuery(String queryId) {
        List<ConnectorMetadata> metadata = queryMetadata.remove(queryId);
        if (metadata == null) {
            return;
        }
        for (var connector : metadata) {
            try {
                connector.cleanupQuery(queryId);
            } catch (Exception e) {
                LOG.warn("event=connector_cleanup_query_failed queryId={} error={}",
                        queryId, e.getMessage());
            }
        }
    }
}
