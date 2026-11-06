package org.pangrova.coordinator.query.execution;

import org.pangrova.connector.spi.metadata.ConnectorMetadataScope;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.JdbcShuffleCleanupIntentStore;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager;
import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
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
    private final SplitAssignmentManager splitAssignmentManager;
    private final TaskCancelRetrier cancelRetrier;
    private final AtomicReference<OwnershipSnapshot> ownershipSnapshot = new AtomicReference<>();
    private final ConcurrentHashMap<String, ConnectorMetadataScope> queryAuthorities =
            new ConcurrentHashMap<>();
    private volatile JdbcShuffleCleanupIntentStore shuffleCleanupIntents;
    private volatile ShuffleLifecycleManager shuffleLifecycleManager;

    /** Immutable snapshot for atomic HA ownership reads. */
    record OwnershipSnapshot(
            HighAvailabilityCoordinationService service, String coordinatorId) {}

    /** Creates a query ownership tracker for task and connector cleanup. */
    public QueryOwnershipTracker(CoordinatorTaskTracker taskTracker,
                                 WorkerManager workerManager,
                                 CatalogRegistry catalogRegistry,
                                 SplitAssignmentManager splitAssignmentManager,
                                 TaskCancelRetrier cancelRetrier) {
        this.taskTracker = checkNotNull(taskTracker, "taskTracker must not be null");
        checkNotNull(workerManager, "workerManager must not be null");
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
        cleanupLocalQuery(queryId);
        cleanupExecutionOwnership(queryId);
    }

    /** Drains only this process's creation owner; no execution anchor grant is implied. */
    public void cleanupLocalQuery(String queryId) {
        taskTracker.sealQueryIssuance(queryId);
        taskTracker.requireQueryExecutionDrained(queryId);
        notifyConnectorsCleanupQuery(queryId);
        if (splitAssignmentManager != null) {
            splitAssignmentManager.cleanup(queryId);
        }
    }

    private void cleanupExecutionOwnership(String queryId) {
        JdbcShuffleCleanupIntentStore intents = shuffleCleanupIntents;
        if (intents != null) {
            // QueryStateMachine writes the durable intent before its terminal
            // transition. A retry must only consume that intent: recreating it
            // after remote cleanup atomically deleted the scope and intent
            // would make cleanup loop forever.
            if (intents.isPending(queryId)) {
                if (shuffleLifecycleManager instanceof CoordinatorShuffleLifecycleManager remote) {
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
            snapshot.service().releaseQueryOwnership(queryId, snapshot.coordinatorId());
        }
    }

    /** Cancels all dispatched tasks for a query. */
    public void cancelDispatchedTasks(String queryId) {
        for (var issued : taskTracker.sealQueryIssuance(queryId)) {
            if (issued.drainReceipt().isPresent()) continue;
            WorkerInfo worker = checkNotNull(issued.worker(), "Issued task has no exact dispatch target");
            String taskId = issued.request().taskRequest().getAttempt().getTaskId();
            cancelRetrier.cancelWithRetry(queryId, taskId, worker.host(), worker.httpPort());
        }
    }

    public TaskCancelRetrier cancelRetrier() {
        return cancelRetrier;
    }

    void registerConnectorAuthority(org.pangrova.connector.spi.metadata.ConnectorMetadataScope authority) {
        var previous = queryAuthorities.putIfAbsent(authority.queryId(), authority);
        if (previous != null && previous != authority) {
            throw new IllegalStateException("Query cleanup already belongs to another connector lifetime");
        }
    }

    private void notifyConnectorsCleanupQuery(String queryId) {
        var authority = queryAuthorities.get(queryId);
        if (authority == null) return;
        authority.cleanupMetadata().requireSuccess();
        queryAuthorities.remove(queryId, authority);
    }
}
