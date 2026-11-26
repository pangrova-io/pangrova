package org.pangrova.coordinator.query.execution;

import org.pangrova.query.lifecycle.QueryExecutionPort.Ownership;
import java.util.OptionalLong;
import org.pangrova.connector.api.metadata.ConnectorMetadataScope;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.query.api.lifecycle.ShuffleCleanupIntentStore;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.shuffle.api.ShuffleCleanupRecovery;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
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
    private final TaskCancelRetrier cancelRetrier;
    private final AtomicReference<OwnershipSnapshot> ownershipSnapshot = new AtomicReference<>();
    private final ConcurrentHashMap<String, ConnectorMetadataScope> queryAuthorities =
            new ConcurrentHashMap<>();
    private volatile CleanupBinding shuffleCleanup;

    private record CleanupBinding(ShuffleCleanupIntentStore intents, ShuffleCleanupRecovery recovery) {}

    /** Immutable snapshot for atomic HA ownership reads. */
    record OwnershipSnapshot(
            HighAvailabilityCoordinationService service, String coordinatorId) {}

    /** Creates a query ownership tracker for task and connector cleanup. */
    public QueryOwnershipTracker(CoordinatorTaskTracker taskTracker,
                                 WorkerManager workerManager,
                                 CatalogRegistry catalogRegistry,
                                 TaskCancelRetrier cancelRetrier) {
        this.taskTracker = checkNotNull(taskTracker, "taskTracker must not be null");
        checkNotNull(workerManager, "workerManager must not be null");
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
            ShuffleCleanupIntentStore intents,
            ShuffleCleanupRecovery recovery) {
        shuffleCleanup = new CleanupBinding(checkNotNull(intents, "intents must not be null"),
                checkNotNull(recovery, "recovery must not be null"));
    }

    /** Acquires the query grant from the configured ownership backend. */
    public Ownership acquireQueryOwnership(String queryId, String tenantId) {
        return acquireQueryOwnership(queryId, tenantId, java.util.OptionalInt.empty());
    }

    /** Acquires fresh ownership and an authorized durable slot as one grant when capacity is supplied. */
    Ownership acquireQueryOwnership(String queryId, String tenantId, java.util.OptionalInt capacity) {
        OwnershipSnapshot snapshot = ownershipSnapshot.get();
        if (snapshot == null) {
            if (capacity.isPresent()) throw new IllegalStateException("Durable query admission requires ownership");
            return new Ownership(true, OptionalLong.empty());
        }
        try {
            var acquired = snapshot.service().acquireQueryOwnership(queryId, snapshot.coordinatorId(),
                    tenantId, capacity);
            if (acquired.isEmpty()) {
                LOG.warn("event=query_ownership_denied queryId={} coordinatorId={}",
                        queryId, snapshot.coordinatorId());
                return new Ownership(false, OptionalLong.empty());
            }
            var grant = acquired.orElseThrow();
            return new Ownership(true, grant.publicationRevision(), grant.anchor());
        } catch (org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore.TenantCapacityExceeded denied) {
            throw new org.pangrova.security.exception.TenantQuotaExceededException(denied.getMessage());
        } catch (RuntimeException failure) {
            LOG.warn("event=query_ownership_acquire_failed queryId={} coordinatorId={} error={}",
                    queryId, snapshot.coordinatorId(), failure.getMessage());
            return new Ownership(false, OptionalLong.empty());
        }
    }

    /** Releases a candidate grant without claiming or cleaning a local issued query. */
    public void releaseQueryOwnership(String queryId) {
        OwnershipSnapshot snapshot = ownershipSnapshot.get();
        if (snapshot != null) {
            snapshot.service().releaseQueryOwnership(queryId, snapshot.coordinatorId());
        }
    }

    void releaseIssuedQueryOwnership(ExecutionAnchor anchor) {
        OwnershipSnapshot snapshot = ownershipSnapshot.get();
        if (snapshot != null) snapshot.service().releaseRecoveryClaim(anchor);
    }

    /** Drains split, connector and Shuffle owners while retaining the execution grant. */
    public boolean tryCleanupQuery(String queryId, ExecutionAnchor originalGrant) {
        if (!tryCleanupLocalQuery(queryId)) return false;
        cleanupShuffle(queryId, originalGrant);
        return true;
    }

    /** Drains only this process's creation owner; no execution anchor grant is implied. */
    public boolean tryCleanupLocalQuery(String queryId) {
        if (!taskTracker.tryClaimQueryCleanup(queryId)) return false;
        notifyConnectorsCleanupQuery(queryId);
        return true;
    }

    private void cleanupShuffle(String queryId, ExecutionAnchor grant) {
        CleanupBinding cleanup = shuffleCleanup;
        if (cleanup == null) return;
        checkNotNull(grant, "Remote Shuffle cleanup requires its original query grant");
        var ownership = checkNotNull(ownershipSnapshot.get(), "Remote Shuffle cleanup requires ownership");
        if (!queryId.equals(grant.executionId())) {
            throw new IllegalArgumentException("Shuffle cleanup requires the original query identity");
        }
        if (!ownership.service().validateExecutionToken(queryId, grant.fencingToken())) return;

        var owner = new ShuffleExecutionOwner(grant.ownerId(), grant.ownerIncarnation(), grant.fencingToken());
        if (cleanup.recovery().cleanupTrackedShuffle(queryId, owner)) return;
        if (!cleanup.intents().completeWithoutScope(grant)) {
            cleanup.recovery().recoverCleanupShuffle(queryId, owner);
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

    void registerConnectorAuthority(org.pangrova.connector.api.metadata.ConnectorMetadataScope authority) {
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
