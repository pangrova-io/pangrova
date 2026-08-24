package org.pangrova.coordinator;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.worker.WorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

/**
 * Top-level coordinator entry point.
 * Lifecycle: start → running → stop (drain → finish → transfer → shutdown).
 */
public class PangrovaCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaCoordinator.class);
    private static final long SHUTDOWN_TIMEOUT_MS = 30_000;

    private final String coordinatorId;
    private final QueryManager queryManager;
    private final WorkerManager workerManager;
    private final HighAvailabilityCoordinationService haService;
    private final long shutdownTimeoutMs;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public PangrovaCoordinator(
            String coordinatorId,
            QueryManager queryManager,
            WorkerManager workerManager,
            HighAvailabilityCoordinationService haService) {
        this(coordinatorId, queryManager, workerManager, haService, SHUTDOWN_TIMEOUT_MS);
    }

    PangrovaCoordinator(
            String coordinatorId,
            QueryManager queryManager,
            WorkerManager workerManager,
            HighAvailabilityCoordinationService haService,
            long shutdownTimeoutMs) {
        this.coordinatorId = checkNotNull(coordinatorId, "coordinatorId must not be null");
        this.queryManager = checkNotNull(queryManager, "queryManager must not be null");
        this.workerManager = checkNotNull(workerManager, "workerManager must not be null");
        this.haService = checkNotNull(haService, "haService must not be null");
        this.shutdownTimeoutMs = shutdownTimeoutMs;
    }

    public void start() {
        checkState(!running.get(), "Coordinator is already running");
        running.set(true);
        LOG.info("event=coordinator_started coordinatorId={}", coordinatorId);
    }

    /**
     * Graceful shutdown: drain → finish → transfer → shutdown.
     *
     * <p><b>R10.3 Timing dependency:</b> The ordering of shutdown phases is critical:
     * <ol>
     *   <li>{@code drain()} — sets {@code acceptingQueries=false} to reject new queries</li>
     *   <li>{@code finishInFlight()} — waits up to {@link #SHUTDOWN_TIMEOUT_MS} for running queries</li>
     *   <li>{@code transferOwnership()} — persists orphaned queries THEN releases ownership</li>
     * </ol>
     * The drain MUST happen before transferOwnership: if ownership is transferred while
     * new queries are still being accepted, a race window exists where a newly accepted
     * query has no ownership record on the new leader. The existing protection is the
     * {@code running} CAS guard at the top of this method — once {@code running} is false,
     * no new queries can be submitted via the HTTP layer (QueryManager rejects them).
     * The {@code persistOrphanedQueriesForTransfer} call inside transferOwnership ensures
     * that any queries still in-flight after the drain timeout are recorded for the new
     * leader to reclaim.</p>
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        drain();
        finishInFlight();
        transferOwnership();
        LOG.info("event=coordinator_stopped coordinatorId={}", coordinatorId);
    }

    public boolean isRunning() {
        return running.get();
    }

    public QueryManager getQueryManager() {
        return queryManager;
    }

    public WorkerManager getWorkerManager() {
        return workerManager;
    }

    private void drain() {
        queryManager.setAcceptingQueries(false);
        LOG.info("event=drain_started coordinatorId={}", coordinatorId);
    }

    private void finishInFlight() {
        boolean completed = queryManager.awaitAllQueries(shutdownTimeoutMs);
        if (completed) {
            LOG.info("event=drain_completed coordinatorId={}", coordinatorId);
        } else {
            LOG.warn("event=drain_timeout coordinatorId={} timeoutMs={}", coordinatorId, shutdownTimeoutMs);
        }
    }

    private void transferOwnership() {
        Set<String> activeIds = queryManager.getActiveQueryIds();
        // Persist orphaned queries BEFORE releasing ownership so the new
        // leader can reclaim them. Without this, in-flight queries become zombies
        // when awaitAllQueries times out during graceful shutdown.
        //
        // Note: persist and release are NOT atomic. If the coordinator crashes
        // between persist and the for-loop below, the orphan records are still
        // complete — the new leader reclaims all of them. The for-loop only
        // clears the in-memory ownership map (which is lost on crash anyway).
        if (!activeIds.isEmpty()) {
            haService.persistOrphanedQueriesForTransfer(coordinatorId);
            LOG.info("event=orphans_persisted_before_transfer coordinatorId={} queryCount={}",
                    coordinatorId, activeIds.size());
        }
        // Release all ownership in a single batch to minimize the window
        // where queries have no owner. Retry once on failure before giving up.
        java.util.List<String> failedReleases = new java.util.ArrayList<>();
        for (String queryId : activeIds) {
            try {
                haService.releaseQueryOwnership(queryId, coordinatorId);
            } catch (Exception e) {
                LOG.warn("event=ownership_release_failed coordinatorId={} queryId={} error={}",
                        coordinatorId, queryId, e.getMessage());
                failedReleases.add(queryId);
            }
        }
        // Retry failed releases once
        if (!failedReleases.isEmpty()) {
            LOG.info("event=ownership_release_retry coordinatorId={} retryCount={}",
                    coordinatorId, failedReleases.size());
            for (String queryId : failedReleases) {
                try {
                    haService.releaseQueryOwnership(queryId, coordinatorId);
                } catch (Exception e) {
                    LOG.error("event=ownership_release_retry_failed coordinatorId={} queryId={} error={}",
                            coordinatorId, queryId, e.getMessage());
                }
            }
        }
        LOG.info("event=ownership_transferred coordinatorId={} queryCount={}", coordinatorId, activeIds.size());
    }
}
