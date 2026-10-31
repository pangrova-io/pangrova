package org.pangrova.coordinator;

import java.util.Set;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.worker.WorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

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
    private final org.pangrova.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission lifecycle =
            new org.pangrova.coordinator.bootstrap.lifecycle.CoordinatorLifecycleAdmission();

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
        lifecycle.beginStart();
        lifecycle.endStart(true);
        LOG.info("event=coordinator_started coordinatorId={}", coordinatorId);
    }

    /**
     * Graceful shutdown: drain, finish, then yield durable execution anchors.
     *
     * <p>The ordering of shutdown phases is critical:
     * <ol>
     *   <li>{@code drain()} — sets {@code acceptingQueries=false} to reject new queries</li>
     *   <li>{@code finishInFlight()} — waits up to {@link #SHUTDOWN_TIMEOUT_MS} for running queries</li>
     *   <li>yield live execution anchors for bounded fenced takeover</li>
     * </ol>
     * The drain must actually complete before anchors are yielded. Failed shutdown remains
     * sealed and retryable; logical termination and a timeout are not physical exit evidence.
     * Durable anchors already contain exact process identity and fencing tokens, so a
     * takeover never depends on best-effort orphan-row creation.</p>
     */
    public void stop() {
        lifecycle.close(() -> {
            drain();
            finishInFlight();
            yieldExecutionAnchors();
            LOG.info("event=coordinator_stopped coordinatorId={}", coordinatorId);
        });
    }

    public boolean isRunning() {
        return lifecycle.isRunning();
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
            throw new IllegalStateException("Coordinator still owns physical query cleanup: " + coordinatorId);
        }
    }

    private void yieldExecutionAnchors() {
        Set<String> activeIds = queryManager.getActiveQueryIds();
        int yielded = haService.yieldLocalExecutions();
        LOG.info("event=execution_anchors_yielded coordinatorId={} "
                        + "activeQueries={} yielded={}",
                coordinatorId, activeIds.size(), yielded);
    }
}
