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
     * Graceful shutdown: drain, finish, then yield durable execution anchors.
     *
     * <p><b>R10.3 Timing dependency:</b> The ordering of shutdown phases is critical:
     * <ol>
     *   <li>{@code drain()} — sets {@code acceptingQueries=false} to reject new queries</li>
     *   <li>{@code finishInFlight()} — waits up to {@link #SHUTDOWN_TIMEOUT_MS} for running queries</li>
     *   <li>yield live execution anchors for bounded fenced takeover</li>
     * </ol>
     * The drain must happen before anchors are yielded. The existing protection is the
     * {@code running} CAS guard at the top of this method — once {@code running} is false,
     * no new queries can be submitted via the HTTP layer (QueryManager rejects them).
     * Durable anchors already contain exact process identity and fencing tokens, so a
     * takeover never depends on best-effort orphan-row creation.</p>
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        drain();
        finishInFlight();
        yieldExecutionAnchors();
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

    private void yieldExecutionAnchors() {
        Set<String> activeIds = queryManager.getActiveQueryIds();
        int yielded = haService.yieldLocalExecutions();
        LOG.info("event=execution_anchors_yielded coordinatorId={} "
                        + "activeQueries={} yielded={}",
                coordinatorId, activeIds.size(), yielded);
    }
}
