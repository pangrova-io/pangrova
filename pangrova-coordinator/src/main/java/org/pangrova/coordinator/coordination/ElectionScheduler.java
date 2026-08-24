package org.pangrova.coordinator.coordination;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the {@link ScheduledExecutorService} and all periodic scheduling logic
 * for HA leader election: lease renewal (leader) and failure detection (standby).
 *
 * <p>Extracted from {@link HighAvailabilityCoordinationService} to keep that
 * class under the 500-line governance limit. Communicates back to the service
 * via the {@link Callback} interface.</p>
 */
@ThreadSafe
class ElectionScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ElectionScheduler.class);

    /**
     * Callback interface for the coordination service to receive scheduler events.
     * Keeps the scheduler decoupled from service internals.
     */
    interface Callback {
        /** Attempts to renew the leader lease. Returns true on success. */
        boolean renewLease(String coordinatorId);

        /** Attempts to become leader. Returns true on success. */
        boolean tryBecomeLeader(String coordinatorId);

        /** Returns the current leader id, if any. */
        Optional<String> currentLeader();

        /** Called when lease renewal fails and the node must demote to standby. */
        void onLeaseRenewalFailed(String coordinatorId);

        /** Called on each successful heartbeat tick (leader or standby). */
        void onHeartbeatTick(String coordinatorId);
    }

    private final HighAvailabilityCoordinationService.CoordinationConfig config;
    private final AtomicBoolean running;
    private final Callback callback;
    private volatile ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    ElectionScheduler(HighAvailabilityCoordinationService.CoordinationConfig config,
                        AtomicBoolean running,
                        Callback callback) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.running = Objects.requireNonNull(running, "running must not be null");
        this.callback = Objects.requireNonNull(callback, "callback must not be null");
    }

    /** Creates the underlying executor. Must be called before scheduling. */
    void start() {
        scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("ha-election");
    }

    void scheduleLeaseRenewal(String coordinatorId) {
        // BUG-D13 fix: guard against scheduling on a shutting-down executor
        if (!running.get()) {
            return;
        }
        scheduledTask = scheduler.scheduleAtFixedRate(
                () -> handleLeaseRenewal(coordinatorId),
                config.heartbeatIntervalMs(),
                config.heartbeatIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    private void handleLeaseRenewal(String coordinatorId) {
        if (!running.get()) {
            return;
        }
        try {
            if (!callback.renewLease(coordinatorId)) {
                LOG.warn("event=lease_renewal_failed coordinator={} action=demoting_to_standby", coordinatorId);
                callback.onLeaseRenewalFailed(coordinatorId);
                cancelScheduledTask();
                scheduleFailureDetection(coordinatorId);
            } else {
                callback.onHeartbeatTick(coordinatorId);
            }
        } catch (Exception e) {
            // Critical: ScheduledExecutorService silently cancels future executions
            // if the Runnable throws an exception. Treat any unexpected error
            // (network partition, JDBC timeout, clock skew) as a renewal failure
            // to prevent this node from becoming a zombie leader.
            LOG.error("event=lease_renewal_exception coordinator={} action=demoting_to_standby error={}",
                    coordinatorId, e.getMessage(), e);
            try {
                callback.onLeaseRenewalFailed(coordinatorId);
            } catch (Exception inner) {
                LOG.error("event=lease_renewal_demotion_failed coordinator={} error={}",
                        coordinatorId, inner.getMessage(), inner);
            }
            cancelScheduledTask();
            scheduleFailureDetection(coordinatorId);
        }
    }

    void scheduleFailureDetection(String coordinatorId) {
        // BUG-D13 fix: guard against scheduling on a shutting-down executor
        if (!running.get()) {
            return;
        }
        scheduledTask = scheduler.scheduleAtFixedRate(
                () -> handleFailureDetection(coordinatorId),
                config.heartbeatIntervalMs(),
                config.heartbeatIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    private void handleFailureDetection(String coordinatorId) {
        if (!running.get()) {
            return;
        }
        try {
            callback.onHeartbeatTick(coordinatorId);
            Optional<String> currentLeader = callback.currentLeader();
            if (currentLeader.isEmpty()) {
                LOG.info("event=leader_failure_detected standbyNode={} action=attempting_takeover", coordinatorId);
                if (callback.tryBecomeLeader(coordinatorId)) {
                    cancelScheduledTask();
                    scheduleLeaseRenewal(coordinatorId);
                }
            } else {
                LOG.debug("event=leader_check standbyNode={} currentLeader={}",
                        coordinatorId, currentLeader.get());
            }
        } catch (Exception e) {
            // Critical: ScheduledExecutorService silently cancels future executions
            // if the Runnable throws. Swallow the exception so the standby node
            // keeps retrying failure detection on the next heartbeat interval.
            // Transient errors (network partition, JDBC timeout) should not
            // permanently disable the standby node's ability to take over.
            LOG.error("event=failure_detection_exception standbyNode={} error={}",
                    coordinatorId, e.getMessage(), e);
        }
    }

    void cancelScheduledTask() {
        ScheduledFuture<?> task = scheduledTask;
        if (task != null) {
            task.cancel(false);
            scheduledTask = null;
        }
    }

    void shutdown() {
        if (scheduler == null) {
            return;
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("event=ha_scheduler_shutdown_timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=ha_scheduler_shutdown_interrupted");
        }
    }
}
