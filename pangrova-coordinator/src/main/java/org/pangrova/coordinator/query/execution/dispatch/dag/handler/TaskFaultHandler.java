package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles task-level retry and shuffle fault recovery for DAG dispatch.
 * Determines retryability from error code prefixes, manages per-task retry
 * counters via Stage, and triggers upstream re-computation on shuffle faults.
 */
@ThreadSafe
public final class TaskFaultHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskFaultHandler.class);
    private static final Set<String> RETRYABLE_PREFIXES =
            Set.of("EXCHANGE", "TRANSPORT", "RESOURCE", "EXTERNAL");
    private static final String EXCHANGE_FETCH_FAILED_CODE = "EXCHANGE-0004";

    /** Tracks upstream stages already re-computed per query (max 1 per stage). */
    private final ConcurrentHashMap<String, Set<Integer>> recomputedUpstreams =
            new ConcurrentHashMap<>();

    /**
     * Attempts task-level retry for a failed task. Returns true if retry was
     * initiated (caller should skip stage-level failure), false otherwise.
     */
    public boolean tryTaskRetry(String queryId, String taskId, Stage stage,
                         ErrorDetail error) {
        if (!isRetryable(error)) {
            return false;
        }
        int attempt = stage.incrementTaskRetryCount(taskId);
        if (attempt > Stage.MAX_TASK_RETRIES) {
            LOG.info("event=task_retry_exhausted queryId={} taskId={}"
                    + " stageId={} attempts={}",
                    queryId, taskId, stage.stageId(), attempt);
            return false;
        }
        LOG.info("event=task_retry_eligible queryId={} taskId={}"
                + " stageId={} attempt={} errorCode={}",
                queryId, taskId, stage.stageId(), attempt,
                error != null ? error.errorCode() : "unknown");
        return true;
    }

    /**
     * Returns true if the error indicates a shuffle fetch failure
     * (upstream data unavailable).
     */
    public boolean isShuffleFetchFailure(ErrorDetail error) {
        return error != null
                && EXCHANGE_FETCH_FAILED_CODE.equals(error.errorCode());
    }

    /**
     * Attempts upstream stage re-computation for shuffle fault recovery.
     * Returns true if recovery was initiated, false if already attempted
     * or no upstream found.
     */
    public boolean tryUpstreamRecomputation(String queryId, Stage failedStage,
                                     StageDAG dag) {
        List<Integer> upstreamIds = dag.edges().getOrDefault(
                failedStage.stageId(), List.of());
        if (upstreamIds.isEmpty()) {
            return false;
        }
        Set<Integer> alreadyRecomputed = recomputedUpstreams
                .computeIfAbsent(queryId, k -> ConcurrentHashMap.newKeySet());

        // Attempt recomputation of all upstream stages (not just the first).
        // For JOIN stages with multiple upstreams, the shuffle fetch failure
        // may originate from any upstream — recompute all to ensure recovery.
        boolean anyRecovered = false;
        for (int upstreamId : upstreamIds) {
            if (!alreadyRecomputed.add(upstreamId)) {
                LOG.info("event=shuffle_recovery_skip queryId={} stageId={}"
                        + " upstreamStageId={} reason=already_recomputed",
                        queryId, failedStage.stageId(), upstreamId);
                continue;
            }
            Stage upstream = dag.findStage(upstreamId);
            if (upstream == null || !upstream.tryPrepareRetry()) {
                alreadyRecomputed.remove(upstreamId);
                continue;
            }
            upstream.stateMachine().reset();
            anyRecovered = true;
            LOG.info("event=shuffle_fault_recovery_upstream queryId={}"
                    + " failedStage={} upstreamStage={}",
                    queryId, failedStage.stageId(), upstreamId);
        }

        if (!anyRecovered) {
            return false;
        }
        failedStage.tryPrepareRetry();
        failedStage.stateMachine().reset();
        LOG.info("event=shuffle_fault_recovery queryId={} failedStage={}"
                + " upstreamCount={}",
                queryId, failedStage.stageId(), upstreamIds.size());
        return true;
    }

    /** Cleans up per-query tracking state. */
    public void clearQuery(String queryId) {
        recomputedUpstreams.remove(queryId);
    }

    private static boolean isRetryable(ErrorDetail error) {
        if (error == null || error.errorCode() == null) {
            return false;
        }
        String code = error.errorCode();
        int dash = code.indexOf('-');
        String prefix = dash > 0 ? code.substring(0, dash) : code;
        return RETRYABLE_PREFIXES.contains(prefix);
    }
}
