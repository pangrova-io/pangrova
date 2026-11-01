package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.scheduler.ft.FaultTolerantStageScheduler;
import org.pangrova.coordinator.scheduler.ft.FaultTolerantStageScheduler.FailureAction;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.common.api.annotation.ThreadSafe;
import org.pangrova.scheduler.api.placement.FaultTolerancePolicy;
import org.pangrova.shuffle.spool.SpoolStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FTE-aware task failure handler that consults {@link FaultTolerantStageScheduler}
 * to decide between task-level retry (re-reading spooled partitions), stage-level
 * retry (upstream re-computation), or query failure.
 *
 * <p>This is the execution point where FTE policy decisions become concrete recovery
 * actions in the DAG dispatch engine. It extends the existing {@link TaskFaultHandler}
 * behavior by adding:
 * <ul>
 *   <li><b>Task retry with spool re-read:</b> When shuffle is durable (LOCAL_SPILL/DURABLE)
 *       and the FTE policy allows task retry, the handler signals RETRY_TASK. The DAG engine
 *       re-dispatches the failed task to a different worker, and the new task reads committed
 *       partitions from the {@link SpoolStore} rather than requiring upstream re-execution.</li>
 *   <li><b>Fallback to stage retry:</b> When task retry budget is exhausted, the handler
 *       falls back to stage-level retry (upstream re-computation), matching Spark's
 *       DAGScheduler behavior.</li>
 *   <li><b>Speculative execution consistency:</b> When a speculative copy finishes before
 *       the original, the "losing" task is cancelled. The handler skips retry for
 *       cancelled speculative tasks (they are not real failures).</li>
 * </ul>
 *
 * <h3>Industry alignment</h3>
 * <ul>
 *   <li><b>Trino FTE:</b> Task granularity retry with spooled exchange. Failed task
 *       re-dispatched to another worker; reads committed partitions from S3/local spool.
 *       Budget: {@code task-retry-attempts-per-task=4}.</li>
 *   <li><b>Spark:</b> Stage-level retry via DAGScheduler. Lost shuffle triggers full
 *       upstream re-computation. Task-level retry within a stage via
 *       {@code spark.task.maxFailures=4}.</li>
 * </ul>
 */
@ThreadSafe
public final class FteAwareTaskFaultHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FteAwareTaskFaultHandler.class);

    private final FaultTolerantStageScheduler fteScheduler;
    private final TaskFaultHandler baseFaultHandler;

    /** Tracks speculative task IDs per query to avoid retrying cancelled speculative losers. */
    private final ConcurrentHashMap<String, Set<String>> speculativeTaskIds =
            new ConcurrentHashMap<>();

    /**
     * Creates an FTE-aware fault handler.
     *
     * @param fteScheduler     the FTE scheduler for policy-based retry decisions
     * @param baseFaultHandler the existing task fault handler for error classification
     *                         and upstream re-computation
     */
    public FteAwareTaskFaultHandler(FaultTolerantStageScheduler fteScheduler,
                                    TaskFaultHandler baseFaultHandler) {
        if (fteScheduler == null) {
            throw new IllegalArgumentException("fteScheduler must not be null");
        }
        if (baseFaultHandler == null) {
            throw new IllegalArgumentException("baseFaultHandler must not be null");
        }
        this.fteScheduler = fteScheduler;
        this.baseFaultHandler = baseFaultHandler;
    }

    /**
     * Handles a task failure using the FTE policy to determine recovery action.
     *
     * <p>Decision flow:
     * <ol>
     *   <li>If the task is a cancelled speculative loser, skip retry (not a real failure).</li>
     *   <li>Consult {@link FaultTolerantStageScheduler#handleTaskFailure} for the policy decision.</li>
     *   <li>RETRY_TASK: signal the DAG engine to re-dispatch the task (consumer re-reads spooled data).</li>
     *   <li>RETRY_STAGE: signal upstream re-computation via {@link TaskFaultHandler#tryUpstreamRecomputation}.</li>
     *   <li>FAIL_QUERY: return false to let the stage fail and propagate to the query.</li>
     * </ol>
     *
     * @param queryId the query identifier
     * @param taskId  the failed task identifier
     * @param stage   the stage containing the failed task
     * @param dag     the query's stage DAG (for upstream identification in stage retry)
     * @param error   the failure error detail
     * @return the recovery action type
     */
    public RecoveryAction handleTaskFailure(String queryId, String taskId, Stage stage,
                                            StageDAG dag, ErrorDetail error) {
        if (queryId == null || taskId == null || stage == null) {
            return RecoveryAction.FAIL_QUERY;
        }

        // Skip retry for cancelled speculative copies — they are not real failures.
        if (isSpeculativeLoser(queryId, taskId)) {
            LOG.debug("event=fte_skip_speculative_loser queryId={} taskId={} stageId={}",
                    queryId, taskId, stage.stageId());
            return RecoveryAction.SKIP;
        }

        String stageId = String.valueOf(stage.stageId());
        FailureAction action = fteScheduler.handleTaskFailure(taskId, stageId);

        return switch (action.type()) {
            case RETRY_TASK -> {
                LOG.info("event=fte_task_retry queryId={} taskId={} stageId={} attempt={} backoff={}",
                        queryId, taskId, stageId, action.attemptNumber(),
                        fteScheduler.retryBudget().computeBackoff(action.attemptNumber()));
                yield RecoveryAction.RETRY_TASK;
            }
            case RETRY_STAGE -> {
                LOG.info("event=fte_stage_retry queryId={} taskId={} stageId={}",
                        queryId, taskId, stageId);
                boolean recovered = baseFaultHandler.tryUpstreamRecomputation(
                        queryId, stage, dag);
                if (recovered) {
                    yield RecoveryAction.RETRY_STAGE;
                }
                LOG.warn("event=fte_stage_retry_failed queryId={} taskId={} stageId={}"
                        + " reason=upstream_recomputation_rejected",
                        queryId, taskId, stageId);
                yield RecoveryAction.FAIL_QUERY;
            }
            case FAIL_QUERY -> {
                LOG.info("event=fte_fail_query queryId={} taskId={} stageId={} reason={}",
                        queryId, taskId, stageId, action.reason());
                yield RecoveryAction.FAIL_QUERY;
            }
        };
    }

    /**
     * Registers a task ID as speculative for a query. Speculative losers that are
     * cancelled should not be retried — they are expected cancellations, not failures.
     */
    public void registerSpeculativeTask(String queryId, String taskId) {
        speculativeTaskIds
                .computeIfAbsent(queryId, k -> ConcurrentHashMap.newKeySet())
                .add(taskId);
    }

    /** Returns the underlying FTE scheduler for policy and budget inspection. */
    public FaultTolerantStageScheduler fteScheduler() {
        return fteScheduler;
    }

    /** Returns the FTE policy. */
    public FaultTolerancePolicy policy() {
        return fteScheduler.policy();
    }

    /** Cleans up per-query tracking state. */
    public void clearQuery(String queryId) {
        speculativeTaskIds.remove(queryId);
        fteScheduler.resetCounters();
        baseFaultHandler.clearQuery(queryId);
    }

    private boolean isSpeculativeLoser(String queryId, String taskId) {
        Set<String> specTasks = speculativeTaskIds.get(queryId);
        return specTasks != null && specTasks.contains(taskId);
    }

    /**
     * Recovery action returned by {@link #handleTaskFailure}.
     */
    public enum RecoveryAction {
        /** Re-dispatch the task on a different worker; re-read spooled partitions. */
        RETRY_TASK,
        /** Re-compute upstream stages and re-dispatch this stage. */
        RETRY_STAGE,
        /** Fail the entire query — no recovery possible. */
        FAIL_QUERY,
        /** Skip processing — the failure is expected (e.g. speculative loser cancellation). */
        SKIP
    }
}
