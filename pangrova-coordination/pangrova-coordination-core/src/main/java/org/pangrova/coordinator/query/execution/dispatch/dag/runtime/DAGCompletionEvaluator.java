package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.query.core.task.StageDispatchRecord;

import org.pangrova.query.api.stage.StageState;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.core.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.handler.DispatchCanceller;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates DAG completion state: determines when all stages are finished,
 * resolves the final dispatch result, and handles timeout awaiting.
 */
public final class DAGCompletionEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(DAGCompletionEvaluator.class);

    private DAGCompletionEvaluator() {}

    /** Returns whether every stage reached FINISHED. */
    public static boolean allStagesFinished(StageDAG dag) {
        return dag.stageEntities().stream()
                .allMatch(s -> s.stateMachine().getState()
                        == StageState.FINISHED);
    }

    public static Set<Integer> collectActiveStageIds(StageDAG dag) {
        Set<Integer> ids = new HashSet<>();
        for (Stage s : dag.stageEntities()) {
            StageState st = s.stateMachine().getState();
            // SCHEDULING does not yet have producer task records.  A downstream
            // exchange can only bind sources after dispatch reaches RUNNING.
            if (st == StageState.RUNNING
                    || st == StageState.FLUSHING
                    || st == StageState.FINISHED) {
                ids.add(s.stageId());
            }
        }
        return ids;
    }

    /** Completes a query with its output-stage dispatch result. */
    public static void completeWithLastResult(
            ConcurrentHashMap<Integer, DispatchResult> results,
            StageDAG dag, CompletableFuture<DispatchResult> completion,
            List<StageDispatchRecord> dispatched) {
        int rootId = dag.stageEntities().stream()
                .filter(s -> "OUTPUT".equals(s.plan().taskType()))
                .mapToInt(Stage::stageId)
                .findFirst()
                .orElseGet(() -> {
                    int fallback = findSinkStageId(dag);
                    LOG.warn("event=output_stage_not_found fallbackStageId={} "
                            + "reason=no_stage_with_OUTPUT_taskType", fallback);
                    return fallback;
                });
        DispatchResult r = results.get(rootId);
        if (r != null) {
            completion.complete(r);
        } else if (!results.isEmpty()) {
            completion.complete(results.values().iterator().next());
        } else {
            completion.completeExceptionally(
                    new PangrovaException(PangrovaErrorCode.RUNTIME_TASK_FAILED,
                            "All stages finished but no dispatch result collected"));
        }
    }

    static int findSinkStageId(StageDAG dag) {
        Set<Integer> hasDownstream = new HashSet<>();
        for (List<Integer> upstreams : dag.edges().values()) {
            hasDownstream.addAll(upstreams);
        }
        // Collect all sink stages (no downstream consumers). Deterministically
        // pick the lowest stageId to ensure consistent result selection.
        return dag.stageEntities().stream()
                .mapToInt(Stage::stageId)
                .filter(id -> !hasDownstream.contains(id))
                .min()
                .orElse(dag.stageEntities().stream()
                        .mapToInt(Stage::stageId).max().orElse(0));
    }

    public static DispatchResult awaitCompletion(
            CompletableFuture<DispatchResult> completion,
            long timeoutMs, String queryId) {
        try {
            return completion.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var failure = new PangrovaException(PangrovaErrorCode.COORDINATOR_DISPATCH_INTERRUPTED,
                    "DAG dispatch interrupted [queryId=" + queryId + "]", interrupted);
            completion.completeExceptionally(failure);
            throw failure;
        } catch (java.util.concurrent.TimeoutException timeout) {
            var failure = new PangrovaException(PangrovaErrorCode.RUNTIME_QUERY_TIMEOUT,
                    "DAG dispatch timed out [queryId=" + queryId + "]", timeout);
            completion.completeExceptionally(failure);
            throw failure;
        } catch (java.util.concurrent.ExecutionException failed) {
            Throwable cause = failed.getCause();
            throw DispatchCanceller.wrapIfNeeded(
                    cause instanceof Exception ex ? ex
                            : new RuntimeException(cause), queryId);
        }
    }

    /** Finds the dispatch record for a task, or null when it is absent. */
    public static StageDispatchRecord findDispatchRecord(
            String taskId, List<StageDispatchRecord> dispatched) {
        for (StageDispatchRecord rec : dispatched) {
            if (rec.result().taskId().equals(taskId)) {
                return rec;
            }
        }
        return null;
    }
}
