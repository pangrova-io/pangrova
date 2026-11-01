package org.pangrova.coordinator.query.task;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.execution.dispatch.dag.StageCompletionCallback;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.scheduler.speculation.DuplicateTaskResolver;
import org.pangrova.scheduler.speculation.SpeculativeTaskLauncher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Winner/loser handling over the same issued-attempt authority and actual drain receipts. */
@ThreadSafe
final class SpeculativeTaskReportCompletion {
    private static final Logger LOG = LoggerFactory.getLogger(SpeculativeTaskReportCompletion.class);
    private final CoordinatorTaskTracker taskTracker;
    private final WorkerManager workerManager;
    private final SpeculativeTaskLauncher speculativeLauncher;
    private final DuplicateTaskResolver duplicateResolver;
    private final TaskDispatcher taskDispatcher;
    private final StageCompletionCallback stageCompletionCallback;
    private final NativeTaskWriteCompletion writes;
    private final TaskReportSequences sequences;
    private final BiConsumer<String, TaskStatusReport> evaluate;
    private final ConcurrentHashMap<String, Set<String>> speculativeFailures = new ConcurrentHashMap<>();

    SpeculativeTaskReportCompletion(TaskStatusReceiverContext deps, NativeTaskWriteCompletion writes,
            TaskReportSequences sequences, BiConsumer<String, TaskStatusReport> evaluate) {
        taskTracker = deps.taskTracker();
        workerManager = deps.workerManager();
        speculativeLauncher = deps.speculativeLauncher();
        duplicateResolver = deps.duplicateResolver();
        taskDispatcher = deps.taskDispatcher();
        stageCompletionCallback = deps.stageCompletionCallback();
        this.writes = writes;
        this.sequences = sequences;
        this.evaluate = evaluate;
    }

    void clearQuery(String queryId) {
        if (duplicateResolver != null) duplicateResolver.clearQuery(queryId);
        speculativeFailures.remove(queryId);
    }

    private void notifyStageTerminal(String queryId, String logicalTaskId, TaskStatusReport report) {
        if (stageCompletionCallback != null) {
            stageCompletionCallback.onTaskTerminal(queryId, logicalTaskId, report.state(), report.error());
        }
    }

    boolean handleSpeculativeFailure(
            String queryId, String taskId, TaskStatusReport report,
            String owningCoordinator) {
        if (!report.attempt().equals(taskTracker.getTaskAttempt(taskId))) {
            return false;
        }
        String counterpart = findCounterpartTaskId(taskId);
        if (counterpart == null) {
            return false;
        }
        // Keep the logical task pending while its other physical attempt can
        // still succeed. In particular, do not invoke the global failure callback.
        Set<String> failed = speculativeFailures.computeIfAbsent(
                queryId, ignored -> ConcurrentHashMap.newKeySet());
        failed.add(taskId);
        taskTracker.clearTaskTracking(taskId);
        sequences.clearTaskSequence(taskId);
        if (!failed.contains(counterpart)) {
            return true;
        }

        String logicalTaskId = resolveOriginalTaskId(taskId);
        String copyTaskId = taskId.equals(logicalTaskId) ? counterpart : taskId;
        notifyStageTerminal(queryId, logicalTaskId, report);
        taskTracker.updateTaskState(queryId, logicalTaskId, "FAILED");
        cleanupLoserTask(queryId, copyTaskId);
        clearSpeculativeFailures(queryId, logicalTaskId, copyTaskId);
        speculativeLauncher.removePair(logicalTaskId);
        evaluate.accept(queryId, report);
        return true;
    }

    private void clearSpeculativeFailures(
            String queryId, String originalTaskId, String copyTaskId) {
        Set<String> failed = speculativeFailures.get(queryId);
        if (failed == null) {
            return;
        }
        failed.remove(originalTaskId);
        failed.remove(copyTaskId);
    }

    boolean handleTaskCompletion(String queryId, String taskId, TaskStatusReport report) {
        if (speculativeLauncher != null && speculativeLauncher.isAbortedCopy(taskId)) {
            cleanupLoserTask(queryId, taskId);
            return true;
        }
        if (!isSpeculationActive()) {
            // The DAG terminal callback may finalize a distributed write.
            // Retain its physical fragment before making the stage terminal.
            writes.collectWriteCommitMetadata(queryId, report);
            notifyStageTerminal(queryId, taskId, report);
            evaluate.accept(queryId, report);
            return true;
        }
        return resolveSpeculativeCompletion(queryId, taskId, report);
    }

    private boolean resolveSpeculativeCompletion(String queryId, String taskId,
                                                  TaskStatusReport report) {
        String stageId = String.valueOf(report.stageId());
        boolean isWinner = duplicateResolver.onTaskComplete(
                queryId, taskId, stageId, report.partitionId());

        if (isWinner) {
            if (!report.attempt().equals(taskTracker.getTaskAttempt(taskId))) {
                // The other attempt may have fenced this one after the first
                // status check but before it entered the winner election.
                return false;
            }
            // A speculative loser may finish concurrently with its winner.
            // Only the elected attempt contributes objects to the catalog commit.
            writes.collectWriteCommitMetadata(queryId, report);
            String logicalTaskId = resolveOriginalTaskId(taskId);
            String counterpart = findCounterpartTaskId(taskId);
            cancelLoserTask(queryId, taskId, report);
            notifyStageTerminal(queryId, logicalTaskId, report);
            if (counterpart != null) {
                clearSpeculativeFailures(queryId, logicalTaskId,
                        taskId.equals(logicalTaskId) ? counterpart : taskId);
            }
            speculativeLauncher.removePair(logicalTaskId);
            LOG.info("event=speculation_winner queryId={} taskId={} stageId={} partitionId={}",
                    queryId, taskId, report.stageId(), report.partitionId());
        } else {
            cleanupLoserTask(queryId, taskId);
            LOG.info("event=speculation_loser queryId={} taskId={} stageId={} partitionId={}",
                    queryId, taskId, report.stageId(), report.partitionId());
            return true;
        }

        evaluate.accept(queryId, report);
        return true;
    }

    private void cancelLoserTask(String queryId, String taskId, TaskStatusReport report) {
        String loserId = findCounterpartTaskId(taskId);
        if (loserId == null) {
            return;
        }
        cancelAndCleanup(queryId, loserId);
    }

    private void cleanupLoserTask(String queryId, String taskId) {
        if (taskTracker.unregisterRecordedDrainedTask(taskId)) {
            sequences.clearTaskSequence(taskId);
        }
    }

    private void cancelAndCleanup(String queryId, String taskId) {
        sendCancelToWorker(taskId);
        cleanupLoserTask(queryId, taskId);
    }

    private void sendCancelToWorker(String taskId) {
        if (taskDispatcher == null) {
            return;
        }
        WorkerInfo worker = resolveWorker(taskId);
        if (worker == null) {
            return;
        }
        try {
            speculativeLauncher.cancelIssuedAttempt(taskTracker.getTaskRequest(taskId).taskRequest()
                    .getAttempt().getQueryOrJobId(), taskId, worker);
        } catch (Exception e) {
            LOG.debug("event=loser_cancel_dispatch_failed taskId={} error={}",
                    taskId, e.getMessage());
        }
    }

    private WorkerInfo resolveWorker(String taskId) {
        String workerId = taskTracker.getWorkerForTask(taskId);
        return workerId != null ? workerManager.getWorker(workerId) : null;
    }

    /**
     * Finds the counterpart (original or speculative) task ID for a given task.
     */
    String findCounterpartTaskId(String taskId) {
        if (speculativeLauncher.isSpeculative(taskId)) {
            return speculativeLauncher.getOriginalTaskId(taskId).orElse(null);
        }
        return speculativeLauncher.getSpeculativeTaskId(taskId).orElse(null);
    }

    private String resolveOriginalTaskId(String taskId) {
        if (speculativeLauncher.isSpeculative(taskId)) {
            return speculativeLauncher.getOriginalTaskId(taskId).orElse(taskId);
        }
        return taskId;
    }

    boolean isSpeculationActive() {
        return speculativeLauncher != null && duplicateResolver != null;
    }

}

