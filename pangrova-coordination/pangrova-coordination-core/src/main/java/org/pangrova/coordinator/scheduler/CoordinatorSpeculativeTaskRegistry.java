package org.pangrova.coordinator.scheduler;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.api.placement.TaskDuration;
import org.pangrova.scheduler.speculation.SpeculativeTaskRegistry;

/**
 * Exposes the query-owned task registry to scheduler speculation without
 * transferring task identity or drain authority to the scheduler.
 */
public final class CoordinatorSpeculativeTaskRegistry implements SpeculativeTaskRegistry {

    private final CoordinatorTaskTracker tracker;

    public CoordinatorSpeculativeTaskRegistry(CoordinatorTaskTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public String getWorkerForTask(String taskId) {
        return tracker.getWorkerForTask(taskId);
    }

    @Override
    public List<TaskDuration> getTaskDurationSnapshots(
            String queryId, String stageId, Set<String> speculativeTaskIds, Instant now) {
        return tracker.getTaskDurationSnapshots(queryId, stageId, speculativeTaskIds, now);
    }

    @Override
    public Set<String> getRunningStageIds(String queryId) {
        return tracker.getRunningStageIds(queryId);
    }

    @Override
    public SubmitTaskRequest getTaskRequest(String taskId) {
        return tracker.getTaskRequest(taskId);
    }

    @Override
    public Optional<TaskReport> executionDrainReceipt(String taskId) {
        return tracker.executionDrainReceipt(taskId);
    }

    @Override
    public boolean unregisterRecordedDrainedTask(String taskId) {
        return tracker.unregisterRecordedDrainedTask(taskId);
    }

    @Override
    public void unregisterUndispatchedTask(String queryId, String taskId) {
        tracker.unregisterUndispatchedTask(queryId, taskId);
    }

    @Override
    public void registerTask(String queryId, String taskId) {
        tracker.registerTask(queryId, taskId);
    }

    @Override
    public void registerTaskRequest(String taskId, SubmitTaskRequest request) {
        tracker.registerTaskRequest(taskId, request);
    }

    @Override
    public void bindDispatchTarget(SubmitTaskRequest request, PlacementWorker worker) {
        tracker.bindDispatchTarget(request, requireWorkerInfo(worker));
    }

    @Override
    public void markScheduled(String queryId, String taskId) {
        tracker.markScheduled(queryId, taskId);
    }

    @Override
    public void assignWorker(String taskId, String workerId) {
        tracker.assignWorker(taskId, workerId);
    }

    @Override
    public void setDeadline(String taskId, Instant deadline) {
        tracker.setDeadline(taskId, deadline);
    }

    @Override
    public void enterTaskDispatch(SubmitTaskRequest request) {
        tracker.enterTaskDispatch(request);
    }

    @Override
    public void updateTaskState(String queryId, String taskId, String workerState) {
        tracker.updateTaskState(queryId, taskId, workerState);
    }

    @Override
    public void recordTaskStart(String taskId, String stageId) {
        tracker.recordTaskStart(taskId, stageId);
    }

    private static WorkerInfo requireWorkerInfo(PlacementWorker worker) {
        if (worker instanceof WorkerInfo info) {
            return info;
        }
        throw new IllegalArgumentException(
                "Coordinator task registry requires a WorkerInfo dispatch target");
    }
}
