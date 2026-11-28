package org.pangrova.scheduler.speculation;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.api.speculation.TaskDuration;

/**
 * Query-owned task registry as seen by speculative scheduling.
 *
 * <p>The registry remains the sole authority for issued task identities,
 * admission seals and exact drain receipts. Speculation may register and
 * observe copies, but never becomes the durable owner of query state.</p>
 */
public interface SpeculativeTaskRegistry {

    String getWorkerForTask(String taskId);

    List<TaskDuration> getTaskDurationSnapshots(
            String queryId, String stageId, Set<String> speculativeTaskIds, Instant now);

    Set<String> getRunningStageIds(String queryId);

    SubmitTaskRequest getTaskRequest(String taskId);

    Optional<TaskReport> executionDrainReceipt(String taskId);

    boolean unregisterRecordedDrainedTask(String taskId);

    void unregisterUndispatchedTask(String queryId, String taskId);

    /** Installs the local attempt and commits its exact issuance before transport entry. */
    void prepareDispatch(SubmitTaskRequest request, PlacementWorker worker);

    void assignWorker(String taskId, String workerId);

    void enterTaskDispatch(SubmitTaskRequest request);

    void updateTaskState(String queryId, String taskId, String workerState);

    void recordTaskStart(String taskId, String stageId);
}
