package org.pangrova.worker.task;

import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.TaskAdmissionSealRequest;
import org.pangrova.protocol.v2.TaskReport;

import java.util.Optional;

/** Exact admission sealing and real drain-receipt retention for one task manager. */
@org.pangrova.common.concurrent.ThreadSafe
final class TaskAdmissionControl {
    private final TaskManager owner;

    TaskAdmissionControl(TaskManager owner) { this.owner = owner; }

    Optional<TaskReport> seal(TaskAdmissionSealRequest request) {
        CanonicalTaskAdmissionSeals.validate(request, request.getAttempt().getTaskId());
        var attempt = request.getAttempt();
        boolean known = owner.taskAdmissions.knownIdentity(request);
        if (known) owner.verifyExistingCleanupControlEpoch(attempt.getControlEpoch());
        else {
            owner.verifyControlEpochForAdmission(attempt.getControlEpoch());
            owner.verifyExecutionOwnerForControl(attempt.getQueryOrJobId(), attempt.getExecutionOwner());
        }
        var sealed = owner.taskAdmissions.seal(request, known);
        if (sealed.receipt().isPresent()) return sealed.receipt();
        if (sealed.noOwnerExited()) return Optional.of(CanonicalTaskAdmissionSeals.closedWithoutOwner(request));
        var source = owner.sourcePreparations();
        if (source != null && source.owns(request)) return source.seal(request);
        TaskInfo info = owner.tasks.get(sealed.workerTaskId());
        if (info == null) return Optional.empty();
        if (isTerminal(info.getState())) owner.releaseTaskExecutor(info.taskId());
        else owner.cancelTask(info.taskId());
        return captureReceipt(info);
    }

    void completed(TaskInfo info) {
        if (info.request() == null) return;
        captureReceipt(info);
        owner.taskAdmissions.removeExpired(info.request().taskRequest().getAttempt().getTaskId(), info.taskId());
    }

    private Optional<TaskReport> captureReceipt(TaskInfo info) {
        if (owner.statusCollector == null) return Optional.empty();
        var receipt = owner.statusCollector.executionDrainReceipt(info);
        if (receipt.isEmpty()) return Optional.empty();
        TaskReport compact = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(receipt.orElseThrow());
        owner.taskAdmissions.retainDrainReceipt(info.taskId(), compact,
                info.request().taskRequest().getDeadlineEpochMs());
        return Optional.of(compact);
    }

    private static boolean isTerminal(TaskManager.TaskState state) {
        return state == TaskManager.TaskState.FINISHED || state == TaskManager.TaskState.FAILED
                || state == TaskManager.TaskState.CANCELLED;
    }
}
