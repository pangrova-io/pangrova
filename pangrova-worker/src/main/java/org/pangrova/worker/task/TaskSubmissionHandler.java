/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.worker.task;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.execution.TaskDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Handles task submission, cancellation, and release operations. Extracted from TaskManager to keep
 * each class under the 500-line limit.
 */
final class TaskSubmissionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskSubmissionHandler.class);

    private final TaskManager owner;

    TaskSubmissionHandler(TaskManager owner) {
        this.owner = owner;
    }

    String submitTask(TaskDescriptor descriptor, SubmitTaskRequest request) {
        checkNotNull(descriptor, "descriptor must not be null");
        checkNotNull(request, "request must not be null");
        // Leadership fencing must run before draining checks, tenant admission,
        // deduplication, memory allocation, or Native task creation. In
        // particular, a stale leader must not receive a successful dedup reply.
        var canonical = request.taskRequest();
        owner.verifyControlEpochForAdmission(canonical.getAttempt().getControlEpoch());
        owner.taskAdmissions.requireUnexpired(canonical.getDeadlineEpochMs());
        owner.verifyExecutionOwnerForAdmission(request);
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(canonical);
        if (canonical.hasCopySourceStagingCommandSha256()) {
            // A COPY task must stage and verify the exact managed object before
            // Native creation. No Worker task may bypass that pending wiring.
            throw new IllegalStateException(
                    "COPY source task admission requires pre-Native staging integration");
        }
        if (owner.drainExecutor.isDraining()) {
            throw new IllegalStateException("Worker is draining — new tasks are rejected");
        }
        String taskId =
                descriptor.queryId().toString() + "-" + owner.taskIdCounter.incrementAndGet();
        var reservation = owner.taskAdmissions.reserve(canonical, taskId);
        if (!reservation.first()) return reservation.awaitAdmission();
        return submitReserved(descriptor, request, taskId, reservation);
    }

    private String submitReserved(
            TaskDescriptor descriptor,
            SubmitTaskRequest request,
            String taskId,
            TaskAdmissionRegistry.Reservation reservation) {
        String queryId = descriptor.queryId().toString();
        TaskInfo info = null;
        try {
            if (owner.tasks.size() >= TaskManager.MAX_TRACKED_TASKS) {
                throw new TaskAdmissionException(
                        TaskAdmissionException.Reason.CAPACITY,
                        "Task resource tracking limit reached: " + TaskManager.MAX_TRACKED_TASKS);
            }
            info = new TaskInfo(taskId, queryId, request, descriptor, Instant.now());
            reserveTenantQuota(info);
            if (owner.lifecycleManager != null) {
                owner.lifecycleManager.reserveAdmissionResources(info);
            }
            TaskInfo published = info;
            reservation.publish(() -> owner.tasks.put(taskId, published));
            owner.transition(taskId, TaskManager.TaskState.INITIALIZING);
            registerSubmittedTask(taskId, queryId, request);
            reservation.admitted();
        } catch (RuntimeException | Error failure) {
            rejectBeforeOwnership(info, reservation, failure);
            throw failure;
        }
        LOG.info("event=task_submitted taskId={} queryId={}", taskId, queryId);
        if (owner.lifecycleManager != null) {
            owner.lifecycleManager.executeTask(taskId, info, owner.lifecycleCallback);
        } else {
            autoFinishIfConfigured(taskId, queryId, request);
        }
        return taskId;
    }

    private void rejectBeforeOwnership(
            TaskInfo info, TaskAdmissionRegistry.Reservation reservation, Throwable failure) {
        if (info == null) {
            reservation.rejectedBeforeOwnership(failure);
            return;
        }
        try {
            rollbackBeforeOwnership(info);
            reservation.rejectedBeforeOwnership(failure);
        } catch (RuntimeException | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
            info.retainCleanupRetry(
                    () -> {
                        rollbackBeforeOwnership(info);
                        reservation.rejectedBeforeOwnership(failure);
                        owner.tasks.remove(info.taskId(), info);
                    });
            owner.tasks.put(info.taskId(), info);
        }
    }

    private void rollbackBeforeOwnership(TaskInfo info) {
        TaskAdmissionRollback.release(
                () -> {
                    if (owner.lifecycleManager != null) {
                        owner.lifecycleManager.releaseMemoryIfNeeded(info);
                    }
                },
                () -> {
                    if (owner.quotaEnforcer != null) owner.quotaEnforcer.releaseTask(info.taskId());
                },
                () -> {
                    if (owner.cancellationPropagator != null) {
                        owner.cancellationPropagator.deregisterTask(info.queryId(), info.taskId());
                    }
                });
        owner.tasks.remove(info.taskId(), info);
    }

    String submitTask(TaskDescriptor descriptor) {
        checkNotNull(descriptor, "descriptor must not be null");
        owner.verifyControlEpochForAdmission(null);
        if (owner.tasks.size() >= TaskManager.MAX_TRACKED_TASKS) {
            throw new IllegalStateException(
                    "Task tracking limit reached: "
                            + owner.tasks.size()
                            + " tasks (max="
                            + TaskManager.MAX_TRACKED_TASKS
                            + "). "
                            + "Possible leak in forHttpWiringOnly path.");
        }
        String taskId =
                descriptor.queryId().toString() + "-" + owner.taskIdCounter.incrementAndGet();
        String queryId = descriptor.queryId().toString();
        TaskInfo info = new TaskInfo(taskId, queryId, null, descriptor, Instant.now());
        owner.tasks.put(taskId, info);
        owner.transition(taskId, TaskManager.TaskState.INITIALIZING);
        QueryCancellationPropagator prop = owner.cancellationPropagator;
        if (prop != null) {
            prop.registerTask(queryId, taskId);
        }
        if (owner.statusCollector != null) {
            owner.statusCollector.logTaskSubmitted(taskId);
        }
        LOG.info("event=task_submitted_no_wiring taskId={} queryId={}", taskId, queryId);
        return taskId;
    }

    TaskCancelResult cancelTask(String taskId) {
        TaskInfo info = owner.tasks.get(taskId);
        if (info == null) {
            return TaskCancelResult.notFound(taskId);
        }
        TaskManager.TaskState current = info.getState();
        if (owner.lifecycleManager == null) {
            if (!owner.transition(taskId, TaskManager.TaskState.CANCELLING)) {
                return TaskCancelResult.notCancellable(taskId, current.name());
            }
            owner.transition(taskId, TaskManager.TaskState.CANCELLED);
            owner.removeCoordinatorIndex(info);
            return TaskCancelResult.cancelled();
        }
        if (current != TaskManager.TaskState.CANCELLING
                && !owner.transition(taskId, TaskManager.TaskState.CANCELLING)) {
            return TaskCancelResult.notCancellable(taskId, current.name());
        }
        owner.lifecycleManager.cancelNativeExecutor(info);
        // Terminal resource release does not reopen the exact admission identity.
        LOG.info("event=task_cancel_requested taskId={} queryId={}", taskId, info.queryId());
        return TaskCancelResult.cancelled();
    }

    void releaseTaskExecutor(String taskId) {
        TaskInfo info = owner.tasks.get(taskId);
        if (info != null && owner.lifecycleManager != null) {
            // Explicit retirement abandons an unclaimed result; completion already committed keeps
            // its state.
            if (info.getState() == TaskManager.TaskState.RUNNING
                    || info.getState() == TaskManager.TaskState.INITIALIZING) {
                cancelTask(taskId);
            }
            owner.lifecycleManager.releaseRetiredTaskExecutor(info, owner.lifecycleCallback);
        }
    }

    private void reserveTenantQuota(TaskInfo info) {
        TenantExecutionQuotaEnforcer enforcer = owner.quotaEnforcer;
        String tenantId = info.request().taskRequest().getSecurity().getTenantId();
        if (enforcer != null && tenantId != null && !tenantId.isBlank()) {
            if (!enforcer.tryReserveTask(info.taskId(), tenantId)) {
                throw new IllegalStateException(
                        "Tenant quota exceeded: tenantId="
                                + tenantId
                                + " — new tasks rejected until resources are released");
            }
        }
    }

    private void registerSubmittedTask(String taskId, String queryId, SubmitTaskRequest request) {
        if (owner.statusCollector != null) {
            owner.statusCollector.logTaskSubmitted(taskId);
        }
        recordSplitFileAccess(request);
        QueryCancellationPropagator prop = owner.cancellationPropagator;
        if (prop != null) {
            prop.registerTask(queryId, taskId);
        }
    }

    private void recordSplitFileAccess(SubmitTaskRequest request) {
        org.pangrova.worker.cache.RecentFileAccessTracker tracker = owner.recentFileAccessTracker;
        if (tracker == null) {
            return;
        }
        var splits = request.taskRequest().getSplitsList();
        if (splits.isEmpty()) {
            return;
        }
        for (var split : splits) {
            String filePath = split.getOpaqueLocation();
            if (!filePath.isEmpty()) {
                tracker.recordAccess(filePath);
            }
        }
    }

    private void autoFinishIfConfigured(String taskId, String queryId, SubmitTaskRequest request) {
        TaskStatusReporter reporter = owner.autoFinishReporter;
        if (reporter == null) {
            return;
        }
        owner.transition(taskId, TaskManager.TaskState.RUNNING);
        owner.transition(taskId, TaskManager.TaskState.FINISHING);
        owner.transition(taskId, TaskManager.TaskState.FINISHED);
        if (request == null || !request.taskRequest().hasAttempt()) {
            throw new IllegalStateException(
                    "Auto-finish task is missing fenced request identity: " + taskId);
        }
        var canonical = request.taskRequest();
        String coordinatorTaskId = canonical.getAttempt().getTaskId();
        var report =
                new org.pangrova.protocol.rest.task.TaskStatusReport(
                        coordinatorTaskId,
                        queryId,
                        canonical.getAttempt().getStageId(),
                        canonical.getAttempt().getPartitionId(),
                        owner.tasks.get(taskId).descriptor().taskType(),
                        "auto-finish-worker",
                        "FINISHED",
                        null,
                        null,
                        null,
                        1,
                        System.currentTimeMillis(),
                        null,
                        null,
                        org.pangrova.protocol.rest.task.TaskAttemptDescriptor.from(
                                canonical.getAttempt()));
        reporter.report(report);
        LOG.info("event=task_auto_finished taskId={} queryId={}", taskId, queryId);
    }
}
