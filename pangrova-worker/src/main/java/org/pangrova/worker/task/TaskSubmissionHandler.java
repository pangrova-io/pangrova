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

import org.pangrova.worker.enums.TaskAdmissionReason;
import org.pangrova.worker.exception.TaskAdmissionException;
import org.pangrova.worker.task.report.TaskStatusReporter;
import org.pangrova.worker.enums.TaskState;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.ValidatedTaskSubmission;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Admits a task, reserves execution resources and hands the exact request to its lifecycle owner.
 */
final class TaskSubmissionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskSubmissionHandler.class);

    private final TaskManager owner;

    TaskSubmissionHandler(TaskManager owner) {
        this.owner = owner;
    }

    String submitTask(TaskDescriptor descriptor, SubmitTaskRequest request) {
        return submitTask(descriptor, request, null);
    }

    String submitValidatedTask(TaskDescriptor descriptor, ValidatedTaskSubmission request) {
        return submitTask(descriptor, request.submission(), request);
    }

    private String submitTask(TaskDescriptor descriptor, SubmitTaskRequest request,
            ValidatedTaskSubmission previouslyValidated) {
        checkNotNull(descriptor, "descriptor must not be null");
        checkNotNull(request, "request must not be null");
        // Leadership fencing must run before draining checks, tenant admission,
        // deduplication, memory allocation, or Native task creation. In
        // particular, a stale leader must not receive a successful dedup reply.
        var canonical = request.taskRequest();
        owner.verifyWorkerExecutionIdentity(canonical.getAttempt());
        owner.verifyControlEpochForAdmission(canonical.getAttempt().getControlEpoch());
        owner.taskAdmissions.requireUnexpired(canonical.getDeadlineEpochMs());
        var admission = previouslyValidated == null ? new ValidatedTaskSubmission(request) : previouslyValidated;
        owner.verifyExecutionOwnerForAdmission(request);
        if (canonical.getTaskKind() == org.pangrova.protocol.v2.TaskKind.TASK_KIND_CDC_INGEST
                && !owner.cdcIngestSupported()) {
            throw new TaskAdmissionException(TaskAdmissionReason.CLOSED,
                    "Worker has no configured CDC ingestion backend");
        }
        if (canonical.hasCopySourceStagingCommandSha256()) {
            boolean hasCopyInputs = owner.getSharedNativeExecutor()
                    .flatMap(org.pangrova.worker.execution.NativeTaskExecutor::ownerOf)
                    .filter(org.pangrova.worker.execution.NativeTaskExecutor::hasCopyInputs)
                    .isPresent();
            if (!hasCopyInputs) {
                throw new IllegalStateException("COPY source task requires its configured Native input runtime");
            }
        }
        if (owner.drainExecutor.isDraining()) {
            throw new TaskAdmissionException(TaskAdmissionReason.CLOSED,
                    "Worker is draining — new tasks are rejected");
        }
        String taskId =
                owner.tasks.nextId(descriptor.queryId().toString());
        var reservation = owner.taskAdmissions.reserve(canonical, taskId);
        if (!reservation.first()) return reservation.awaitAdmission();
        return submitReserved(descriptor, admission, taskId, reservation);
    }

    private String submitReserved(
            TaskDescriptor descriptor,
            ValidatedTaskSubmission admission,
            String taskId,
            TaskAdmissionRegistry.Reservation reservation) {
        var request = admission.submission();
        String queryId = descriptor.queryId().toString();
        TaskInfo info = null;
        try {
            owner.tasks.requireCapacity();
            info = new TaskInfo(taskId, queryId, request, descriptor, Instant.now());
            reserveTenantQuota(info);
            if (owner.lifecycleManager != null) {
                owner.lifecycleManager.reserveAdmissionResources(info);
            }
            TaskInfo published = info;
            reservation.publish(() -> owner.tasks.register(published));
            owner.transition(taskId, TaskState.INITIALIZING);
            registerSubmittedTask(taskId, queryId, request);
            reservation.handOff();
        } catch (RuntimeException | Error failure) {
            rejectBeforeOwnership(info, reservation, failure);
            throw failure;
        }
        try {
            LOG.info("event=task_submitted taskId={} queryId={}", taskId, queryId);
            if (owner.lifecycleManager != null) {
                owner.lifecycleManager.executeTask(taskId, info, admission, owner.lifecycleCallback);
            } else {
                autoFinishIfConfigured(taskId, queryId, request);
            }
            reservation.completeTaskSetup();
            return taskId;
        } catch (RuntimeException | Error failure) {
            reservation.failAfterHandoff(failure);
            throw failure;
        }
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
                    });
            owner.tasks.retainForCleanup(info);
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
        owner.tasks.remove(info);
    }

    String submitTask(TaskDescriptor descriptor) {
        checkNotNull(descriptor, "descriptor must not be null");
        owner.verifyControlEpochForAdmission(null);
        owner.tasks.requireCapacity();
        String taskId =
                owner.tasks.nextId(descriptor.queryId().toString());
        String queryId = descriptor.queryId().toString();
        TaskInfo info = new TaskInfo(taskId, queryId, null, descriptor, Instant.now());
        owner.tasks.register(info);
        owner.transition(taskId, TaskState.INITIALIZING);
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
        owner.transition(taskId, TaskState.RUNNING);
        owner.transition(taskId, TaskState.FINISHING);
        owner.transition(taskId, TaskState.FINISHED);
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
