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
import java.util.List;

/**
 * Handles task submission, cancellation, and release operations.
 * Extracted from TaskManager to keep each class under the 500-line limit.
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
        if (owner.drainExecutor.isDraining()) {
            throw new IllegalStateException("Worker is draining — new tasks are rejected");
        }
        checkTenantQuota(request);

        String coordinatorTaskId = request.taskId();
        boolean hasCoordinatorId = coordinatorTaskId != null && !coordinatorTaskId.isBlank();

        if (hasCoordinatorId) {
            String existing = owner.coordinatorTaskIndex.get(coordinatorTaskId);
            if (existing != null) {
                LOG.info("event=task_submit_dedup coordinatorTaskId={} workerTaskId={}",
                        coordinatorTaskId, existing);
                return existing;
            }
        }

        String taskId = descriptor.queryId().toString() + "-" + owner.taskIdCounter.incrementAndGet();
        String queryId = descriptor.queryId().toString();

        if (hasCoordinatorId) {
            String race = owner.coordinatorTaskIndex.putIfAbsent(coordinatorTaskId, taskId);
            if (race != null) {
                LOG.info("event=task_submit_dedup_race coordinatorTaskId={} workerTaskId={}",
                        coordinatorTaskId, race);
                return race;
            }
        }

        TaskInfo info = new TaskInfo(taskId, queryId, request, descriptor, Instant.now());
        owner.tasks.put(taskId, info);
        owner.transition(taskId, TaskManager.TaskState.INITIALIZING);
        registerSubmittedTask(taskId, queryId, request);
        LOG.info("event=task_submitted taskId={} queryId={}", taskId, queryId);
        if (owner.lifecycleManager != null) {
            owner.lifecycleManager.executeTask(taskId, info, owner.lifecycleCallback);
        } else {
            autoFinishIfConfigured(taskId, queryId, request);
        }
        return taskId;
    }

    String submitTask(TaskDescriptor descriptor) {
        checkNotNull(descriptor, "descriptor must not be null");
        if (owner.tasks.size() >= TaskManager.MAX_TRACKED_TASKS) {
            throw new IllegalStateException(
                    "Task tracking limit reached: " + owner.tasks.size()
                            + " tasks (max=" + TaskManager.MAX_TRACKED_TASKS + "). "
                            + "Possible leak in forHttpWiringOnly path.");
        }
        String taskId = descriptor.queryId().toString() + "-" + owner.taskIdCounter.incrementAndGet();
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
        if (!owner.transition(taskId, TaskManager.TaskState.CANCELLED)) {
            return TaskCancelResult.notCancellable(taskId, current.name());
        }
        if (owner.lifecycleManager != null) {
            try {
                owner.lifecycleManager.cancelNativeExecutor(info);
            } finally {
                owner.lifecycleManager.releaseMemoryIfNeeded(info);
            }
        }
        if (owner.statusCollector != null) {
            owner.statusCollector.reportStatus(info, "CANCELLED", null);
        }
        owner.removeCoordinatorIndex(info);
        LOG.info("event=task_cancelled taskId={} queryId={}", taskId, info.queryId());
        return TaskCancelResult.cancelled();
    }

    void releaseTaskExecutor(String taskId) {
        TaskInfo info = owner.tasks.get(taskId);
        if (info != null && owner.lifecycleManager != null) {
            owner.lifecycleManager.releaseFinishedTaskExecutor(info);
        }
    }

    private void checkTenantQuota(SubmitTaskRequest request) {
        TenantExecutionQuotaEnforcer enforcer = owner.quotaEnforcer;
        String tenantId = request.tenantId();
        if (enforcer != null && tenantId != null && !tenantId.isBlank()) {
            if (!enforcer.canAdmitTask(tenantId)) {
                throw new IllegalStateException(
                        "Tenant quota exceeded: tenantId=" + tenantId
                                + " — new tasks rejected until resources are released");
            }
        }
    }

    private void registerSubmittedTask(String taskId, String queryId,
                                       SubmitTaskRequest request) {
        TenantExecutionQuotaEnforcer enforcer = owner.quotaEnforcer;
        String tenantId = request.tenantId();
        if (enforcer != null && tenantId != null && !tenantId.isBlank()) {
            enforcer.registerTask(taskId, tenantId);
        }
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
        List<String> splits = request.splits();
        if (splits == null || splits.isEmpty()) {
            return;
        }
        for (String encoded : splits) {
            recordEncodedSplit(tracker, encoded);
        }
    }

    private static void recordEncodedSplit(
            org.pangrova.worker.cache.RecentFileAccessTracker tracker, String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return;
        }
        try {
            byte[] bytes = java.util.Base64.getDecoder().decode(encoded);
            org.pangrova.protocol.v1.SplitInfo split =
                    org.pangrova.protocol.v1.SplitInfo.parseFrom(bytes);
            String filePath = split.getFilePath();
            if (filePath != null && !filePath.isEmpty()) {
                tracker.recordAccess(filePath);
            }
        } catch (Exception e) {
            LOG.debug("event=split_file_path_record_failed error={}", e.getMessage());
        }
    }

    private void autoFinishIfConfigured(String taskId, String queryId,
                                        SubmitTaskRequest request) {
        TaskStatusReporter reporter = owner.autoFinishReporter;
        if (reporter == null) {
            return;
        }
        owner.transition(taskId, TaskManager.TaskState.RUNNING);
        owner.transition(taskId, TaskManager.TaskState.FINISHING);
        owner.transition(taskId, TaskManager.TaskState.FINISHED);
        String coordinatorTaskId = request != null ? request.taskId() : taskId;
        var report = new org.pangrova.protocol.rest.task.TaskStatusReport(
                coordinatorTaskId != null ? coordinatorTaskId : taskId,
                queryId, 0, 0, "COMPUTE", "auto-finish-worker",
                "FINISHED", null, null, null, 1,
                System.currentTimeMillis(), null, null);
        reporter.report(report);
        LOG.info("event=task_auto_finished taskId={} queryId={}", taskId, queryId);
    }
}
