package org.pangrova.worker.task;

import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.spi.execution.TaskDescriptor;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Collects task statistics and reports status to the coordinator.
 * Extracted from TaskManager to isolate status reporting concerns.
 */
final class TaskStatusCollector {

    private static final Logger LOG = LoggerFactory.getLogger(TaskStatusCollector.class);

    private final TaskStatusReporter reporter;
    private final String workerId;
    private final TaskMemoryAllocator memoryAllocator;
    private volatile org.pangrova.worker.jni.write.NativeWriteExecutor writeExecutor;

    TaskStatusCollector(TaskStatusReporter reporter, String workerId,
                        TaskMemoryAllocator memoryAllocator) {
        this.reporter = reporter;
        this.workerId = workerId;
        this.memoryAllocator = memoryAllocator;
    }

    void setWriteExecutor(org.pangrova.worker.jni.write.NativeWriteExecutor executor) {
        this.writeExecutor = executor;
    }

    void reportStatus(TaskInfo info, String state, ErrorDetail error) {
        reportStatus(info, state, error, info.terminalTaskReport());
    }

    void reportWorkerTerminalStatus(TaskInfo info, String state, ErrorDetail error) {
        if (reporter == null) {
            return;
        }
        if (!"FAILED".equals(state) && !"CANCELLED".equals(state)) {
            throw new IllegalArgumentException("Worker terminal status must be unsuccessful");
        }
        var taskState = "CANCELLED".equals(state)
                ? org.pangrova.protocol.v2.TaskState.TASK_STATE_CANCELLED
                : org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED;
        byte[] canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessful(
                info.request().taskRequest(), taskState, error,
                info.executorReleaseReport());
        canonical = withActualDrain(info, canonical);
        reportStatus(info, state, error, canonical);
    }

    void reportWorkerTerminalFailure(TaskInfo info, org.pangrova.protocol.v2.ExecutionError error) {
        if (reporter == null) return;
        byte[] canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessfulWithError(
                info.request().taskRequest(), org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED,
                error, info.executorReleaseReport());
        canonical = withActualDrain(info, canonical);
        reportStatus(info, "FAILED", new ErrorDetail(error.getCode(), error.getErrorClass().name(),
                error.getMessage()), canonical);
    }

    private static byte[] withActualDrain(TaskInfo info, byte[] canonical) {
        try {
            var report = org.pangrova.protocol.v2.TaskReport.parseFrom(canonical).toBuilder()
                    .setExecutionDrainState(info.executionDrainState());
            if (info.executorHandle() == 0 && info.executorReleaseReport() == null) {
                report.getUsageBuilder().setObjectStoreReadBytes(info.preNativeReadBytes());
            }
            return report.build().toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Worker produced a malformed canonical terminal report", failure);
        }
    }

    /** Reads only the exact successful resource-release checkpoint, never a poll/ACK or handle value. */
    java.util.Optional<org.pangrova.protocol.v2.TaskReport> executionDrainReceipt(TaskInfo info) {
        if (!info.resourcesReleased()) return java.util.Optional.empty();
        byte[] actual = info.executorReleaseReport();
        if (actual != null) {
            var report = TaskCompletionMonitor.requireCanonicalTaskReport(info.request().taskRequest(), actual);
            if (report.getExecutionDrainState() != org.pangrova.protocol.v2.ExecutionDrainState
                    .EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED) {
                throw new IllegalStateException("Released Native receipt lacks actual execution drain evidence");
            }
            return java.util.Optional.of(report);
        }
        if (info.executionDrainState() != org.pangrova.protocol.v2.ExecutionDrainState
                .EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER) {
            throw new IllegalStateException("Resource release lacks verified closed admission");
        }
        var closed = org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.closedWithoutOwner(
                org.pangrova.protocol.task.CanonicalTaskAdmissionSeals.create(info.request().taskRequest()));
        return java.util.Optional.of(closed.toBuilder().setUsage(closed.getUsage().toBuilder()
                .setObjectStoreReadBytes(info.preNativeReadBytes())).build());
    }

    private void reportStatus(
            TaskInfo info, String state, ErrorDetail error, byte[] terminalReport) {
        if (reporter == null) {
            return;
        }
        if ("FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state)) {
            boolean released = info.resourcesReleased();
            info.publishTerminalStatus(released,
                    () -> prepareTerminalReport(info, state, error, terminalReport, released), reporter::report);
        } else {
            reporter.report(buildReport(info, state, error, terminalReport));
        }
    }

    private TaskStatusReport prepareTerminalReport(
            TaskInfo info, String state, ErrorDetail error, byte[] terminalReport, boolean released) {
        if (!released) return buildReport(info, state, error, terminalReport);
        if (terminalReport == null) return releasedReport(info);
        // Preserve classified Worker failures as well as Native terminal status; drain comes only from the actual exit.
        return buildReport(info, state, error, withActualDrain(info, terminalReport));
    }

    /** Publishes the actual later release, without cancelling a successfully consumed result. */
    void reportReleasedTerminal(TaskInfo info) {
        if (reporter == null) return;
        info.publishTerminalStatus(true, () -> releasedReport(info), reporter::report);
    }

    private TaskStatusReport releasedReport(TaskInfo info) {
        var actual = executionDrainReceipt(info).orElseThrow(() ->
                new IllegalStateException("Terminal publication lacks actual released execution evidence"));
        String state = info.getState().name();
        if ("FINISHED".equals(state)) {
            if (actual.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
                throw new IllegalStateException("Successful Worker state conflicts with actual release state");
            }
            return buildReport(info, state, null, actual.toByteArray());
        } else if ("FAILED".equals(state) || "CANCELLED".equals(state)) {
            if ("FAILED".equals(state) && !actual.hasError()) {
                throw new IllegalStateException("Failed terminal publication lacks its original classified failure");
            }
            ErrorDetail error = actual.hasError()
                    ? new ErrorDetail(actual.getError().getCode(), actual.getError().getErrorClass().name(),
                            actual.getError().getMessage())
                    : new ErrorDetail("TASK_CANCELLED", "Cancellation", "Task cancelled by control plane");
            byte[] canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessful(
                    info.request().taskRequest(), org.pangrova.protocol.v2.TaskState.valueOf("TASK_STATE_" + state),
                    error, info.executorReleaseReport());
            return buildReport(info, state, error, withActualDrain(info, canonical));
        } else {
            throw new IllegalStateException("Released task is not terminal: " + state);
        }
    }

    private TaskStatusReport buildReport(TaskInfo info, String state, ErrorDetail error, byte[] terminalReport) {
        long seq = info.nextReportSequence();
        TaskStats taskStats = collectTaskStats(info, state);
        WriteResultDescriptor writeResult = collectWriteResult(info, state);
        SubmitTaskRequest req = info.request();
        if (req == null || !req.taskRequest().hasAttempt()) {
            throw new IllegalStateException(
                    "Cannot report task status without fenced request identity: task="
                            + info.taskId());
        }
        var canonical = req.taskRequest();
        int stageId = canonical.getAttempt().getStageId();
        int partitionId = canonical.getAttempt().getPartitionId();
        String taskType = info.descriptor().taskType();
        // Use coordinator's taskId from the request for status reporting.
        // The coordinator registers tasks by this ID in CoordinatorTaskTracker.
        // Using the worker's internal taskId would cause a mismatch and the
        // coordinator's awaitTaskCompletion future would never complete.
        String reportTaskId = canonical.getAttempt().getTaskId();
        TaskStatusReport report = new TaskStatusReport(
                reportTaskId, info.queryId(), stageId, partitionId, taskType,
                workerId, state, taskStats, error, writeResult,
                seq, System.currentTimeMillis(), null, null,
                TaskAttemptDescriptor.from(canonical.getAttempt()),
                terminalReport);
        logStatusReport(info, state, seq);
        return report;
    }

    void logTaskSubmitted(String taskId) {
        LOG.info("event=task_submitted taskId={} workerId={}", taskId, workerId);
    }

    void logTaskCompleted(String taskId) {
        LOG.info("event=task_completed taskId={} workerId={}", taskId, workerId);
    }

    void logTaskError(String taskId, Exception cause) {
        LOG.error("event=task_failed taskId={} workerId={} error={}",
                taskId, workerId, cause.getMessage(), cause);
    }

    private TaskStats collectTaskStats(TaskInfo info, String state) {
        TaskStats captured = decodeCapturedTaskStats(info);
        if (captured != null) {
            logCacheUsage(info.taskId(), captured);
            return captured;
        }
        if (isReleasedTerminalWithoutStats(info, state)) {
            if (!"FINISHED".equals(state)) {
                LOG.info("event=unsuccessful_terminal_native_stats_unavailable taskId={}", info.taskId());
                return null;
            }
            throw new IllegalStateException(
                    "Terminal task has no captured canonical NativeTaskStats: "
                            + info.taskId());
        }
        return collectInProgressTaskStats(info);
    }

    private static TaskStats decodeCapturedTaskStats(TaskInfo info) {
        byte[] terminalStats = info.terminalTaskStats();
        if (terminalStats != null && terminalStats.length > 0) {
            return NativeOperatorStatsCollector.decodeTaskStats(terminalStats);
        }
        return null;
    }

    private static boolean isReleasedTerminalWithoutStats(TaskInfo info, String state) {
        return info.resourcesReleased()
                && ("FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state));
    }

    private static void logCacheUsage(String taskId, TaskStats taskStats) {
        LOG.info("event=native_task_stats taskId={} cache_hit_count={} "
                        + "cache_miss_count={} cache_eviction_count={} "
                        + "cache_admission_rejection_count={}",
                taskId, taskStats.cacheHitCount(), taskStats.cacheMissCount(),
                taskStats.cacheEvictionCount(), taskStats.cacheAdmissionRejectionCount());
    }

    private TaskStats collectInProgressTaskStats(TaskInfo info) {
        long elapsed = java.time.Duration.between(info.startTime(), Instant.now()).toMillis();
        long outputRows = info.outputBatches();
        long outputBytes = info.outputBytes();
        long memoryBytes = (memoryAllocator != null) ? memoryAllocator.resolveCurrentMemoryUsage(info) : 0;
        long peakMemory = (memoryAllocator != null) ? memoryAllocator.resolvePeakMemoryUsage(info) : 0;
        if (memoryAllocator != null) {
            memoryAllocator.logMemoryWarningIfNeeded(info, memoryBytes);
        }
        return new TaskStats(outputRows, outputBytes, outputRows, outputBytes,
                elapsed, 0, memoryBytes, peakMemory, 0, 0, 0,
                0, 0, List.of(), 0, 0, 0, 0, 0);
    }

    private WriteResultDescriptor collectWriteResult(TaskInfo info, String state) {
        if (!info.isWriteTask()) {
            return null;
        }
        long totalRows = info.outputBatches();
        long totalBytes = info.outputBytes();
        byte[] metadata = retainedCommitMetadata(info, state);
        return new WriteResultDescriptor(List.of(), totalRows, totalBytes,
                metadata != null ? metadata : new byte[0]);
    }

    private byte[] retainedCommitMetadata(TaskInfo info, String state) {
        byte[] metadata = info.commitMetadata();
        if (!"FINISHED".equals(state) || (metadata != null && metadata.length > 0)) {
            return metadata;
        }
        // Cache the fragment before reporting. Retries and subsequent terminal
        // reports must carry the identical payload; in-progress reports never drain it.
        synchronized (info) {
            metadata = info.commitMetadata();
            if (metadata == null || metadata.length == 0) {
                metadata = drainCommitMetadata(info);
                if (metadata != null && metadata.length > 0) {
                    info.setCommitMetadata(metadata);
                }
            }
            return metadata;
        }
    }

    /**
     * Drains commit metadata from the write executor for the given task,
     * logging a warning when no executor is configured.
     */
    private byte[] drainCommitMetadata(TaskInfo info) {
        org.pangrova.worker.jni.write.NativeWriteExecutor executor = writeExecutor;
        if (executor == null) {
            LOG.warn("event=collect_write_result_no_executor taskId={}", info.taskId());
            return null;
        }
        SubmitTaskRequest req = info.request();
        String taskId = req != null
                ? req.taskRequest().getAttempt().getTaskId() : info.taskId();
        byte[] metadata = executor.drainCommitMetadataForTask(taskId);
        LOG.debug("event=collect_write_result_drained taskId={} bytes={}",
                taskId, metadata.length);
        return metadata;
    }

    private void logStatusReport(TaskInfo info, String state, long seq) {
        TaskDescriptor desc = info.descriptor();
        if (desc == null) {
            return;
        }
        LOG.debug("event=status_report taskId={} stage={} partition={} type={} state={} seq={}",
                info.taskId(), desc.stageId(), desc.partitionId(), desc.taskType(), state, seq);
    }
}
