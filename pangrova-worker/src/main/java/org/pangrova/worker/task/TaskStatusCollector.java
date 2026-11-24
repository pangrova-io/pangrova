package org.pangrova.worker.task;

import org.pangrova.protocol.rest.task.TaskReportEvidence;

import org.pangrova.worker.task.report.TaskStatusReporter;

import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.worker.enums.TaskState;
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
    private volatile org.pangrova.worker.write.NativeWriteExecutor writeExecutor;

    TaskStatusCollector(TaskStatusReporter reporter, String workerId,
                        TaskMemoryAllocator memoryAllocator) {
        this.reporter = reporter;
        this.workerId = workerId;
        this.memoryAllocator = memoryAllocator;
    }

    void setWriteExecutor(org.pangrova.worker.write.NativeWriteExecutor executor) {
        this.writeExecutor = executor;
    }

    void reportStatus(TaskInfo info, String state, ErrorDetail error) {
        reportStatus(info, state, error, info.terminalTaskEvidence());
    }

    /** Reserves the canonical finish sequence before arming result release or awaiting its ACK. */
    void reportFinishedResult(TaskInfo info, Runnable watchResult, Runnable onAcknowledgement) {
        if (reporter == null) {
            watchResult.run();
            onAcknowledgement.run();
            return;
        }
        boolean released = info.resourcesReleased();
        info.publishResultStatus(released,
                () -> prepareTerminalReport(info, "FINISHED", null, info.terminalTaskEvidence(), released),
                report -> {
                    watchResult.run();
                    return reporter.reportTerminal(report);
                }, onAcknowledgement).whenComplete((ignored, failure) -> {
                    if (failure != null) {
                        LOG.error("event=result_finish_publication_pending taskId={}", info.taskId(), failure);
                    }
                });
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
        var canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessful(
                info.request().taskRequest(), taskState, error, releasedReportMessage(info));
        reportReconciledStatus(info, state, error, canonical);
    }

    void reportWorkerTerminalFailure(TaskInfo info, org.pangrova.protocol.v2.ExecutionError error) {
        if (reporter == null) return;
        var canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessfulWithError(
                info.request().taskRequest(), org.pangrova.protocol.v2.TaskState.TASK_STATE_FAILED,
                error, releasedReportMessage(info));
        reportReconciledStatus(info, "FAILED", new ErrorDetail(error.getCode(), error.getErrorClass().name(),
                error.getMessage()), canonical);
    }

    private void reportReconciledStatus(TaskInfo info, String state, ErrorDetail error,
            org.pangrova.protocol.v2.TaskReport canonical) {
        var initial = attachActualDrain(info, canonical);
        boolean released = info.resourcesReleased();
        info.publishTerminalStatus(released,
                () -> {
                    if (released && "CANCELLED".equals(state)) {
                        return releasedReport(info, state, error, initial);
                    }
                    var report = released ? attachActualDrain(info, initial) : initial;
                    return buildReport(info, state, error, TaskReportEvidence.encode(report));
                }, reporter::report);
    }

    private static org.pangrova.protocol.v2.TaskReport releasedReportMessage(TaskInfo info) {
        var actual = info.executorReleaseReceipt();
        return actual == null ? null : actual.report();
    }

    private static TaskReportEvidence withActualDrain(TaskInfo info, org.pangrova.protocol.v2.TaskReport canonical) {
        return TaskReportEvidence.encode(attachActualDrain(info, canonical));
    }

    private static org.pangrova.protocol.v2.TaskReport attachActualDrain(
            TaskInfo info, org.pangrova.protocol.v2.TaskReport canonical) {
        var report = canonical.toBuilder().setExecutionDrainState(info.executionDrainState());
        if (info.executorHandle() == 0 && info.executorReleaseReceipt() == null) {
            report.getUsageBuilder().setObjectStoreReadBytes(info.preNativeReadBytes());
        }
        return report.build();
    }

    /** Reads only the exact successful resource-release checkpoint, never a poll/ACK or handle value. */
    java.util.Optional<org.pangrova.protocol.v2.TaskReport> executionDrainReceipt(TaskInfo info) {
        if (!info.resourcesReleased()) return java.util.Optional.empty();
        var actual = info.executorReleaseReceipt();
        if (actual != null) return java.util.Optional.of(actual.report());
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
            TaskInfo info, String state, ErrorDetail error, TaskReportEvidence terminalReport) {
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
            TaskInfo info, String state, ErrorDetail error, TaskReportEvidence terminalReport, boolean released) {
        if (!released) return buildReport(info, state, error, terminalReport);
        if (terminalReport != null && !"CANCELLED".equals(state)) {
            if (terminalReport == info.executorReleaseReceipt()
                    && terminalReport.report().getState() == TaskStateMapper.toProtoState(TaskState.valueOf(state))) {
                return buildReport(info, state, error, terminalReport);
            }
            return buildReport(info, state, error, withActualDrain(info, terminalReport.report()));
        }
        return releasedReport(info, state, error, terminalReport == null ? null : terminalReport.report());
    }

    /** Publishes the actual later release, without cancelling a successfully consumed result. */
    void reportReleasedTerminal(TaskInfo info) {
        if (reporter == null) return;
        info.publishTerminalStatus(true, () -> releasedReport(info), reporter::report);
    }

    java.util.concurrent.CompletionStage<Void> reportReleasedTerminalAsync(TaskInfo info) {
        if (reporter == null) return java.util.concurrent.CompletableFuture.completedFuture(null);
        return info.publishResultStatus(true, () -> releasedReport(info), reporter::reportTerminal, null);
    }

    void retryInitialResultStatus(TaskInfo info) {
        if (reporter != null) info.retryInitialResultStatus(reporter::reportTerminal);
    }

    private TaskStatusReport releasedReport(TaskInfo info) {
        return releasedReport(info, info.getState().name(), null, null);
    }

    private TaskStatusReport releasedReport(TaskInfo info, String state, ErrorDetail error,
            org.pangrova.protocol.v2.TaskReport terminalReport) {
        var actual = executionDrainReceipt(info).orElseThrow(() ->
                new IllegalStateException("Terminal publication lacks actual released execution evidence"));
        if ("CANCELLED".equals(state) && actual.getState() == org.pangrova.protocol.v2.TaskState.TASK_STATE_CANCELLED) {
            // The cancellation response may already have published this exact checkpoint.
            // Keep the Native classification, including a cancellation without an error.
            return buildReport(info, state, terminalError(actual), retainedEvidence(info, actual));
        }
        if (terminalReport != null) {
            // A classified Worker failure can differ from the Native execution outcome.
            return buildReport(info, state, error, withActualDrain(info, terminalReport));
        }
        if ("FINISHED".equals(state)) {
            if (actual.getState() != org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED) {
                throw new IllegalStateException("Successful Worker state conflicts with actual release state");
            }
            return buildReport(info, state, null, retainedEvidence(info, actual));
        } else if ("FAILED".equals(state) || "CANCELLED".equals(state)) {
            if ("FAILED".equals(state) && !actual.hasError()) {
                throw new IllegalStateException("Failed terminal publication lacks its original classified failure");
            }
            ErrorDetail failure = terminalError(actual);
            if (failure == null) {
                failure = new ErrorDetail("TASK_CANCELLED", "Cancellation", "Task cancelled by control plane");
            }
            var canonical = org.pangrova.protocol.task.WorkerTerminalTaskReports.unsuccessful(
                    info.request().taskRequest(), TaskStateMapper.toProtoState(TaskState.valueOf(state)),
                    failure, actual);
            return buildReport(info, state, failure, withActualDrain(info, canonical));
        } else {
            throw new IllegalStateException("Released task is not terminal: " + state);
        }
    }

    private static TaskReportEvidence retainedEvidence(TaskInfo info, org.pangrova.protocol.v2.TaskReport report) {
        var actual = info.executorReleaseReceipt();
        return actual != null ? actual : TaskReportEvidence.encode(report);
    }

    private static ErrorDetail terminalError(org.pangrova.protocol.v2.TaskReport report) {
        if (!report.hasError()) return null;
        var error = report.getError();
        return new ErrorDetail(error.getCode(), error.getErrorClass().name(), error.getMessage());
    }

    private TaskStatusReport buildReport(TaskInfo info, String state, ErrorDetail error, TaskReportEvidence terminal) {
        long seq = info.nextReportSequence();
        var canonical = terminal == null ? null : terminal.report();
        var taskStats = collectTaskStats(info, state, canonical);
        WriteResultDescriptor writeResult = collectWriteResult(info, state);
        SubmitTaskRequest req = info.request();
        if (req == null || !req.taskRequest().hasAttempt()) {
            throw new IllegalStateException(
                    "Cannot report task status without fenced request identity: task="
                            + info.taskId());
        }
        // Use coordinator's taskId from the request for status reporting.
        // The coordinator registers tasks by this ID in CoordinatorTaskTracker.
        // Using the worker's internal taskId would cause a mismatch and the
        // coordinator's awaitTaskCompletion future would never complete.
        TaskStatusReport report = TaskStatusReport.fromEvidence(req.taskRequest().getAttempt(),
                info.taskType(), workerId, state, taskStats, error, writeResult,
                seq, System.currentTimeMillis(), terminal);
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

    private NativeTaskStats collectTaskStats(
            TaskInfo info, String state, org.pangrova.protocol.v2.TaskReport canonical) {
        if (canonical != null && canonical.hasNativeTaskStats()) {
            var stats = canonical.getNativeTaskStats();
            logCacheUsage(info.taskId(), stats.getCacheHitCount(), stats.getCacheMissCount(),
                    stats.getCacheEvictionCount(), stats.getCacheAdmissionRejectionCount());
            return null;
        }
        var captured = info.terminalTaskStats();
        if (captured != null && captured.getSerializedSize() > 0) {
            logCacheUsage(info.taskId(), captured.getCacheHitCount(), captured.getCacheMissCount(),
                    captured.getCacheEvictionCount(), captured.getCacheAdmissionRejectionCount());
            return captured.getBuildKeySummariesCount() == 0
                    ? captured : captured.toBuilder().clearBuildKeySummaries().build();
        }
        if ("FINISHED".equals(state) && info.request() != null
                && TaskCompletionMonitor.isWorkerExecuted(info.request().taskRequest())) {
            return collectInProgressTaskStats(info);
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

    private static boolean isReleasedTerminalWithoutStats(TaskInfo info, String state) {
        return info.resourcesReleased()
                && ("FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state));
    }

    private static void logCacheUsage(String taskId, long hits, long misses, long evictions, long rejected) {
        LOG.info("event=native_task_stats taskId={} cache_hit_count={} "
                        + "cache_miss_count={} cache_eviction_count={} "
                        + "cache_admission_rejection_count={}",
                taskId, hits, misses, evictions, rejected);
    }

    private NativeTaskStats collectInProgressTaskStats(TaskInfo info) {
        long elapsed = java.time.Duration.between(info.startTime(), Instant.now()).toMillis();
        long outputRows = info.outputBatches();
        long outputBytes = info.outputBytes();
        long memoryBytes = (memoryAllocator != null)
                ? memoryAllocator.resolveCurrentMemoryUsage(info.memoryAccount()) : 0;
        long peakMemory = (memoryAllocator != null) ? memoryAllocator.resolvePeakMemoryUsage(info.memoryAccount()) : 0;
        if (memoryAllocator != null) {
            memoryAllocator.logMemoryWarningIfNeeded(info.memoryAccount(), memoryBytes);
        }
        return NativeTaskStats.newBuilder()
                .setRowsRead(outputRows).setRowsWritten(outputRows)
                .setBytesRead(outputBytes).setBytesWritten(outputBytes)
                .setElapsedTimeMs(elapsed).setMemoryBytes(memoryBytes).setPeakMemoryBytes(peakMemory)
                .build();
    }

    private WriteResultDescriptor collectWriteResult(TaskInfo info, String state) {
        if (!info.isWriteTask() && !info.isWorkerExecuted()) {
            return null;
        }
        long totalRows = info.outputBatches();
        long totalBytes = info.outputBytes();
        byte[] metadata = retainedCommitMetadata(info, state);
        return new WriteResultDescriptor(List.of(), totalRows, totalBytes,
                metadata != null ? metadata : new byte[0],
                info.isWorkerExecuted() ? info.terminalCommitFragments() : List.of());
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
        org.pangrova.worker.write.NativeWriteExecutor executor = writeExecutor;
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
        LOG.debug("event=status_report taskId={} stage={} partition={} type={} state={} seq={}",
                info.taskId(), info.stageId(), info.partitionId(), info.taskType(), state, seq);
    }
}
