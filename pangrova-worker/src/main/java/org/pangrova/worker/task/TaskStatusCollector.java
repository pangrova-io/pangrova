package org.pangrova.worker.task;

import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
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
        if (reporter == null) {
            return;
        }
        long seq = info.nextReportSequence();
        TaskStats taskStats = collectTaskStats(info);
        WriteResultDescriptor writeResult = collectWriteResult(info);
        SubmitTaskRequest req = info.request();
        int stageId = req != null ? req.stageId() : 0;
        int partitionId = req != null ? req.partitionId() : 0;
        String taskType = req != null ? req.taskType() : "COMPUTE";
        // Use coordinator's taskId from the request for status reporting.
        // The coordinator registers tasks by this ID in CoordinatorTaskTracker.
        // Using the worker's internal taskId would cause a mismatch and the
        // coordinator's awaitTaskCompletion future would never complete.
        String reportTaskId = (req != null && req.taskId() != null && !req.taskId().isBlank())
                ? req.taskId() : info.taskId();
        TaskStatusReport report = new TaskStatusReport(
                reportTaskId, info.queryId(), stageId, partitionId, taskType,
                workerId, state, taskStats, error, writeResult,
                seq, System.currentTimeMillis(), null, null);
        reporter.report(report);
        logStatusReport(info, state, seq);
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

    private TaskStats collectTaskStats(TaskInfo info) {
        long elapsed = java.time.Duration.between(info.startTime(), Instant.now()).toMillis();
        long outputRows = info.outputBatches();
        long outputBytes = info.outputBytes();
        long memoryBytes = (memoryAllocator != null) ? memoryAllocator.resolveCurrentMemoryUsage(info) : 0;
        long peakMemory = (memoryAllocator != null) ? memoryAllocator.resolvePeakMemoryUsage(info) : 0;
        if (memoryAllocator != null) {
            memoryAllocator.logMemoryWarningIfNeeded(info, memoryBytes);
        }
        return new TaskStats(outputRows, outputBytes, outputRows, outputBytes,
                elapsed, 0, memoryBytes, peakMemory, 0, 0, 0);
    }

    private WriteResultDescriptor collectWriteResult(TaskInfo info) {
        if (!info.isWriteTask()) {
            return null;
        }
        long totalRows = info.outputBatches();
        long totalBytes = info.outputBytes();
        byte[] metadata = info.commitMetadata();
        // If TaskInfo has no commit metadata (common path: native WriteOperator
        // calls commitWrite via JNI but the result is not propagated back to
        // TaskInfo), drain from NativeWriteExecutor by taskId for per-task isolation.
        if (metadata == null || metadata.length == 0) {
            metadata = drainCommitMetadata(info);
        }
        return new WriteResultDescriptor(List.of(), totalRows, totalBytes,
                metadata != null ? metadata : new byte[0]);
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
        String taskId = (req != null && req.taskId() != null) ? req.taskId() : info.taskId();
        byte[] metadata = executor.drainCommitMetadataForTask(taskId);
        LOG.debug("event=collect_write_result_drained taskId={} bytes={}",
                taskId, metadata != null ? metadata.length : 0);
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
