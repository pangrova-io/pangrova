package org.pangrova.protocol.rest.task;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;

import java.util.Map;

/** In-process task status view used by publication, completion and diagnostics. */
public record TaskStatusReport(
        String taskId,
        String queryId,
        int stageId,
        int partitionId,
        String taskType,
        String workerId,
        String state,
        NativeTaskStats supplementalStats,
        ErrorDetail error,
        WriteResultDescriptor writeResult,
        long reportSequence,
        long timestampMs,
        String errorCode,
        Map<String, String> errorMetadata,
        TaskAttemptDescriptor attempt,
        TaskReportEvidence terminalEvidence,
        TaskAttemptId reportedAttempt) {

    /** Maximum binary HTTP delivery, including any write commit fragments. */
    public static final int MAX_HTTP_BODY_BYTES = 8 * 1024 * 1024;

    /** Maximum canonical terminal evidence accepted at the control boundary. */
    public static final int MAX_CANONICAL_BYTES = 4 * 1024 * 1024;

    public TaskStatusReport(
            String taskId, String queryId, int stageId, int partitionId,
            String taskType, String workerId, String state, NativeTaskStats stats,
            ErrorDetail error, WriteResultDescriptor writeResult,
            long reportSequence, long timestampMs, String errorCode,
            Map<String, String> errorMetadata, TaskAttemptDescriptor attempt) {
        this(taskId, queryId, stageId, partitionId, taskType, workerId, state,
                stats, error, writeResult, reportSequence, timestampMs, errorCode,
                errorMetadata, attempt, null, null);
    }

    public TaskStatusReport(
            String taskId, String queryId, int stageId, int partitionId,
            String taskType, String workerId, String state, NativeTaskStats stats,
            ErrorDetail error, WriteResultDescriptor writeResult,
            long reportSequence, long timestampMs, String errorCode,
            Map<String, String> errorMetadata, TaskAttemptDescriptor attempt,
            byte[] canonicalTaskReport) {
        this(taskId, queryId, stageId, partitionId, taskType, workerId, state,
                stats, error, writeResult, reportSequence, timestampMs, errorCode,
                errorMetadata, attempt, decodeEvidence(canonicalTaskReport), null);
    }

    public TaskStatusReport {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be null or blank");
        }
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be null or blank");
        }
        if (state == null || state.isBlank()) {
            throw new IllegalArgumentException("state must not be null or blank");
        }
        if (attempt == null) {
            throw new IllegalArgumentException("attempt must not be null");
        }
        errorMetadata = errorMetadata == null ? null : Map.copyOf(errorMetadata);
    }

    /** Builds a Worker report using the complete issued attempt identity. */
    public static TaskStatusReport fromAttempt(TaskAttemptId identity, String taskType, String workerId,
                                              String state, NativeTaskStats stats, ErrorDetail error,
                                              WriteResultDescriptor writeResult, long sequence, long timestampMs,
                                              byte[] terminalEvidence) {
        return fromEvidence(identity, taskType, workerId, state, stats, error, writeResult, sequence,
                timestampMs, decodeEvidence(terminalEvidence));
    }

    /** Reuses evidence already parsed by the producer. */
    public static TaskStatusReport fromEvidence(TaskAttemptId identity, String taskType, String workerId,
                                              String state, NativeTaskStats stats, ErrorDetail error,
                                              WriteResultDescriptor writeResult, long sequence, long timestampMs,
                                              TaskReportEvidence terminalEvidence) {
        return new TaskStatusReport(identity.getTaskId(), identity.getQueryOrJobId(), identity.getStageId(),
                identity.getPartitionId(), taskType, workerId, state, stats, error, writeResult, sequence,
                timestampMs, null, null, TaskAttemptDescriptor.from(identity),
                terminalEvidence, identity);
    }

    /** Returns immutable evidence in its original wire encoding. */
    public ByteString canonicalEvidence() {
        return terminalEvidence == null ? null : terminalEvidence.bytes();
    }

    /** Returns owned bytes for callers that pass the exact evidence to an external sink. */
    public byte[] canonicalTaskReport() {
        return terminalEvidence == null ? null : terminalEvidence.bytes().toByteArray();
    }

    /** Returns the immutable terminal message paired with those exact bytes. */
    public TaskReport canonicalStatus() {
        return terminalEvidence == null ? null : terminalEvidence.report();
    }

    private static TaskReportEvidence decodeEvidence(byte[] bytes) {
        if (bytes == null) return null;
        try {
            return TaskReportEvidence.decode(ByteString.copyFrom(bytes));
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed canonical Native TaskReport", failure);
        }
    }
}
