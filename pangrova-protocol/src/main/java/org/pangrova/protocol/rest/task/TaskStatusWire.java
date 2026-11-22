package org.pangrova.protocol.rest.task;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.http.TaskProgress;
import org.pangrova.protocol.http.TaskRole;
import org.pangrova.protocol.http.TaskStatusDelivery;
import org.pangrova.protocol.http.TaskWriteEvidence;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;

import java.util.List;

/** Binary Worker-to-Coordinator task status delivery. */
public final class TaskStatusWire {
    public static final String MEDIA_TYPE = "application/x-protobuf";

    private TaskStatusWire() {}

    /** Encodes the exact terminal evidence or a typed progress update. */
    public static byte[] encode(TaskStatusReport report) throws InvalidProtocolBufferException {
        var role = TaskRole.valueOf("TASK_ROLE_" + report.taskType());
        var state = TaskState.valueOf("TASK_STATE_" + report.state());
        var delivery = TaskStatusDelivery.newBuilder().setRole(role).setWorkerId(report.workerId())
                .setSequence(report.reportSequence()).setTimestampMs(report.timestampMs());
        if (report.canonicalEvidence() != null
                && report.canonicalEvidence().size() > TaskStatusReport.MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Canonical task report exceeds the control-plane limit");
        }
        TaskReport terminal = report.canonicalStatus();
        if (isTerminal(state)) {
            if (terminal == null || !terminal.hasAttempt() || terminal.getState() != state) {
                throw new IllegalArgumentException("Terminal delivery requires matching canonical evidence");
            }
            if (report.reportedAttempt() != null && !report.reportedAttempt().equals(terminal.getAttempt())) {
                throw new IllegalArgumentException("Terminal delivery changed its issued task attempt");
            }
            delivery.setTerminalReport(report.canonicalEvidence());
        } else {
            if (terminal != null || !isProgress(state) || report.reportedAttempt() == null) {
                throw new IllegalArgumentException(
                        "Progress requires the complete issued attempt and a progress state");
            }
            delivery.setProgress(TaskProgress.newBuilder().setAttempt(report.reportedAttempt()).setState(state));
        }
        if ((terminal == null || !terminal.hasNativeTaskStats()) && report.stats() != null) {
            delivery.setSupplementalStats(encodeStats(report.stats()));
        }
        if (report.writeResult() != null) {
            delivery.setWrite(encodeWrite(report.writeResult()));
        }
        var payload = delivery.build();
        if (payload.getSerializedSize() > TaskStatusReport.MAX_HTTP_BODY_BYTES) {
            throw new IllegalArgumentException("Task status delivery exceeds the HTTP limit");
        }
        return payload.toByteArray();
    }

    /** Parses each evidence object once and retains the exact received terminal bytes. */
    public static TaskStatusReport decode(byte[] body) throws InvalidProtocolBufferException {
        if (body == null || body.length > TaskStatusReport.MAX_HTTP_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid task status body size");
        }
        var delivery = TaskStatusDelivery.parseFrom(body);
        var role = delivery.getRole();
        if (!delivery.getUnknownFields().asMap().isEmpty() || delivery.getWorkerId().isBlank()
                || delivery.getSequence() <= 0 || role == TaskRole.TASK_ROLE_UNSPECIFIED
                || role == TaskRole.UNRECOGNIZED) {
            throw new IllegalArgumentException("Invalid task status delivery identity");
        }

        TaskAttemptId attempt;
        TaskState state;
        TaskReport canonical = null;
        ByteString evidence = null;
        switch (delivery.getStatusCase()) {
            case PROGRESS -> {
                var progress = delivery.getProgress();
                if (!progress.hasAttempt() || !isProgress(progress.getState())
                        || !progress.getUnknownFields().asMap().isEmpty()) {
                    throw new IllegalArgumentException("Invalid task progress");
                }
                attempt = progress.getAttempt();
                state = progress.getState();
            }
            case TERMINAL_REPORT -> {
                evidence = delivery.getTerminalReport();
                if (evidence.isEmpty() || evidence.size() > TaskStatusReport.MAX_CANONICAL_BYTES) {
                    throw new IllegalArgumentException("Invalid canonical task report size");
                }
                canonical = TaskReport.parseFrom(evidence);
                if (!canonical.hasAttempt() || !isTerminal(canonical.getState())
                        || !canonical.getUnknownFields().asMap().isEmpty()) {
                    throw new IllegalArgumentException("Invalid canonical terminal task status");
                }
                attempt = canonical.getAttempt();
                state = canonical.getState();
            }
            default -> throw new IllegalArgumentException("Task status evidence is required");
        }

        var stats = decodeStats(delivery, canonical);
        var error = canonical != null && canonical.hasError() ? canonical.getError() : null;
        return new TaskStatusReport(attempt.getTaskId(), attempt.getQueryOrJobId(), attempt.getStageId(),
                attempt.getPartitionId(), role.name().substring("TASK_ROLE_".length()),
                delivery.getWorkerId(), state.name().substring("TASK_STATE_".length()), stats,
                error == null ? null
                        : new ErrorDetail(error.getCode(), error.getErrorClass().name(), error.getMessage()),
                delivery.hasWrite() ? decodeWrite(delivery.getWrite()) : null,
                delivery.getSequence(), delivery.getTimestampMs(), null, null,
                TaskAttemptDescriptor.from(attempt), evidence, attempt, canonical);
    }

    private static TaskStats decodeStats(TaskStatusDelivery delivery, TaskReport canonical) {
        if (canonical != null && canonical.hasNativeTaskStats()) {
            if (delivery.hasSupplementalStats()) {
                throw new IllegalArgumentException("Terminal delivery already carries canonical Native statistics");
            }
            return TaskStats.fromNative(canonical.getNativeTaskStats());
        }
        return delivery.hasSupplementalStats() ? TaskStats.fromNative(delivery.getSupplementalStats()) : null;
    }

    private static boolean isTerminal(TaskState state) {
        return state == TaskState.TASK_STATE_FINISHED || state == TaskState.TASK_STATE_FAILED
                || state == TaskState.TASK_STATE_CANCELLED;
    }

    private static boolean isProgress(TaskState state) {
        return state == TaskState.TASK_STATE_PENDING || state == TaskState.TASK_STATE_RUNNING
                || state == TaskState.TASK_STATE_FLUSHING;
    }

    private static NativeTaskStats encodeStats(TaskStats stats) {
        var proto = NativeTaskStats.newBuilder().setRowsRead(stats.rowsRead()).setBytesRead(stats.bytesRead())
                .setRowsWritten(stats.rowsWritten()).setBytesWritten(stats.bytesWritten())
                .setElapsedTimeMs(stats.elapsedTimeMs()).setCpuTimeMs(stats.cpuTimeMs())
                .setMemoryBytes(stats.memoryBytes()).setPeakMemoryBytes(stats.peakMemoryBytes())
                .setSpillBytes(stats.spillBytes()).setCacheHitBytes(stats.cacheHitBytes())
                .setCacheMissBytes(stats.cacheMissBytes()).setBackpressureWaitMs(stats.backpressureWaitMs())
                .setCacheHitCount(stats.cacheHitCount()).setCacheMissCount(stats.cacheMissCount())
                .setCacheEvictionCount(stats.cacheEvictionCount())
                .setCacheAdmissionRejectionCount(stats.cacheAdmissionRejectionCount());
        for (var op : stats.operatorStats()) {
            proto.addOperatorStats(org.pangrova.protocol.v2.OperatorStats.newBuilder()
                    .setOperatorType(op.operatorType()).setInputRows(op.inputRows()).setOutputRows(op.outputRows())
                    .setInputBytes(op.inputBytes()).setOutputBytes(op.outputBytes()).setCpuNanos(op.cpuNanos())
                    .setWallNanos(op.wallNanos()).setPlanNodeId(op.planNodeId()).setMemoryBytes(op.memoryBytes())
                    .setPeakMemoryBytes(op.peakMemoryBytes()).setSpillBytes(op.spillBytes()));
        }
        return proto.build();
    }

    private static TaskWriteEvidence encodeWrite(WriteResultDescriptor write) {
        if (!write.dataFiles().isEmpty()) {
            throw new IllegalArgumentException("Distributed write delivery requires connector commit evidence");
        }
        var proto = TaskWriteEvidence.newBuilder().setTotalRows(write.totalRows()).setTotalBytes(write.totalBytes())
                .setCommitMetadata(ByteString.copyFrom(write.commitMetadata()));
        for (byte[] fragment : write.commitFragments()) {
            proto.addCommitFragments(ByteString.copyFrom(fragment));
        }
        return proto.build();
    }

    private static WriteResultDescriptor decodeWrite(TaskWriteEvidence write) {
        if (!write.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Unexpected fields in task write evidence");
        }
        return new WriteResultDescriptor(List.of(), write.getTotalRows(), write.getTotalBytes(),
                write.getCommitMetadata().toByteArray(),
                write.getCommitFragmentsList().stream().map(ByteString::toByteArray).toList());
    }
}
