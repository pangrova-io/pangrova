package org.pangrova.coordinator.task;

import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicBoolean;
import org.pangrova.protocol.checksum.ProtocolChecksums;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskAttemptId;

/** An actual report-operation lease; its publication obligation survives failed journal/side effects. */
@javax.annotation.concurrent.NotThreadSafe
public final class TaskReportPublication implements AutoCloseable {
    private final TaskStatusReport report;
    private final boolean resourceOnly;
    private final boolean publicationCompleted;
    private final Runnable accept;
    private final Runnable exit;
    private final AtomicBoolean closed = new AtomicBoolean();

    TaskReportPublication(TaskStatusReport report, boolean resourceOnly, boolean publicationCompleted,
            Runnable accept, Runnable exit) {
        this.report = report;
        this.resourceOnly = resourceOnly;
        this.publicationCompleted = publicationCompleted;
        this.accept = accept;
        this.exit = exit;
    }

    /** Returns the original frozen report, not a newly timestamped retry. */
    public TaskStatusReport report() { return report; }
    /** Returns the first validated disposition, never one recomputed after partial side effects. */
    public boolean resourceOnly() { return resourceOnly; }
    /** Completed publication need not be repeated when only a required drain observer failed. */
    public boolean publicationCompleted() { return publicationCompleted; }
    /** Marks actual required journal and publication work complete before releasing the operation. */
    public void accepted() {
        if (closed.get()) {
            throw new IllegalStateException("Report operation already exited");
        }
        accept.run();
    }
    @Override public void close() { if (closed.compareAndSet(false, true)) exit.run(); }

    /** A single retained publication fact; mutable fields are guarded by its TaskIdentityRegistry owner. */
    @javax.annotation.concurrent.NotThreadSafe
    static final class Fact {
        SubmitTaskRequest request;
        org.pangrova.protocol.rest.task.TaskStatusReport report;
        final boolean resourceOnly;
        final long sequence;
        final long timestamp;
        final ByteString canonicalDigest;
        final org.pangrova.protocol.rest.task.TaskStats stats;
        final org.pangrova.common.client.error.ErrorDetail error;
        final org.pangrova.protocol.rest.data.WriteResultDescriptor writeResult;
        final String errorCode;
        final java.util.Map<String, String> errorMetadata;
        boolean completed;
        boolean notificationsCompleted;
        boolean notifying;
        int operations;
        final String taskId;
        final String queryId;
        final String workerId;
        final String state;
        final String taskType;
        final int stageId;
        final int partitionId;
        final TaskAttemptDescriptor attempt;
        final TaskAttemptId reportedAttempt;

        Fact(SubmitTaskRequest request,
                org.pangrova.protocol.rest.task.TaskStatusReport report, boolean resourceOnly) {
            this.request = request;
            this.report = report;
            this.resourceOnly = resourceOnly;
            sequence = report.reportSequence();
            timestamp = report.timestampMs();
            taskId = report.taskId();
            queryId = report.queryId();
            workerId = report.workerId();
            state = report.state();
            taskType = report.taskType();
            stageId = report.stageId();
            partitionId = report.partitionId();
            attempt = report.attempt();
            reportedAttempt = report.reportedAttempt();
            stats = report.stats();
            error = report.error();
            writeResult = report.writeResult();
            errorCode = report.errorCode();
            errorMetadata = report.errorMetadata();
            var canonical = report.canonicalEvidence();
            canonicalDigest = canonical == null ? null : ProtocolChecksums.sha256(canonical).getDigest();
        }

        boolean matches(org.pangrova.protocol.rest.task.TaskStatusReport retry) {
            return matchesIdentity(retry) && matchesPayload(retry)
                    && matchesCanonical(retry.canonicalEvidence());
        }

        private boolean matchesIdentity(TaskStatusReport retry) {
            return sequence == retry.reportSequence() && timestamp == retry.timestampMs()
                    && taskId.equals(retry.taskId()) && queryId.equals(retry.queryId())
                    && workerId.equals(retry.workerId()) && state.equals(retry.state())
                    && java.util.Objects.equals(taskType, retry.taskType()) && stageId == retry.stageId()
                    && partitionId == retry.partitionId() && attempt.equals(retry.attempt())
                    && java.util.Objects.equals(reportedAttempt, retry.reportedAttempt());
        }

        private boolean matchesPayload(TaskStatusReport retry) {
            return java.util.Objects.equals(stats, retry.stats())
                    && java.util.Objects.equals(error, retry.error())
                    && java.util.Objects.equals(writeResult, retry.writeResult())
                    && java.util.Objects.equals(errorCode, retry.errorCode())
                    && java.util.Objects.equals(errorMetadata, retry.errorMetadata());
        }

        private boolean matchesCanonical(ByteString canonical) {
            if (canonicalDigest == null || canonical == null) return canonicalDigest == null && canonical == null;
            return canonicalDigest.equals(ProtocolChecksums.sha256(canonical).getDigest());
        }
    }
}
