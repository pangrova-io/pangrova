package org.pangrova.protocol.rest.task;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.TaskReport;

import java.util.Objects;

/** One immutable terminal report and the exact bytes that produced it. */
public final class TaskReportEvidence {
    private final ByteString bytes;
    private final TaskReport report;

    private TaskReportEvidence(ByteString bytes, TaskReport report) {
        this.bytes = bytes;
        this.report = report;
    }

    /** Parses the original bytes once and retains them for exact durable replay. */
    public static TaskReportEvidence decode(ByteString bytes) throws InvalidProtocolBufferException {
        Objects.requireNonNull(bytes, "bytes");
        return new TaskReportEvidence(bytes, TaskReport.parseFrom(bytes));
    }

    /** Serializes a locally constructed report once. */
    public static TaskReportEvidence encode(TaskReport report) {
        Objects.requireNonNull(report, "report");
        return new TaskReportEvidence(report.toByteString(), report);
    }

    public ByteString bytes() { return bytes; }

    public TaskReport report() { return report; }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaskReportEvidence evidence && bytes.equals(evidence.bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }
}
