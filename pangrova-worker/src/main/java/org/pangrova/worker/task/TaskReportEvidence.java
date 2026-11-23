package org.pangrova.worker.task;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.TaskReport;

/** Carries the immutable bytes and their already parsed report through Worker ownership. */
record TaskReportEvidence(ByteString bytes, TaskReport report) {

    static TaskReportEvidence encode(TaskReport report) {
        return new TaskReportEvidence(report.toByteString(), report);
    }
}
