package org.pangrova.worker.task;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.CoverageReport;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;

/** Terminal Native envelopes for TaskManager tests using a small executor fake. */
final class CanonicalTerminalTaskTestFixture {

    private CanonicalTerminalTaskTestFixture() {}

    static byte[] finishedReport(byte[] taskRequestBytes) {
        try {
            TaskRequest request = TaskRequest.parseFrom(taskRequestBytes);
            long workUnits = Math.max(1, request.getSplitsCount());
            return TaskReport.newBuilder()
                    .setReportOrigin(org.pangrova.protocol.v2.TaskReportOrigin
                            .TASK_REPORT_ORIGIN_NATIVE)
                    .setAttempt(request.getAttempt())
                    .setState(TaskState.TASK_STATE_FINISHED)
                    .setCoverage(CoverageReport.newBuilder()
                            .setCompletionState(CompletionState.COMPLETION_STATE_COMPLETE)
                            .setExpectedWorkUnits(workUnits)
                            .setCompletedWorkUnits(workUnits)
                            .setCoverageRatio(1.0))
                    .build().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Test task request is not canonical protobuf", e);
        }
    }

    static byte[] finishedStats() {
        return NativeTaskStats.newBuilder().setElapsedTimeMs(1).build().toByteArray();
    }
}
