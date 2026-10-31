package org.pangrova.protocol.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

import static org.assertj.core.api.Assertions.assertThat;

class TaskReportOriginRoundTripTest {

    @Test
    void nativeAndWorkerOriginsRemainDistinctAcrossSerialization() throws Exception {
        TaskReport nativeReport = TaskReport.newBuilder()
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setState(TaskState.TASK_STATE_FINISHED)
                .build();
        TaskReport reconciledReport = TaskReport.newBuilder()
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED)
                .setState(TaskState.TASK_STATE_FAILED)
                .build();

        assertThat(TaskReport.parseFrom(nativeReport.toByteArray()).getReportOrigin())
                .isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        assertThat(TaskReport.parseFrom(reconciledReport.toByteArray()).getReportOrigin())
                .isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
    }
}
