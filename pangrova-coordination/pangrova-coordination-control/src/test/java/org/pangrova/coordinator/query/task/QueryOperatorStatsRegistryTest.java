package org.pangrova.coordinator.query.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.OperatorStats;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.query.profiling.OperatorStatsSource.Attempt;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryOperatorStatsRegistryTest {

    @Test
    void aLateOlderReportCannotOverwriteTheFullLatestAttempt() {
        var registry = new QueryOperatorStatsRegistry();
        var attempt = attempt(2, 3, 4, "first");
        registry.record(attempt, report(attempt, 3, "FINISHED", stats(40)));
        registry.record(attempt, report(attempt, 2, "RUNNING", stats(10)));
        registry.record(attempt, report(attempt, 4, "FINISHED", NativeTaskStats.getDefaultInstance()));

        assertThat(registry.attemptStats("query")).singleElement().satisfies(sample -> {
            assertThat(sample.stageId()).isEqualTo(2);
            assertThat(sample.partitionId()).isEqualTo(3);
            assertThat(sample.attemptId()).isEqualTo(4);
            assertThat(sample.planEpoch()).isEqualTo(1);
            assertThat(sample.reportSequence()).isEqualTo(3);
            assertThat(sample.reportState()).isEqualTo("FINISHED");
            assertThat(sample.backpressureWaitMs()).isEqualTo(5);
            assertThat(sample.operators()).singleElement().satisfies(metric ->
                    assertThat(metric.outputRows()).isEqualTo(40));
        });
    }

    @Test
    void sameNamedOperatorsAndRetriedAttemptsRemainDistinct() {
        var registry = new QueryOperatorStatsRegistry();
        var original = attempt(0, 2, 0, "original");
        var retry = attempt(0, 2, 1, "retry");
        var downstream = attempt(1, 2, 0, "downstream");
        var twoOperators = stats(8).toBuilder().addOperatorStats(OperatorStats.newBuilder()
                .setPlanNodeId("node-1").setOperatorType("Project").setOutputRows(7)).build();
        registry.record(downstream, report(downstream, 1, "FINISHED", stats(11)));
        registry.record(original, report(original, 3, "CANCELLED", stats(6)));
        registry.record(retry, report(retry, 3, "FINISHED", twoOperators));

        assertThat(registry.attemptStats("query")).extracting(Attempt::taskId)
                .containsExactly("original", "retry", "downstream");
        assertThat(registry.attemptStats("query").get(1).operators()).hasSize(2);
    }

    @Test
    void absentAndEmptyReportsProvideNoTelemetry() {
        var registry = new QueryOperatorStatsRegistry();
        var attempt = attempt(0, 0, 0, "empty");
        registry.record(attempt, report(attempt, 1, "RUNNING", NativeTaskStats.getDefaultInstance()));
        assertThat(registry.attemptStats("absent")).isEmpty();
        assertThat(registry.attemptStats("query")).isEmpty();
    }

    private static TaskAttemptId attempt(int stage, int partition, int number, String taskId) {
        return TaskAttemptId.newBuilder().setQueryOrJobId("query").setStageId(stage).setPartitionId(partition)
                .setPlanEpoch(1).setAttemptId(number).setFencingToken(1).setTaskId(taskId).build();
    }

    private static TaskStatusReport report(TaskAttemptId attempt, long sequence, String state, NativeTaskStats stats) {
        return new TaskStatusReport(attempt.getTaskId(), attempt.getQueryOrJobId(), attempt.getStageId(),
                attempt.getPartitionId(), "COMPUTE", "worker", state, TaskStats.fromNative(stats),
                null, null, sequence, 1L, null, Map.of(),
                new TaskAttemptDescriptor(attempt.getPlanEpoch(), attempt.getAttemptId(), attempt.getFencingToken()));
    }

    private static NativeTaskStats stats(long rows) {
        return NativeTaskStats.newBuilder().setBackpressureWaitMs(5)
                .addOperatorStats(OperatorStats.newBuilder().setPlanNodeId("node-1")
                        .setOperatorType("TableScan").setOutputRows(rows)).build();
    }
}
