package org.pangrova.worker.property.task;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;
import org.pangrova.worker.task.ActualTaskManagerFixture;
import org.pangrova.worker.task.TaskManager;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Real JNI task completion, data, failure and report properties; no simulated executor. */
@Tag("native-required")
@org.junit.jupiter.api.Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerExecutionPropertyTest {
    private static final String WORKER_ID = "test-worker-1";

    @Property(tries = 10)
    void executionLoopCompletion(@ForAll("batchCounts") int batches) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, WORKER_ID)) {
            var request = runtime.bindWorker(ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.values(batches, 3)));
            var info = tasks.submit(request);
            tasks.awaitState(info, org.pangrova.worker.task.state.TaskState.RUNNING);
            var received = tasks.consume(info);
            tasks.awaitState(info, org.pangrova.worker.task.state.TaskState.FINISHED);
            assertThat(received.batches()).isEqualTo(batches);
            assertThat(received.rows()).isEqualTo(batches * 3L);
            assertThat(received.values().stream().sorted())
                    .containsExactlyElementsOf(LongStream.range(0, batches * 3L).boxed().toList());
            assertThat(info.executorHandle()).isZero();
            assertThat(tasks.manager().getRunningTaskCount()).isZero();
        }
    }

    @Property(tries = 10)
    void errorPropagation(@ForAll("errorBatchIndices") int successfulSources) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, WORKER_ID)) {
            var request = runtime.bindWorker(ActualNativeTaskFixture.planTaskRequest(false,
                    ActualNativeTaskPlans.failingCast(successfulSources)));
            var info = tasks.submit(request);
            tasks.awaitState(info, org.pangrova.worker.task.state.TaskState.FAILED);
            var report = TaskReport.parseFrom(tasks.terminalReport(info).canonicalTaskReport());
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
            assertThat(report.getCoverage().getCompletedWorkUnits()).isZero();
            assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(report.getError().getMessage()).contains("not-an-integer");
            assertThat(info.executorHandle()).isZero();
            assertThat(tasks.manager().getRunningTaskCount()).isZero();
            assertThat(tasks.reports().reports()).noneMatch(value -> value.state().equals("FINISHED"));
        }
    }

    @Property(tries = 10)
    void statusReportingOnStateChange(@ForAll("batchCounts") int batches) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, WORKER_ID)) {
            var request = runtime.bindWorker(ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.values(batches, 1)));
            var info = tasks.submit(request);
            tasks.awaitState(info, org.pangrova.worker.task.state.TaskState.RUNNING);
            assertThat(tasks.consume(info).rows()).isEqualTo(batches);
            tasks.awaitState(info, org.pangrova.worker.task.state.TaskState.FINISHED);
            var reports = tasks.reports().reports();
            assertThat(reports).hasSizeGreaterThanOrEqualTo(2);
            assertThat(reports.get(0).state()).isEqualTo("RUNNING");
            assertThat(reports.get(0).queryId()).isEqualTo(request.getAttempt().getQueryOrJobId());
            assertThat(reports.get(0).taskId()).isEqualTo(request.getAttempt().getTaskId());
            assertThat(reports.get(0).workerId()).isEqualTo(WORKER_ID);
            var finished = tasks.terminalReport(info);
            assertThat(finished.state()).isEqualTo("FINISHED");
            assertThat(finished.reportSequence()).isGreaterThan(reports.get(0).reportSequence());
            assertThat(TaskReport.parseFrom(finished.canonicalTaskReport()).getAttempt()).isEqualTo(request.getAttempt());
        }
    }

    @Property(tries = 10)
    void statusReportingOnError(@ForAll("errorBatchIndices") int successfulSources) throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, WORKER_ID)) {
            var request = runtime.bindWorker(ActualNativeTaskFixture.planTaskRequest(false,
                    ActualNativeTaskPlans.failingCast(successfulSources)));
            var info = tasks.submit(request);
            tasks.awaitState(info, org.pangrova.worker.task.state.TaskState.FAILED);
            var reports = tasks.reports().reports();
            assertThat(reports).hasSizeGreaterThanOrEqualTo(2);
            assertThat(reports.get(0).state()).isEqualTo("RUNNING");
            var failed = tasks.terminalReport(info);
            assertThat(failed.state()).isEqualTo("FAILED");
            assertThat(failed.taskId()).isEqualTo(request.getAttempt().getTaskId());
            assertThat(failed.queryId()).isEqualTo(request.getAttempt().getQueryOrJobId());
            assertThat(failed.error()).isNotNull();
            assertThat(failed.reportSequence()).isGreaterThan(reports.get(0).reportSequence());
            assertThat(TaskReport.parseFrom(failed.canonicalTaskReport()).getError().getMessage())
                    .contains("not-an-integer");
            assertThat(reports).noneMatch(value -> value.state().equals("FINISHED"));
        }
    }

    @Provide Arbitrary<Integer> batchCounts() { return Arbitraries.integers().between(0, 10); }
    @Provide Arbitrary<Integer> errorBatchIndices() { return Arbitraries.integers().between(0, 5); }
}
