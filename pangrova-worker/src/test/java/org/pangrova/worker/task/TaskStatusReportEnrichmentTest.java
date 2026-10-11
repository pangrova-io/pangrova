package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.protocol.v2.WriteTaskOptions;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import java.util.List;

/**
 * Actual TaskManager reports; the observer only records production emissions. Successful connector
 * write/identical receipt resend oracles require the real connector E2E migration.
 */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskStatusReportEnrichmentTest {
    @Test
    void finishedReportContainsNonNullStatsAndIncreasingSequence() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "enrichment-stats")) {
            var info = tasks.submit(runtime.valuesRequest(false));
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            var reports = taskReports(tasks, info);
            assertThat(reports).hasSizeGreaterThanOrEqualTo(2);
            var running = reports.get(0);
            assertThat(running.state()).isEqualTo("RUNNING");
            assertThat(running.stats()).isNotNull();
            assertThat(running.reportSequence()).isEqualTo(1);
            var finished = tasks.terminalReport(info);
            assertThat(finished.stats()).isNotNull();
            assertThat(finished.stats().elapsedTimeMs()).isGreaterThanOrEqualTo(0);
            assertThat(finished.reportSequence()).isGreaterThan(running.reportSequence());
            assertThat(TaskReport.parseFrom(finished.canonicalTaskReport()).getState())
                    .isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(finished.stats().operatorStats()).isNotEmpty();
            assertThat(info.executorHandle()).isZero();
        }
    }

    @Test
    void finishedReportContainsOutputRowsFromActualNativeOutput() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "enrichment-rows")) {
            var request =
                    ActualNativeTaskFixture.planTaskRequest(
                            true, ActualNativeTaskPlans.values(5, 2));
            var info = tasks.submit(request);
            assertThat(tasks.consume(info).rows()).isEqualTo(10);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            // Output rows are the terminal relation cardinality, not source/each-operator sums.
            tasks.manager().statusCollector.reportStatus(info, "FINISHED", null);
            var finished = taskReports(tasks, info).get(taskReports(tasks, info).size() - 1);
            assertThat(finished.stats()).isNotNull();
            assertThat(finished.stats().operatorStats())
                    .filteredOn(operator -> operator.operatorType().equals("PartitionedOutput"))
                    .singleElement()
                    .satisfies(operator -> assertThat(operator.inputRows()).isEqualTo(10));
            assertThat(TaskReport.parseFrom(finished.canonicalTaskReport()).hasNativeTaskStats())
                    .isTrue();
        }
    }

    @Test
    void writeTaskWithoutConnectorRuntimeFailsClosed() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "enrichment-unbound-write")) {
            var request =
                    runtime.valuesRequest(false).toBuilder().setTaskKind(TaskKind.TASK_KIND_WRITE);
            request.getExecutionOptionsBuilder()
                    .setWrite(
                            WriteTaskOptions.newBuilder()
                                    .setTargetTable("unbound.default.items")
                                    .setWriteMode("INSERT")
                                    .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                                    .setTargetFileSize(128_000_000));
            var info = tasks.submit(request.build());
            tasks.awaitState(info, TaskManager.TaskState.FAILED);
            assertThat(taskReports(tasks, info))
                    .noneMatch(report -> report.state().equals("FINISHED"));
            var failure = tasks.terminalReport(info);
            assertThat(failure.error()).isNotNull();
            assertThat(failure.canonicalTaskReport()).isNotEmpty();
            assertThat(TaskReport.parseFrom(failure.canonicalTaskReport()).getState())
                    .isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(info.executorHandle()).isZero();
            assertThat(info.allocatedBufferMemory()).isZero();
        }
    }

    @Test
    void computeTaskReportHasNullWriteResult() {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "enrichment-compute")) {
            var info = tasks.submit(runtime.valuesRequest(false));
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(tasks.terminalReport(info).writeResult()).isNull();
        }
    }

    @Test
    void sequenceIsMonotonicallyIncreasingAcrossReports() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "enrichment-sequence")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            tasks.manager().statusCollector.reportStatus(info, "RUNNING", null);
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            tasks.manager().statusCollector.reportStatus(info, "FINISHED", null);
            var reports = taskReports(tasks, info);
            assertThat(reports).hasSizeGreaterThanOrEqualTo(3);
            for (int i = 1; i < reports.size(); i++) {
                assertThat(reports.get(i).reportSequence())
                        .isGreaterThan(reports.get(i - 1).reportSequence());
            }
        }
    }

    private static List<TaskStatusReport> taskReports(
            ActualTaskManagerFixture tasks, TaskInfo info) {
        return tasks.reports().reports().stream()
                .filter(
                        report ->
                                report.taskId()
                                        .equals(
                                                info.request()
                                                        .taskRequest()
                                                        .getAttempt()
                                                        .getTaskId()))
                .toList();
    }
}
