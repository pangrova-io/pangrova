package org.pangrova.coordinator.testinfra;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.TestObservability;
import org.pangrova.worker.jni.NativeRuntime;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@org.junit.jupiter.api.Tag("native-required")
class SourceFreeNativeWorkerFixtureIntegrationTest {
    @TempDir Path directory;

    @Test
    void closeAndReinitializeExecutesConstantsAndCapturesActualNativeTerminalReports() throws Exception {
        runConstant("first");
        assertThat(NativeRuntime.isInitialized()).isFalse();
        runConstant("second");
        assertThat(NativeRuntime.isInitialized()).isFalse();
    }

    @Test
    void activeOwnerCannotBeBorrowedOrShutDownByAnotherFixture() throws Exception {
        try (var worker = new SourceFreeNativeWorkerFixture("owner", directory)) {
            assertThatThrownBy(() -> new SourceFreeNativeWorkerFixture("intruder", directory))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("another Native runtime owner");
            assertThat(NativeRuntime.isInitialized()).isTrue();
        }
        assertThat(NativeRuntime.isInitialized()).isFalse();
    }

    @Test
    void closingUnconsumedOutputDrainsActualReportsAndAllowsReinitialization() throws Exception {
        try (var worker = new SourceFreeNativeWorkerFixture("w1", directory.resolve("unconsumed"));
                var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            remote.setControlEpochSupplier(() -> 1L);
            remote.setExecutionOwnerResolver(ignored -> new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                    "test-coordinator", "test-incarnation", 1L));
            var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
            workers.registerWorker(worker.workerInfo());
            var tracker = new CoordinatorTaskTracker();
            var dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(
                    QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, remote,
                            new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker,
                            null, null, null, 30L)), null, null);
            var queries = worker.connectAsync(dispatcher, tracker, workers);
            var response = queries.submitQuery("unconsumed", "SELECT 1", SourceFreeQueryFixture.session("unconsumed"));
            assertThat(response.resultEndpoint()).isNotNull();
            var issuedIds = tracker.getTaskIds("unconsumed");
            assertThat(issuedIds).hasSize(1);
            String issuedTaskId = issuedIds.iterator().next();
            var issued = tracker.getTaskRequest(issuedTaskId).taskRequest();
            assertThat(response.resultEndpoint().ticket()).isEqualTo(worker.resultTicket(issued));
            assertThat(response.resultEndpoint().ticket()).isNotEqualTo(issuedTaskId);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
                    .until(() -> worker.taskManager().getRunningTaskCount() == 1);
            assertThat(worker.taskManager().getRunningTaskCount()).isEqualTo(1);
            worker.close();
            assertThat(NativeRuntime.isInitialized()).isFalse();
            assertThat(worker.reports()).anySatisfy(report -> {
                assertThat(report.taskId()).isEqualTo(issuedTaskId);
                assertThat(report.state()).isEqualTo("CANCELLED");
                assertThat(report.canonicalTaskReport()).isNotEmpty();
                assertThat(TaskReport.parseFrom(report.canonicalTaskReport()).getAttempt())
                        .isEqualTo(issued.getAttempt());
            });
        }
        runConstant("after-unconsumed");
    }

    private void runConstant(String queryId) throws Exception {
        try (var worker = new SourceFreeNativeWorkerFixture("w1", directory.resolve(queryId));
                var remote = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING)) {
            remote.setControlEpochSupplier(() -> 1L);
            remote.setExecutionOwnerResolver(ignored -> new org.pangrova.protocol.rest.task.ExecutionOwnerIdentity(
                    "test-coordinator", "test-incarnation", 1L));
            var workers = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
            workers.registerWorker(worker.workerInfo());
            var tracker = new CoordinatorTaskTracker();
            var dispatcher = new QueryDispatcher(SourceFreeQueryFixture.withPolicies(
                    QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null, null, remote,
                            new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker,
                            null, null, null, 30L)), null, null);
            worker.connect(dispatcher, tracker, workers);
            var result = worker.execute(dispatcher, new QueryStateMachine(), "SELECT 42",
                    SourceFreeQueryFixture.session(queryId));
            assertThat(worker.rows(result)).singleElement()
                    .satisfies(row -> assertThat(((Number) row.get(0)).intValue()).isEqualTo(42));
            var terminal = worker.reports().stream().filter(report -> report.state().equals("FINISHED"))
                    .findFirst().orElseThrow();
            TaskReport report = TaskReport.parseFrom(terminal.canonicalTaskReport());
            assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(report.getAttempt().getTaskId()).isEqualTo(terminal.taskId());
            assertThat(dispatcher.dagEngine().isQueryComplete(queryId)).isTrue();
        }
    }
}
