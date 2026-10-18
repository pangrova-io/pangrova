package org.pangrova.testing.e2e.speculation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.task.report.CanonicalTaskReportValidator;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.E2ETest;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Formal same-source Coordinator and two independent Worker processes, with actual SQL and network faults. */
@E2ETest
class IndependentSpeculativeExecutionE2ETest extends ContainerTestBase {
    private static final String READ_SQL = "SELECT MAX(payload) FROM spec_catalog.default.input";
    private static IndependentSpeculationProcesses processes;

    @BeforeAll
    static void startActualProcesses() throws Exception {
        processes = new IndependentSpeculationProcesses();
        processes.start();
    }

    @AfterAll
    static void closeActualProcesses() throws Exception {
        if (processes != null) processes.close();
    }

    @Test
    void slowTask_specLaunched_winnerKept_loserCancelled() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.NONE, false)) {
            var original = scenario.original();
            var copy = scenario.copy();
            scenario.report(id(copy), "FINISHED");
            await("actual losing original cancellation").atMost(Duration.ofSeconds(60))
                    .until(() -> scenario.successfulCancelCalls(id(original)) > 0);
            scenario.awaitExecutionDrain(id(original));
            assertRead(scenario.complete(true));
        }
    }

    @Test
    void originalWins_specCancelled() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.NONE, true)) {
            var original = scenario.original();
            var copy = scenario.copy();
            scenario.report(id(original), "FINISHED");
            var delayedCopyReport = scenario.report(id(copy), "FINISHED");
            scenario.releaseOriginal();
            await("actual losing copy cancellation").atMost(Duration.ofSeconds(60))
                    .until(() -> scenario.successfulCancelCalls(id(copy)) > 0);
            scenario.awaitExecutionDrain(id(copy));
            assertRead(scenario.complete(true));
            await("retired speculative loser terminal ACK").atMost(Duration.ofSeconds(60))
                    .untilAsserted(() -> {
                        var response = delayedCopyReport.response().get();
                        assertThat(response).isNotNull();
                        assertThat(response.statusCode()).isEqualTo(200);
                    });
        }
    }

    @Test
    void speculativeWinnerCompletesOriginalDagSlotWithoutFailingQuery() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.NONE, false)) {
            var original = scenario.original();
            var copy = scenario.copy();
            var actual = scenario.report(id(copy), "FINISHED");
            var canonical = new CanonicalTaskReportValidator().validate(actual.report(), copy.request()).orElseThrow();
            assertThat(canonical.getAttempt().getStageId()).isEqualTo(original.request().taskRequest().getAttempt().getStageId());
            assertThat(canonical.getAttempt().getPartitionId()).isEqualTo(original.request().taskRequest().getAttempt().getPartitionId());
            assertRead(scenario.complete(true));
            scenario.awaitExecutionDrain(id(original));
        }
    }

    @Test
    void failedCopyDoesNotFailOriginalLogicalTask() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.COPY_READ, false)) {
            var original = scenario.original();
            var copy = scenario.copy();
            var failed = scenario.report(id(copy), "FAILED");
            assertThat(failed.report().error()).isNotNull();
            scenario.restoreFiles();
            scenario.releaseOriginal();
            assertRead(scenario.complete(true));
            scenario.awaitExecutionDrain(id(copy));
            assertThat(processes.queryStatus(queryId(original)).error()).isNull();
        }
    }

    @Test
    void failedOriginalCanBeReplacedBySuccessfulCopy() throws Exception {
        String table = createOutputTable();
        try (var scenario = new SpeculativeProcessScenario(
                processes, SpeculativeProcessScenario.Fault.ORIGINAL_READ, false, insertSql(table))) {
            var original = scenario.original();
            var copy = scenario.copy();
            assertThat(scenario.report(id(original), "FAILED").report().error()).isNotNull();
            scenario.report(id(copy), "FINISHED");
            scenario.complete(true);
            assertThat(processes.input().committedRows(table)).isEqualTo(ProcessInputSnapshot.ROW_COUNT);
            scenario.awaitExecutionDrain(id(original));
            assertThat(processes.queryStatus(queryId(original)).error()).isNull();
        }
    }

    @Test
    void bothFailedAttemptsFailTheirLogicalTaskOnce() throws Exception {
        String table = createOutputTable();
        try (var scenario = new SpeculativeProcessScenario(
                processes, SpeculativeProcessScenario.Fault.BOTH_READ, true, insertSql(table))) {
            var original = scenario.original();
            var copy = scenario.copy();
            var first = scenario.report(id(original), "FAILED");
            var second = scenario.report(id(copy), "FAILED");
            scenario.complete(false);
            var terminal = processes.queryStatus(queryId(original));
            assertThat(terminal.error()).isNotNull();
            processes.replayReport(first);
            processes.replayReport(second);
            var repeated = processes.queryStatus(queryId(original));
            assertThat(repeated.state()).isEqualTo(terminal.state());
            assertThat(repeated.error()).isEqualTo(terminal.error());
            assertThat(processes.activeTenantReservations()).isZero();
            scenario.awaitExecutionDrain(id(original));
            scenario.awaitExecutionDrain(id(copy));
        }
    }

    @Test
    void concurrentSpeculativeLoserDoesNotContributeWriteFragment() throws Exception {
        String table = createOutputTable();
        try (var scenario = new SpeculativeProcessScenario(processes, SpeculativeProcessScenario.Fault.NONE, true,
                insertSql(table))) {
            var original = scenario.original();
            var copy = scenario.copy();
            var first = scenario.report(id(original), "FINISHED");
            var second = scenario.report(id(copy), "FINISHED");
            assertThat(first.report().writeResult()).isNotNull();
            assertThat(second.report().writeResult()).isNotNull();
            assertThat(first.report().writeResult().commitMetadata()).isNotEmpty();
            assertThat(second.report().writeResult().commitMetadata()).isNotEmpty();
            releaseConcurrently(scenario);
            scenario.complete(true);
            assertThat(processes.input().committedRows(table)).isEqualTo(ProcessInputSnapshot.ROW_COUNT);
            scenario.awaitExecutionDrain(id(original));
            scenario.awaitExecutionDrain(id(copy));
        }
    }

    @Test
    void launchSpeculative_withDispatchDeps_actuallyDispatchesToDifferentWorker() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.NONE, false)) {
            var original = scenario.original();
            var copy = scenario.copy();
            assertThat(copy.workerId()).isNotEqualTo(original.workerId());
            assertThat(processes.workers().stream().map(worker -> worker.process().get().getContainerId()).distinct()).hasSize(2);
            assertThat(processes.workers().stream().map(worker -> worker.http().port()).distinct()).hasSize(2);
            scenario.report(id(copy), "FINISHED");
            assertRead(scenario.complete(true));
        }
    }

    @Test
    void failedSpeculativeDispatchDoesNotFailOriginalQuery() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.COPY_DISPATCH, false)) {
            var original = scenario.original();
            var copy = scenario.copy();
            assertThat(copy.transfer().response().get()).as("Real connection closed before Worker received POST").isNull();
            scenario.awaitExecutionDrain(id(copy));
            scenario.releaseOriginal();
            assertRead(scenario.complete(true));
            assertThat(processes.queryStatus(queryId(original)).error()).isNull();
        }
    }

    @Test
    void resourceCleanup() throws Exception {
        try (var scenario = scenario(SpeculativeProcessScenario.Fault.NONE, false)) {
            var original = scenario.original();
            var copy = scenario.copy();
            var actual = scenario.report(id(copy), "FINISHED");
            assertRead(scenario.complete(true));
            for (var issued : processes.issuedTasks()) {
                if (queryId(issued).equals(queryId(original))) scenario.awaitExecutionDrain(id(issued));
            }
            long completedCancels = scenario.successfulCancelCalls(id(original));
            processes.replayReport(actual);
            await("No cancellation retry after actual drain").during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(15))
                    .until(() -> scenario.successfulCancelCalls(id(original)) == completedCancels);
            assertThat(processes.activeTenantReservations()).isZero();
            assertThat(processes.queryStatus(queryId(original)).state()).isEqualTo("FINISHED");
        }
    }

    private static SpeculativeProcessScenario scenario(SpeculativeProcessScenario.Fault fault, boolean holdCopy) {
        return new SpeculativeProcessScenario(processes, fault, holdCopy, READ_SQL);
    }

    private static String createOutputTable() throws Exception {
        String table = "spec_output_" + java.util.UUID.randomUUID().toString().replace("-", "");
        var create = processes.query("CREATE TABLE spec_catalog.default." + table + " (id BIGINT, payload VARCHAR)")
                .get(5, TimeUnit.MINUTES);
        assertThat(create.succeeded()).as(create.output() + create.errors()).isTrue();
        return table;
    }

    private static String insertSql(String table) {
        return "INSERT INTO spec_catalog.default." + table + " SELECT id, payload FROM spec_catalog.default.input";
    }

    private static void releaseConcurrently(SpeculativeProcessScenario scenario) throws Exception {
        var executor = ManagedExecutorFactory.newBoundedThreadPool("actual-speculative-report-release", 2, 2);
        try {
            CompletableFuture.allOf(CompletableFuture.runAsync(scenario::releaseOriginal, executor),
                    CompletableFuture.runAsync(scenario::releaseCopy, executor)).get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).as("Actual report release owners exited").isTrue();
        }
    }

    private static String id(IndependentSpeculationProcesses.IssuedTask task) {
        return task.request().taskRequest().getAttempt().getTaskId();
    }

    private static String queryId(IndependentSpeculationProcesses.IssuedTask task) {
        return task.request().taskRequest().getAttempt().getQueryOrJobId();
    }

    private static void assertRead(IndependentSpeculationProcesses.SqlOutput output) {
        assertThat(output.output()).contains(ProcessInputSnapshot.PAYLOAD, "1 row");
        assertThat(output.errors()).isEmpty();
    }
}
