package org.pangrova.coordinator.query.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.protocol.task.TerminalReportDeliveryPolicy;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.rest.task.TaskStatusReportResponse;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.TestObservability;
import org.pangrova.worker.task.RemoteTaskStatusReporter;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Preserves eight delivery-policy scenarios using actual reports, handlers and network faults. */
@Tag("native-required")
@Tag("routing-integration")
class RemoteStatusReportDeliveryIntegrationTest {
    @TempDir Path directory;

    @Test
    void finalStateReport_retriesOnNetworkFailure_eventuallyDelivers() throws Exception {
        try (var fixture = scenario("FINISHED", true, 2, 0)) {
            fixture.finish();
            assertActualDrained(fixture.actualReleasedReport("FINISHED"), TaskState.TASK_STATE_FINISHED);
            awaitAttempts(fixture, 3);
            assertThat(fixture.relay().matchingAttempts()).hasSize(3);
            assertThat(accepted(fixture.relay().matchingAttempts())).isEqualTo(1);
            assertThat(fixture.failedReports()).isZero();
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    @Test
    void finalStateReport_exhaustsRetries_recordsFailureMetric() throws Exception {
        try (var fixture = scenario("FAILED", true, Integer.MAX_VALUE, 0)) {
            fixture.failDuringExecution();
            var report = fixture.actualReleasedReport("FAILED");
            var canonical = assertActualDrained(report, TaskState.TASK_STATE_FAILED);
            assertThat(canonical.hasError()).isTrue();
            assertThat(canonical.getError().getMessage().toLowerCase(java.util.Locale.ROOT))
                    .contains("division by zero");
            assertThat(fixture.awaitDeliveryFailure()).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not acknowledged");
            assertThat(fixture.relay().matchingAttempts()).hasSize(TerminalReportDeliveryPolicy.ATTEMPTS_PER_ROUND);
            assertThat(fixture.failedReports()).isEqualTo(1);
            assertThat(fixture.queries().getActiveQueryCount()).isPositive();
            fixture.recoverAndDrain();
            assertThat(fixture.queries().getActiveQueryCount()).isZero();
        }
    }

    @Test
    void cancelledReport_retriesOnTransientFailure() throws Exception {
        try (var fixture = scenario("CANCELLED", true, 2, 0)) {
            fixture.cancel();
            assertActualDrained(fixture.actualReleasedReport("CANCELLED"), TaskState.TASK_STATE_CANCELLED);
            awaitAttempts(fixture, 3);
            assertThat(fixture.relay().matchingAttempts()).hasSize(3);
            assertThat(accepted(fixture.relay().matchingAttempts())).isEqualTo(1);
            assertThat(fixture.failedReports()).isZero();
            fixture.recoverAndDrain();
        }
    }

    @Test
    void nonFinalStateReport_doesNotRetry() throws Exception {
        try (var fixture = scenario("SCHEDULED", false, 1, 0)) {
            fixture.finish();
            fixture.actualReleasedReport("FINISHED");
            var actual = fixture.actualRunningReport();
            // Explicit transport input: the current collector does not emit SCHEDULED.
            // All issued identity fields are retained; this is not Native state evidence.
            var scheduled = new TaskStatusReport(actual.taskId(), actual.queryId(), actual.stageId(),
                    actual.partitionId(), actual.taskType(), actual.workerId(), "SCHEDULED", actual.stats(),
                    actual.error(), actual.writeResult(), actual.reportSequence(), actual.timestampMs(),
                    actual.errorCode(), actual.errorMetadata(), actual.attempt(), actual.canonicalTaskReport());
            fixture.reporter().report(scheduled);
            assertThat(fixture.relay().matchingAttempts()).hasSize(1);
            assertThat(fixture.failedReports()).isZero();
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    @Test
    void finalStateReport_unreachableHost_exhaustsRetriesAndDoesNotHideFailure() throws Exception {
        try (var fixture = scenario("FINISHED", true, 0, 0)) {
            fixture.finish();
            TaskStatusReport actual = fixture.actualReleasedReport("FINISHED");
            fixture.recoverAndDrain();
            String closedAddress = fixture.relay().uri().toString();
            fixture.relay().close();
            try (var defaultReporter = new RemoteTaskStatusReporter(
                    closedAddress, TestObservability.TRACING, NativeReportDeliveryFixture.SECRET)) {
                assertThatThrownBy(() -> defaultReporter.report(actual))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("not acknowledged");
            }
            assertFortyTwo(fixture);
        }
    }

    @Test
    void finalStateReport_retriesPreserveActualReportSequenceAndFullBodyAfterLostAck() throws Exception {
        try (var fixture = scenario("FINISHED", true, 0, 2)) {
            fixture.finish();
            TaskStatusReport actual = fixture.actualReleasedReport("FINISHED");
            awaitAttempts(fixture, 3);
            var attempts = fixture.relay().matchingAttempts();
            assertThat(attempts).hasSize(3);
            assertThat(accepted(attempts)).isEqualTo(3);
            byte[] original = JsonMapperFactory.shared().writeValueAsBytes(actual);
            for (var attempt : attempts) {
                assertThat(attempt.body()).containsExactly(original);
                var report = JsonMapperFactory.shared().readValue(attempt.body(), TaskStatusReport.class);
                assertThat(report.reportSequence()).isEqualTo(actual.reportSequence());
                assertThat(report.canonicalTaskReport()).containsExactly(actual.canonicalTaskReport());
            }
            assertThat(fixture.failedReports()).isZero();
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    @Test
    void finalStateReport_actualUnregisteredWorker409IsUndeliveredAndRetries() throws Exception {
        try (var fixture = scenario("FINISHED", true, 0, 0)) {
            fixture.unregisterBeforeDrain();
            fixture.finish();
            var actual = fixture.actualReleasedReport("FINISHED");
            assertActualDrained(actual, TaskState.TASK_STATE_FINISHED);
            assertThat(fixture.awaitDeliveryFailure()).isInstanceOf(IllegalStateException.class);
            assertThat(fixture.relay().matchingAttempts()).hasSize(TerminalReportDeliveryPolicy.ATTEMPTS_PER_ROUND);
            assertThat(fixture.relay().matchingAttempts()).allSatisfy(attempt ->
                    assertThat(attempt.response().get().statusCode()).isEqualTo(409));
            assertThat(fixture.failedReports()).isEqualTo(1);
            assertThat(fixture.queries().getActiveQueryCount()).isPositive();
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    @Test
    void statusReportCarriesCredentialVerifiedByTheActualInternalAuthenticationFilter() throws Exception {
        try (var fixture = scenario("FINISHED", true, 0, 0)) {
            fixture.finish();
            var actual = fixture.actualReleasedReport("FINISHED");
            awaitAttempts(fixture, 1);
            var attempt = fixture.relay().matchingAttempts().get(0);
            assertThat(attempt.authorization()).isEqualTo("Bearer " + NativeReportDeliveryFixture.SECRET);
            assertThat(accepted(List.of(attempt))).isEqualTo(1);
            var unauthenticated = HttpRequest.newBuilder(fixture.coordinatorUri().resolve("/v1/task/status-report"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(JsonMapperFactory.shared().writeValueAsBytes(actual)))
                    .build();
            var rejected = fixture.http().httpClient().send(unauthenticated, HttpResponse.BodyHandlers.ofByteArray());
            assertThat(rejected.statusCode()).isEqualTo(401);
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    @Test
    void actualRunningReportUsesItsProductionOneRetryPolicy() throws Exception {
        try (var fixture = scenario("RUNNING", false, 1, 0)) {
            fixture.finish();
            fixture.actualReleasedReport("FINISHED");
            awaitAttempts(fixture, 2);
            assertThat(fixture.relay().matchingAttempts()).hasSize(2);
            assertThat(accepted(fixture.relay().matchingAttempts())).isEqualTo(1);
            assertThat(fixture.actualRunningReport().state()).isEqualTo("RUNNING");
            assertThat(fixture.failedReports()).isZero();
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    private NativeReportDeliveryFixture scenario(String state, boolean drained, int before, int after)
            throws Exception {
        return new NativeReportDeliveryFixture(directory, state, drained, before, after);
    }

    private static void awaitAttempts(NativeReportDeliveryFixture fixture, int count) {
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
            var attempts = fixture.relay().matchingAttempts();
            return attempts.size() >= count && attempts.get(count - 1).response().get() != null;
        });
    }

    private static long accepted(List<StatusReportHttpRelay.Attempt> attempts) {
        return attempts.stream().filter(attempt -> {
            var response = attempt.response().get();
            if (response == null || response.statusCode() != 200) return false;
            try {
                var ack = JsonMapperFactory.shared().readValue(response.body(), TaskStatusReportResponse.class);
                var report = JsonMapperFactory.shared().readValue(attempt.body(), TaskStatusReport.class);
                return ack.taskId().equals(report.taskId()) && ack.command().equals("CONTINUE");
            } catch (java.io.IOException failure) {
                throw new IllegalStateException("Actual Coordinator returned malformed metadata ACK", failure);
            }
        }).count();
    }

    private static TaskReport assertActualDrained(TaskStatusReport actual, TaskState state) throws Exception {
        var canonical = TaskReport.parseFrom(actual.canonicalTaskReport());
        assertThat(canonical.getState()).isEqualTo(state);
        assertThat(canonical.getAttempt().getTaskId()).isEqualTo(actual.taskId());
        assertThat(canonical.getExecutionDrainState()).isIn(
                ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED,
                ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
        return canonical;
    }

    private static void assertFortyTwo(NativeReportDeliveryFixture fixture) {
        assertThat(fixture.rows()).singleElement().satisfies(row ->
                assertThat(((Number) row.get(0)).intValue()).isEqualTo(42));
        assertThat(fixture.queries().getActiveQueryCount()).isZero();
    }
}
