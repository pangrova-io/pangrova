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

    @Test
    void actualProgressWithoutTerminalBytesAcknowledgesOnlyItsUnchangedHttpRetry() throws Exception {
        try (var fixture = scenario("RUNNING", false, 0, 0)) {
            fixture.startWithoutConsumingResult();
            var actual = fixture.actualRunningReport();
            assertThat(actual.canonicalTaskReport()).isNull();
            assertThat(deliverActualProgress(fixture, actual).command()).isEqualTo("CONTINUE");
            var changed = new TaskStatusReport(actual.taskId(), actual.queryId(), actual.stageId(),
                    actual.partitionId(), actual.taskType(), actual.workerId(), actual.state(), null,
                    actual.error(), actual.writeResult(), actual.reportSequence(), actual.timestampMs(),
                    actual.errorCode(), actual.errorMetadata(), actual.attempt(), actual.canonicalTaskReport());
            assertThat(actual.stats()).isNotNull();
            assertThat(deliverActualProgress(fixture, changed).command()).isEqualTo("STALE_REJECTED");
            assertThat(deliverActualProgress(fixture, actual).command()).isEqualTo("CONTINUE");
            fixture.consumeResult();
            fixture.recoverAndDrain();
            assertFortyTwo(fixture);
        }
    }

    @Test
    void successfulExecutionReportLostAckRetriesBeforePublishingActualRelease() throws Exception {
        try (var fixture = scenario("FINISHED", false, 0, 2)) {
            fixture.pauseAutomaticTaskCleanup();
            fixture.finish();
            var actual = fixture.actualExecutionTerminalReport("FINISHED");
            awaitAttempts(fixture, 3);
            assertExecutionTerminalRetries(fixture, actual, 3);
            assertThat(fixture.failedReports()).isZero();
            fixture.recoverAndDrain();
            var released = fixture.actualReleasedReport("FINISHED");
            assertThat(released.reportSequence()).isGreaterThan(actual.reportSequence());
            assertActualDrained(released, TaskState.TASK_STATE_FINISHED);
            assertFortyTwo(fixture);
        }
    }

    @Test
    void successfulExecutionReportExhaustedAckRetainsItsExactFactUntilRealRecovery() throws Exception {
        try (var fixture = scenario("FINISHED", false, 0, Integer.MAX_VALUE)) {
            fixture.pauseAutomaticTaskCleanup();
            fixture.finish();
            var actual = fixture.actualExecutionTerminalReport("FINISHED");
            assertThat(fixture.awaitDeliveryFailure()).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not acknowledged");
            assertExecutionTerminalRetries(fixture, actual, TerminalReportDeliveryPolicy.ATTEMPTS_PER_ROUND);
            assertThat(fixture.failedReports()).isEqualTo(1);
            assertThat(fixture.queries().getQueryState(fixture.queryId()))
                    .contains(org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED);
            assertThat(fixture.queries().getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.actualReports()).noneMatch(report -> report.state().equals("FAILED"));
            fixture.recoverAndDrain();
            var released = fixture.actualReleasedReport("FINISHED");
            assertThat(released.reportSequence()).isGreaterThan(actual.reportSequence());
            assertThat(released.attempt()).isEqualTo(actual.attempt());
            assertActualDrained(released, TaskState.TASK_STATE_FINISHED);
            assertFortyTwo(fixture);
        }
    }

    private static void assertExecutionTerminalRetries(
            NativeReportDeliveryFixture fixture, TaskStatusReport actual, int count) throws Exception {
        var canonical = TaskReport.parseFrom(actual.canonicalTaskReport());
        assertThat(canonical.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(canonical.getExecutionDrainState())
                .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED);
        assertThat(canonical.hasError()).isFalse();
        var attempts = fixture.relay().matchingAttempts();
        assertThat(attempts).hasSize(count);
        assertThat(accepted(attempts)).isEqualTo(count);
        byte[] original = JsonMapperFactory.shared().writeValueAsBytes(actual);
        for (var attempt : attempts) {
            assertThat(attempt.body()).containsExactly(original);
            var retried = JsonMapperFactory.shared().readValue(attempt.body(), TaskStatusReport.class);
            assertThat(retried.reportSequence()).isEqualTo(actual.reportSequence());
            assertThat(retried.canonicalTaskReport()).containsExactly(actual.canonicalTaskReport());
        }
    }

    private static TaskStatusReportResponse deliverActualProgress(
            NativeReportDeliveryFixture fixture, TaskStatusReport report) throws Exception {
        var request = HttpRequest.newBuilder(fixture.coordinatorUri().resolve("/v1/task/status-report"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + NativeReportDeliveryFixture.SECRET)
                .POST(HttpRequest.BodyPublishers.ofByteArray(JsonMapperFactory.shared().writeValueAsBytes(report)))
                .build();
        var response = fixture.http().httpClient().send(request, HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).isEqualTo(200);
        var acknowledgment = JsonMapperFactory.shared().readValue(response.body(), TaskStatusReportResponse.class);
        assertThat(acknowledgment.taskId()).isEqualTo(report.taskId());
        return acknowledgment;
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
