package org.pangrova.testing.integration.e2e.cancellation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.http.TaskSubmitAck;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.testing.testkit.E2ETest;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real SQL and HTTP faults against one in-JVM Native Worker; not restart or multi-process proof. */
@E2ETest
@Tag("native-required")
class TaskAdmissionCancellationE2ETest extends TaskCancellationTestBase {
    @Test
    void cancellationBeforeAdmissionProducesRealReceiptAndBlocksHeldPost() throws Exception {
        try (var scenario = scenario()) {
            assertThat(cancelQuery(scenario.queryId()).statusCode()).isEqualTo(202);
            TaskReport receipt = receipt(scenario.transport.awaitDrainResponse(), scenario.submission.request());
            assertThat(receipt.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            scenario.transport.releasePost();
            assertThat(scenario.transport.awaitPostResponse().statusCode()).isEqualTo(409);
            assertThat(pollUntilTerminal(scenario.queryId())).contains("\"CANCELLED\"");
        }
    }

    @Test
    void delayedPostAfterRealReceiptCannotReopenSealedAttempt() throws Exception {
        try (var scenario = scenario()) {
            assertThat(cancelQuery(scenario.queryId()).statusCode()).isEqualTo(202);
            var first = receipt(scenario.transport.awaitDrainResponse(), scenario.submission.request());
            assertThat(replay(scenario.submission.body(), scenario.submission.headers()).statusCode()).isEqualTo(409);
            var repeated = seal(scenario.submission.request());
            assertThat(receipt(repeated, scenario.submission.request())).isEqualTo(first);
            scenario.transport.releasePost();
            assertThat(scenario.transport.awaitPostResponse().statusCode()).isEqualTo(409);
            assertThat(pollUntilTerminal(scenario.queryId())).contains("\"CANCELLED\"");
        }
    }

    @Test
    void concurrentPostAndDeleteRequireActualDrainAndRejectLaterReplay() throws Exception {
        try (var scenario = scenario()) {
            var executor = Executors.newFixedThreadPool(2);
            var start = new CountDownLatch(1);
            try {
                var cancellation = executor.submit(() -> {
                    start.await();
                    return cancelQuery(scenario.queryId());
                });
                var delivery = executor.submit(() -> {
                    start.await();
                    scenario.transport.releasePost();
                    return scenario.transport.awaitPostResponse();
                });
                start.countDown();
                assertThat(cancellation.get(30, TimeUnit.SECONDS).statusCode()).isEqualTo(202);
                assertThat(delivery.get(30, TimeUnit.SECONDS).statusCode()).isIn(200, 409);
                var request = scenario.submission.request();
                var drained = await().atMost(Duration.ofSeconds(30)).until(() -> seal(request), response -> {
                    if (response.statusCode() == 202) {
                        assertThat(response.body()).isEmpty();
                        return false;
                    }
                    return true;
                });
                var actual = receipt(drained, request);
                assertThat(actual.getExecutionDrainState()).isIn(
                        ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED,
                        ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
                assertThat(replay(scenario.submission.body(), scenario.submission.headers()).statusCode()).isEqualTo(409);
                assertThat(pollUntilTerminal(scenario.queryId())).contains("\"CANCELLED\"");
            } finally {
                start.countDown();
                scenario.transport.releasePost();
                executor.shutdown();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void expiredPostFromActualSqlPlanRejectsBeforeNativeOwnership() throws Exception {
        try (var scenario = scenario()) {
            var expired = SubmitTaskRequest.from(scenario.submission.request().taskRequest()
                    .toBuilder().setDeadlineEpochMs(1).build());
            var response = replay(expired.encode(), scenario.submission.headers());
            assertThat(response.statusCode()).isEqualTo(408);
            assertThat(TaskSubmitAck.parseFrom(response.body()).hasRejected()).isTrue();
            assertThat(cancelQuery(scenario.queryId()).statusCode()).isEqualTo(202);
            var actual = receipt(scenario.transport.awaitDrainResponse(), scenario.submission.request());
            assertThat(actual.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
            scenario.transport.releasePost();
            assertThat(scenario.transport.awaitPostResponse().statusCode()).isEqualTo(409);
        }
    }

    @Test
    void conflictingRequestWithSameTaskIdDoesNotChangeActualSqlRows() throws Exception {
        try (var scenario = scenario()) {
            scenario.transport.releasePost();
            assertThat(scenario.transport.awaitPostResponse().statusCode()).isEqualTo(200);
            var original = scenario.submission.request().taskRequest();
            var conflicting = SubmitTaskRequest.from(original.toBuilder()
                    .setDeadlineEpochMs(Math.addExact(original.getDeadlineEpochMs(), 1)).build());
            var response = replay(conflicting.encode(), scenario.submission.headers());
            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(TaskSubmitAck.parseFrom(response.body()).hasRejected()).isTrue();
            assertThat(consumeSubmittedQuery(scenario.awaitSubmittedQuery())).extracting(row -> row.getLong("id"))
                    .containsExactlyInAnyOrder(7L, 9L);
            assertThat(pollUntilTerminal(scenario.queryId())).contains("\"FINISHED\"");
            var actual = receipt(seal(scenario.submission.request()), scenario.submission.request());
            assertThat(actual.getExecutionDrainState())
                    .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
        }
    }

}
