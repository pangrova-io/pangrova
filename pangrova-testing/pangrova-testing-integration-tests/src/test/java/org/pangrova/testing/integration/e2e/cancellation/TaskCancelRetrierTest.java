package org.pangrova.testing.integration.e2e.cancellation;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.testing.testkit.constants.TestObservability;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real HTTP cancellation and retained-receipt integration against an actual SQL-issued task.
 * Missing-request exceptions are production component guards, not simulated Native failures.
 */
@Tag("integration")
@Tag("native-required")
class TaskCancelRetrierTest extends TaskCancellationTestBase {

    @Test
    void cancelWithRetry_successfulCancel_updatesTrackerAndNoPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            control.cancel(scenario);
            assertThat(control.retrier.pendingCount()).isZero();
            assertThat(scenario.transport.deleteAttempts()).isEqualTo(1);
            assertDrained(control, scenario);
        }
    }

    @Test
    void cancelWithRetry_failedCancel_enqueuesPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            scenario.transport.disconnectDeletes(taskId(scenario));
            control.cancel(scenario);
            assertPending(control, scenario);
            assertThat(scenario.transport.deleteAttempts()).isPositive();
            assertThat(control.tracker.executionDrainReceipt(taskId(scenario))).isEmpty();
        }
    }

    @Test
    void cancelWithRetry_exceptionThrown_enqueuesPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            assertThat(control.tracker.unregisterTaskRequest(taskId(scenario), scenario.submission.request())).isTrue();
            control.cancel(scenario);
            assertPending(control, scenario);
            assertThat(scenario.transport.deleteAttempts()).isZero();
            assertThat(control.tracker.executionDrainReceipt(taskId(scenario))).isEmpty();
        }
    }

    @Test
    void retryPendingCancels_emptyPending_returnsZero() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            assertThat(control.retrier.retryPendingCancels()).isZero();
            assertThat(control.retrier.pendingCount()).isZero();
            assertThat(scenario.transport.deleteAttempts()).isZero();
        }
    }

    @Test
    void retryPendingCancels_successOnRetry_removesPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            failInitialCancel(control, scenario);
            scenario.transport.reconnectDeletes(taskId(scenario));
            assertThat(control.retrier.retryPendingCancels()).isEqualTo(1);
            assertThat(control.retrier.pendingCount()).isZero();
            assertDrained(control, scenario);
        }
    }

    @Test
    void independentReceiptResolvesAPendingCancelAfterRequestCompaction() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            failInitialCancel(control, scenario);
            var request = scenario.submission.request();
            var actual = receipt(sealAt(request, workerUri()), request);
            control.tracker.recordExecutionDrain(request, actual);
            control.tracker.unregisterDrainedTask(actual);
            int attempts = scenario.transport.deleteAttempts();

            assertThat(control.tracker.getTaskRequest(taskId(scenario))).isNull();
            assertThat(control.retrier.retryPendingCancels()).isEqualTo(1);
            assertThat(control.retrier.pendingCount()).isZero();
            assertThat(scenario.transport.deleteAttempts()).isEqualTo(attempts);
        }
    }

    @Test
    void retryPendingCancels_stillFailing_keepsPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            failInitialCancel(control, scenario);
            int previous = scenario.transport.deleteAttempts();
            assertThat(control.retrier.retryPendingCancels()).isZero();
            assertPending(control, scenario);
            assertThat(scenario.transport.deleteAttempts()).isGreaterThan(previous);
            assertThat(control.tracker.executionDrainReceipt(taskId(scenario))).isEmpty();
        }
    }

    @Test
    void multipleTasks_partialRetrySuccess() throws Exception {
        try (var first = scenario(); var second = scenario(); var control = new CancellationControl(first)) {
            control.tracker.openQueryIssuance(second.queryId());
            control.tracker.registerTaskRequest(taskId(second), second.submission.request());
            first.transport.disconnectDeletes(taskId(first));
            second.transport.disconnectDeletes(taskId(second));
            control.cancel(first);
            control.cancel(second);
            assertThat(control.retrier.pendingTaskIds()).containsExactlyInAnyOrder(taskId(first), taskId(second));
            first.transport.reconnectDeletes(taskId(first));
            assertThat(control.retrier.retryPendingCancels()).isEqualTo(1);
            assertThat(control.retrier.pendingCount()).isEqualTo(1);
            assertThat(control.retrier.pendingTaskIds()).containsExactly(taskId(second));
            assertDrained(control, first);
            assertThat(control.tracker.executionDrainReceipt(taskId(second))).isEmpty();
        }
    }

    @Test
    void cancelWithRetry_successAfterPreviousFailure_removesPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            failInitialCancel(control, scenario);
            scenario.transport.reconnectDeletes(taskId(scenario));
            control.cancel(scenario);
            assertThat(control.retrier.pendingCount()).isZero();
            assertDrained(control, scenario);
        }
    }

    @Test
    void retryPendingCancels_exceptionDuringRetry_keepsPending() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            failInitialCancel(control, scenario);
            int previous = scenario.transport.deleteAttempts();
            assertThat(control.tracker.unregisterTaskRequest(taskId(scenario), scenario.submission.request())).isTrue();
            assertThat(control.retrier.retryPendingCancels()).isZero();
            assertPending(control, scenario);
            assertThat(scenario.transport.deleteAttempts()).isEqualTo(previous);
            assertThat(control.tracker.executionDrainReceipt(taskId(scenario))).isEmpty();
        }
    }

    @Test
    void actualServiceNotFoundIsNotExecutionDrainProof() throws Exception {
        try (var scenario = scenario(); var control = new CancellationControl(scenario)) {
            URI coordinator = URI.create("http://localhost:" + coordinatorHttpServer.getPort());
            assertThat(sealAt(scenario.submission.request(), coordinator).statusCode()).isEqualTo(404);
            control.retrier.cancelWithRetry(scenario.queryId(), taskId(scenario),
                    coordinator.getHost(), coordinator.getPort());
            assertPending(control, scenario);
            assertThat(control.tracker.executionDrainReceipt(taskId(scenario))).isEmpty();
            assertThat(scenario.transport.deleteAttempts()).isZero();
        }
    }

    private static void failInitialCancel(CancellationControl control, Scenario scenario) {
        scenario.transport.disconnectDeletes(taskId(scenario));
        control.cancel(scenario);
        assertPending(control, scenario);
    }

    private static void assertPending(CancellationControl control, Scenario scenario) {
        assertThat(control.retrier.pendingCount()).isEqualTo(1);
        assertThat(control.retrier.pendingTaskIds()).containsExactly(taskId(scenario));
    }

    private static void assertDrained(CancellationControl control, Scenario scenario) throws Exception {
        var actual = receipt(scenario.transport.awaitDrainResponse(), scenario.submission.request());
        assertThat(control.tracker.executionDrainReceipt(taskId(scenario))).contains(actual);
        assertThat(actual.getExecutionDrainState())
                .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
    }

    private static String taskId(Scenario scenario) {
        return scenario.submission.request().taskRequest().getAttempt().getTaskId();
    }

    private static final class CancellationControl implements AutoCloseable {
        private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        private final RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), TestObservability.TRACING, E2E_INTERNAL_AUTH_SECRET);
        private final TaskCancelRetrier retrier = new TaskCancelRetrier(dispatcher, tracker);

        private CancellationControl(Scenario scenario) {
            tracker.openQueryIssuance(scenario.queryId());
            tracker.registerTaskRequest(taskId(scenario), scenario.submission.request());
        }

        private void cancel(Scenario scenario) {
            retrier.cancelWithRetry(scenario.queryId(), taskId(scenario),
                    workerAdvertisedHost(), scenario.transport.port());
        }

        @Override
        public void close() { dispatcher.close(); }
    }
}
