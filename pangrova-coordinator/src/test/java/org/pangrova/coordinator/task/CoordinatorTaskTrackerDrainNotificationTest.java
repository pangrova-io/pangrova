package org.pangrova.coordinator.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.CanonicalTaskRequestTestFixture;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual tracker/listener ownership contracts; generated receipts are not Native execution evidence. */
class CoordinatorTaskTrackerDrainNotificationTest {
    @Test
    void acceptedPublicationActuallyNotifiesBeforeReleasingItsScope() {
        var tracker = new CoordinatorTaskTracker();
        var request = issue(tracker);
        var receipt = receipt(request);
        var report = projection(request, receipt);
        var calls = new AtomicInteger();
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            assertThat(tracker.acknowledgesExecutionDrain(report)).isFalse();
            assertThatThrownBy(() -> tracker.requireQueryExecutionDrained("query"))
                    .hasMessageContaining("operations have not exited");
            calls.incrementAndGet();
        });

        try (var publication = tracker.enterReportPublication(request, report, false)) {
            tracker.recordExecutionDrain(request, receipt);
            publication.accepted();
            assertThat(calls).hasValue(0);
            assertThat(tracker.acknowledgesExecutionDrain(report)).isFalse();
        }

        assertThat(calls).hasValue(1);
        assertThat(tracker.acknowledgesExecutionDrain(report)).isTrue();
        tracker.requireQueryExecutionDrained("query");
        tracker.retireQueryIssuance("query");
        assertThat(tracker.getTaskRequest("task")).isNull();
        assertThat(tracker.executionDrainReceipt("task")).contains(receipt);
    }

    @Test
    void rejectedListenerRetainsTheFactAndRetriesTheWholeIdempotentFanout() {
        var tracker = new CoordinatorTaskTracker();
        var request = issue(tracker);
        var receipt = receipt(request);
        var report = projection(request, receipt);
        var reject = new AtomicBoolean(true);
        var rejectingCalls = new AtomicInteger();
        var successfulCalls = new AtomicInteger();
        var rejection = new IllegalStateException("drain observer rejected");
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            rejectingCalls.incrementAndGet();
            if (reject.get()) throw rejection;
        });
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            successfulCalls.incrementAndGet();
        });
        var publication = tracker.enterReportPublication(request, report, false);
        tracker.recordExecutionDrain(request, receipt);
        publication.accepted();

        assertThatThrownBy(publication::close).isSameAs(rejection);
        assertThat(rejectingCalls).hasValue(1);
        assertThat(successfulCalls).hasValue(1);
        assertThat(tracker.acknowledgesExecutionDrain(report)).isFalse();
        assertThat(tracker.getTaskRequest("task")).isEqualTo(request);
        assertThatThrownBy(() -> tracker.retireQueryIssuance("query"))
                .hasMessageContaining("publication");

        reject.set(false);
        try (var retry = tracker.enterReportPublication(request, report, false)) {
            assertThat(retry.publicationCompleted()).isTrue();
            assertThat(retry.report()).isSameAs(report);
        }

        // No per-listener success journal exists: successful listeners must tolerate exact-fact replay.
        assertThat(rejectingCalls).hasValue(2);
        assertThat(successfulCalls).hasValue(2);
        assertThat(tracker.acknowledgesExecutionDrain(report)).isTrue();
        tracker.requireQueryExecutionDrained("query");
    }

    @Test
    void concurrentPublicationExitCannotReplayASuccessfulInFlightNotification() throws Exception {
        var tracker = new CoordinatorTaskTracker();
        var request = issue(tracker);
        var receipt = receipt(request);
        var report = projection(request, receipt);
        var calls = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var exited = new CountDownLatch(1);
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            calls.incrementAndGet();
            entered.countDown();
            try {
                await(release);
            } finally {
                exited.countDown();
            }
        });
        var first = tracker.enterReportPublication(request, report, false);
        tracker.recordExecutionDrain(request, receipt);
        first.accepted();
        var closing = new FutureTask<Void>(() -> {
            first.close();
            return null;
        });
        var thread = new Thread(closing, "task-drain-notification");
        TaskReportPublication concurrent = null;
        thread.start();
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            concurrent = tracker.enterReportPublication(request, report, false);
            assertThat(concurrent.publicationCompleted()).isTrue();
            concurrent.accepted();
            assertThat(tracker.acknowledgesExecutionDrain(report)).isFalse();
            assertThatThrownBy(() -> tracker.requireQueryExecutionDrained("query"))
                    .hasMessageContaining("operations have not exited");
            release.countDown();
            closing.get(5, TimeUnit.SECONDS);
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(tracker.acknowledgesExecutionDrain(report)).isFalse();
            concurrent.close();
            assertThat(calls).hasValue(1);
            assertThat(tracker.acknowledgesExecutionDrain(report)).isTrue();
            tracker.requireQueryExecutionDrained("query");
        } finally {
            release.countDown();
            thread.join(5000);
            assertThat(thread.isAlive()).isFalse();
            if (concurrent != null) concurrent.close();
            first.close();
        }
    }

    @Test
    void reentrantPublicationReceiptCaptureDoesNotRecursivelyNotify() {
        var tracker = new CoordinatorTaskTracker();
        var request = issue(tracker);
        var receipt = receipt(request);
        var report = projection(request, receipt);
        var calls = new AtomicInteger();
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            // The guard gives a useful assertion instead of stack overflow if reentry wrongly republishes.
            assertThat(calls.incrementAndGet()).isEqualTo(1);
            tracker.recordExecutionDrain(request, delivered);
        });

        try (var publication = tracker.enterReportPublication(request, report, false)) {
            tracker.recordExecutionDrain(request, receipt);
            publication.accepted();
        }
        assertThat(calls).hasValue(1);
        assertThat(tracker.acknowledgesExecutionDrain(report)).isTrue();
    }

    @Test
    void completedRunningPublicationDoesNotBlockALaterDirectCancellationReceipt() {
        var tracker = new CoordinatorTaskTracker();
        var request = issue(tracker);
        var receipt = receipt(request);
        var calls = new AtomicInteger();
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            calls.incrementAndGet();
        });
        var running = TaskReport.newBuilder().setAttempt(request.taskRequest().getAttempt())
                .setState(TaskState.TASK_STATE_RUNNING).setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).build();
        var report = new TaskStatusReport("task", "query", 0, 0, "READ", "worker", "RUNNING",
                null, null, null, 1L, 1000L, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), running.toByteArray());
        try (var publication = tracker.enterReportPublication(request, report, false)) {
            publication.accepted();
        }
        assertThat(calls).hasValue(0);
        assertThat(tracker.acknowledgesReportPublication(report)).isTrue();

        tracker.recordExecutionDrain(request, receipt);

        assertThat(calls).hasValue(1);
        tracker.requireQueryExecutionDrained("query");
    }

    @Test
    void receiptWithoutAcceptedPublicationDoesNotNotifyOrRetire() {
        var tracker = new CoordinatorTaskTracker();
        var request = issue(tracker);
        var receipt = receipt(request);
        var report = projection(request, receipt);
        var calls = new AtomicInteger();
        tracker.addExecutionDrainListener(delivered -> {
            assertThat(delivered).isEqualTo(receipt);
            calls.incrementAndGet();
        });
        try (var publication = tracker.enterReportPublication(request, report, false)) {
            assertThat(publication.publicationCompleted()).isFalse();
            tracker.recordExecutionDrain(request, receipt);
        }
        assertThat(calls).hasValue(0);
        assertThat(tracker.acknowledgesExecutionDrain(report)).isFalse();
        assertThatThrownBy(() -> tracker.retireQueryIssuance("query")).hasMessageContaining("publication");
        try (var retry = tracker.enterReportPublication(request, report, false)) {
            assertThat(retry.publicationCompleted()).isFalse();
            retry.accepted();
        }
        assertThat(calls).hasValue(1);
    }

    private static SubmitTaskRequest issue(CoordinatorTaskTracker tracker) {
        var request = CanonicalTaskRequestTestFixture.request("task", "query", 0, 0,
                TaskKind.TASK_KIND_READ, new byte[]{1});
        tracker.openQueryIssuance("query");
        tracker.registerTask("query", "task");
        tracker.registerTaskRequest("task", request);
        tracker.bindDispatchTarget(request, TestWorkerInfoBuilder.active("worker", "original-host", 8001, 8002));
        tracker.enterTaskDispatch(request);
        tracker.sealQueryIssuance("query");
        return request;
    }

    private static TaskReport receipt(SubmitTaskRequest request) {
        return TaskReport.newBuilder().setAttempt(request.taskRequest().getAttempt())
                .setState(TaskState.TASK_STATE_FINISHED).setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
    }

    private static TaskStatusReport projection(SubmitTaskRequest request, TaskReport receipt) {
        return new TaskStatusReport("task", "query", 0, 0, "READ", "worker", "FINISHED",
                null, null, null, 1L, 1000L, null, null,
                TaskAttemptDescriptor.from(request.taskRequest().getAttempt()), receipt.toByteArray());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while holding the actual notification callback", interrupted);
        }
    }
}
