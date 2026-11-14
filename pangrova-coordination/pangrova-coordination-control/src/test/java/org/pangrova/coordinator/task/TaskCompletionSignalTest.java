package org.pangrova.coordinator.task;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Completion acknowledgement is retained with the actual attempt, separately from Worker task state. */
class TaskCompletionSignalTest {
    private static final String QUERY = "completion-query";
    private static final String TASK = "completion-task";

    @Test
    void finishedStateDoesNotReleaseWaitersBeforeExplicitCommitAcknowledgement() {
        var tracker = scheduled();
        tracker.updateTaskState(QUERY, TASK, "FINISHED");
        var waiter = tracker.awaitTaskCompletion(QUERY, TASK);
        assertThat(waiter).isNotDone();
        assertThat(tracker.updateTaskState(QUERY, TASK, "FAILED")).isFalse();
        assertThat(waiter).isNotDone();
        tracker.signalTaskTerminal(QUERY, TASK);
        assertThat(waiter).isCompleted();
    }

    @Test
    void lateWaiterObservesAlreadyAcknowledgedTerminalWithoutCreatingANewUnsettledFuture() {
        var tracker = scheduled();
        tracker.updateTaskState(QUERY, TASK, "FINISHED");
        tracker.signalTaskTerminal(QUERY, TASK);
        var waiter = tracker.awaitTaskCompletion(QUERY, TASK);
        assertThat(waiter).isCompleted();
        assertThat(tracker.awaitTaskCompletion(QUERY, TASK)).isSameAs(waiter);
    }

    @Test
    void lateFailureWaiterObservesTheOriginalFailure() {
        var tracker = scheduled();
        tracker.updateTaskState(QUERY, TASK, "FAILED");
        assertThatThrownBy(() -> tracker.awaitTaskCompletion(QUERY, TASK).join())
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Task terminated: completion-task, state=FAILED");
    }

    @Test
    void nonterminalSignalDoesNotSettleOrDiscardAnExistingWaiter() {
        var tracker = scheduled();
        var waiter = tracker.awaitTaskCompletion(QUERY, TASK);
        tracker.signalTaskTerminal(QUERY, TASK);
        assertThat(tracker.awaitTaskCompletion(QUERY, TASK)).isSameAs(waiter).isNotDone();
        tracker.updateTaskState(QUERY, TASK, "FINISHED");
        tracker.signalTaskTerminal(QUERY, TASK);
        assertThat(waiter).isCompleted();
    }

    @Test
    void concurrentRegistrationAndCommitSignalAlwaysShareTheAttemptFuture() throws Exception {
        var tracker = scheduled();
        tracker.updateTaskState(QUERY, TASK, "FINISHED");
        var executor = Executors.newFixedThreadPool(2);
        var start = new CountDownLatch(1);
        try {
            var waiting = executor.submit(() -> {
                start.await();
                return tracker.awaitTaskCompletion(QUERY, TASK);
            });
            var signalling = executor.submit(() -> {
                start.await();
                tracker.signalTaskTerminal(QUERY, TASK);
                return null;
            });
            start.countDown();
            signalling.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> waiter = waiting.get(5, TimeUnit.SECONDS);
            waiter.get(5, TimeUnit.SECONDS);
            assertThat(tracker.awaitTaskCompletion(QUERY, TASK)).isSameAs(waiter);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void retiredAttemptCannotBeAwaitedOrSignalledAsAnUnregisteredSuccessfulTask() {
        var tracker = scheduled();
        tracker.updateTaskState(QUERY, TASK, "FINISHED");
        tracker.signalTaskTerminal(QUERY, TASK);
        tracker.unregisterSettledTask(QUERY, TASK);
        tracker.signalTaskTerminal(QUERY, TASK);
        assertThatThrownBy(() -> tracker.awaitTaskCompletion(QUERY, TASK))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retryRequiresANewPhysicalTaskIdentityAndDoesNotInheritTheFailedSignal() {
        var tracker = scheduled();
        tracker.updateTaskState(QUERY, TASK, "FAILED");
        tracker.markScheduled(QUERY, TASK);
        assertThat(tracker.getTaskState(QUERY, TASK)).isEqualTo(TaskStateMachine.State.FAILED);
        assertThatThrownBy(() -> tracker.updateTaskState(QUERY, TASK, "RESCHEDULING"))
                .isInstanceOf(IllegalArgumentException.class);
        tracker.unregisterSettledTask(QUERY, TASK);
        tracker.registerTask(QUERY, "retry-task");
        tracker.markScheduled(QUERY, "retry-task");
        var retry = tracker.awaitTaskCompletion(QUERY, "retry-task");
        assertThat(retry).isNotDone();
        tracker.updateTaskState(QUERY, "retry-task", "FINISHED");
        tracker.signalTaskTerminal(QUERY, "retry-task");
        assertThat(retry).isCompleted();
    }

    private static CoordinatorTaskTracker scheduled() {
        var tracker = new CoordinatorTaskTracker();
        tracker.registerTask(QUERY, TASK);
        tracker.markScheduled(QUERY, TASK);
        return tracker;
    }
}
