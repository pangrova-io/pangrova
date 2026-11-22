package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.execution.authority.StageExecutionContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DownstreamStageSchedulerTest {
    @Test
    void coalescesNotificationsWhileAnotherQueryCanPrepare() {
        var tracker = new CoordinatorTaskTracker();
        var first = open(tracker, "first");
        var second = open(tracker, "second");
        var firstEntered = new CountDownLatch(1);
        var firstUnblock = new CountDownLatch(1);
        var firstDrained = drained(tracker, first);
        var secondDrained = drained(tracker, second);
        var firstCalls = new AtomicInteger();
        var preparationThread = new AtomicReference<Thread>();
        var reportThread = Thread.currentThread();
        var executor = ManagedExecutorFactory.newBoundedThreadPool("test-read-prepare", 2, 1);

        try (var scheduler = new DownstreamStageScheduler(tracker, executor)) {
            try {
                scheduler.request(first, () -> {
                    preparationThread.set(Thread.currentThread());
                    if (firstCalls.incrementAndGet() == 1) {
                        firstEntered.countDown();
                        await(firstUnblock);
                    }
                });
                await(firstEntered);
                for (int index = 0; index < 100; index++) {
                    scheduler.request(first, () -> {
                        throw new AssertionError("Coalesced readiness should reuse the queued action");
                    });
                }
                scheduler.request(second, () -> { });
                await(secondDrained);
                assertThat(preparationThread.get()).isNotSameAs(reportThread);
                assertThat(firstCalls.get()).isEqualTo(1);
                assertThat(firstDrained.getCount()).isEqualTo(1);
            } finally {
                firstUnblock.countDown();
            }
            await(firstDrained);
            assertThat(firstCalls.get()).isEqualTo(2);
        }
    }

    @Test
    void cancelledQueueEntryRetainsOwnershipUntilDequeuedAndRejectionLeavesNoLease() {
        var tracker = new CoordinatorTaskTracker();
        var first = open(tracker, "running");
        var queued = open(tracker, "queued");
        var rejected = open(tracker, "rejected");
        var firstEntered = new CountDownLatch(1);
        var firstUnblock = new CountDownLatch(1);
        var queuedDrained = drained(tracker, queued);
        var rejectedDrained = drained(tracker, rejected);
        var queuedCalls = new AtomicInteger();
        var executor = ManagedExecutorFactory.newBoundedThreadPool("test-read-prepare", 1, 1);

        try (var scheduler = new DownstreamStageScheduler(tracker, executor)) {
            try {
                scheduler.request(first, () -> {
                    firstEntered.countDown();
                    await(firstUnblock);
                });
                await(firstEntered);
                scheduler.request(queued, queuedCalls::incrementAndGet);
                queued.cancelled().set(true);
                assertThat(queuedDrained.getCount()).isEqualTo(1);

                assertThatThrownBy(() -> scheduler.request(rejected, () -> { }))
                        .isInstanceOfSatisfying(PangrovaException.class, failure ->
                                assertThat(failure.getErrorCode()).isEqualTo(
                                        PangrovaErrorCode.RUNTIME_RESOURCE_EXHAUSTED));
                await(rejectedDrained);
            } finally {
                firstUnblock.countDown();
            }
            await(queuedDrained);
            assertThat(queuedCalls.get()).isZero();
        }
    }

    @Test
    void preparationFailureCompletesTheQueryBeforeReleasingItsOperation() {
        var tracker = new CoordinatorTaskTracker();
        var state = open(tracker, "failed");
        var drained = drained(tracker, state);
        var expected = new IllegalStateException("Split enumeration failed");
        var executor = ManagedExecutorFactory.newBoundedThreadPool("test-read-prepare", 1, 1);
        try (var scheduler = new DownstreamStageScheduler(tracker, executor)) {
            scheduler.request(state, () -> { throw expected; });
            await(drained);
            assertThat(state.cancelled().get()).isTrue();
            assertThatThrownBy(state.completion()::join).hasCause(expected);
        }
    }

    private static DAGExecutionState open(CoordinatorTaskTracker tracker, String queryId) {
        tracker.openQueryIssuance(queryId);
        return DAGExecutionState.create(new StageExecutionContext.Live(
                SourceFreeQueryFixture.session(queryId)), null, null, null, null, ignored -> { });
    }

    private static CountDownLatch drained(CoordinatorTaskTracker tracker, DAGExecutionState state) {
        var latch = new CountDownLatch(1);
        tracker.addQueryOperationExitListener(queryId -> {
            if (queryId.equals(state.queryId())) latch.countDown();
        });
        return latch;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("Read preparation did not progress");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for read preparation", failure);
        }
    }
}
