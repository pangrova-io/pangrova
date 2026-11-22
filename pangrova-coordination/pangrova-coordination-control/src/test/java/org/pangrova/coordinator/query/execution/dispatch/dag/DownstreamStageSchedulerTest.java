package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.dag.runtime.DAGExecutionState;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.task.QueryOperatorStatsRegistry;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.query.profiling.OperatorStatsSource.DownstreamPreparation;
import org.pangrova.query.profiling.OperatorStatsSource.PreparationOutcome;

import java.util.List;
import java.util.Map;
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
        var registry = new QueryOperatorStatsRegistry();
        var observations = registry.beginExecution(first.queryId(), first.dag());
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
                scheduler.request(first, observations, () -> {
                    preparationThread.set(Thread.currentThread());
                    if (firstCalls.incrementAndGet() == 1) {
                        firstEntered.countDown();
                        await(firstUnblock);
                    }
                });
                await(firstEntered);
                for (int index = 0; index < 100; index++) {
                    scheduler.request(first, observations, () -> {
                        throw new AssertionError("Coalesced readiness should reuse the queued action");
                    });
                }
                scheduler.request(second, null, () -> { });
                await(secondDrained);
                assertThat(preparationThread.get()).isNotSameAs(reportThread);
                assertThat(firstCalls.get()).isEqualTo(1);
                assertThat(firstDrained.getCount()).isEqualTo(1);
                assertThat(preparations(registry, first)).singleElement().satisfies(work -> {
                    assertThat(work.outcome()).isEqualTo(PreparationOutcome.RUNNING);
                    assertThat(work.startedNanos()).isGreaterThanOrEqualTo(work.submittedNanos());
                    assertThat(work.finishedNanos()).isNull();
                });
            } finally {
                firstUnblock.countDown();
            }
            await(firstDrained);
            assertThat(firstCalls.get()).isEqualTo(2);
            assertThat(preparations(registry, first)).singleElement().satisfies(work -> {
                assertThat(work.outcome()).isEqualTo(PreparationOutcome.COMPLETED);
                assertThat(work.finishedNanos()).isGreaterThanOrEqualTo(work.startedNanos());
            });
        }
    }

    @Test
    void cancelledQueueEntryRetainsOwnershipUntilDequeuedAndRejectionLeavesNoLease() {
        var tracker = new CoordinatorTaskTracker();
        var first = open(tracker, "running");
        var queued = open(tracker, "queued");
        var rejected = open(tracker, "rejected");
        var registry = new QueryOperatorStatsRegistry();
        var queuedObservation = registry.beginExecution(queued.queryId(), queued.dag());
        var rejectedObservation = registry.beginExecution(rejected.queryId(), rejected.dag());
        var firstEntered = new CountDownLatch(1);
        var firstUnblock = new CountDownLatch(1);
        var queuedDrained = drained(tracker, queued);
        var rejectedDrained = drained(tracker, rejected);
        var queuedCalls = new AtomicInteger();
        var executor = ManagedExecutorFactory.newBoundedThreadPool("test-read-prepare", 1, 1);

        try (var scheduler = new DownstreamStageScheduler(tracker, executor)) {
            try {
                scheduler.request(first, null, () -> {
                    firstEntered.countDown();
                    await(firstUnblock);
                });
                await(firstEntered);
                scheduler.request(queued, queuedObservation, queuedCalls::incrementAndGet);
                assertThat(preparations(registry, queued)).singleElement().satisfies(work -> {
                    assertThat(work.outcome()).isEqualTo(PreparationOutcome.QUEUED);
                    assertThat(work.startedNanos()).isNull();
                    assertThat(work.finishedNanos()).isNull();
                });
                queued.cancelled().set(true);
                assertThat(queuedDrained.getCount()).isEqualTo(1);

                assertThatThrownBy(() -> scheduler.request(rejected, rejectedObservation, () -> { }))
                        .isInstanceOfSatisfying(PangrovaException.class, failure ->
                                assertThat(failure.getErrorCode()).isEqualTo(
                                        PangrovaErrorCode.RUNTIME_RESOURCE_EXHAUSTED));
                await(rejectedDrained);
                assertThat(preparations(registry, rejected)).singleElement().satisfies(work -> {
                    assertThat(work.outcome()).isEqualTo(PreparationOutcome.REJECTED);
                    assertThat(work.startedNanos()).isNull();
                    assertThat(work.finishedNanos()).isGreaterThanOrEqualTo(work.submittedNanos());
                });
            } finally {
                firstUnblock.countDown();
            }
            await(queuedDrained);
            assertThat(queuedCalls.get()).isZero();
            assertThat(preparations(registry, queued)).singleElement().satisfies(work -> {
                assertThat(work.outcome()).isEqualTo(PreparationOutcome.SKIPPED);
                assertThat(work.startedNanos()).isGreaterThan(work.submittedNanos());
                assertThat(work.finishedNanos()).isGreaterThanOrEqualTo(work.startedNanos());
            });
        }
    }

    @Test
    void preparationFailureCompletesTheQueryBeforeReleasingItsOperation() {
        var tracker = new CoordinatorTaskTracker();
        var state = open(tracker, "failed");
        var registry = new QueryOperatorStatsRegistry();
        var observations = registry.beginExecution(state.queryId(), state.dag());
        var drained = drained(tracker, state);
        var expected = new IllegalStateException("Split enumeration failed");
        var executor = ManagedExecutorFactory.newBoundedThreadPool("test-read-prepare", 1, 1);
        try (var scheduler = new DownstreamStageScheduler(tracker, executor)) {
            scheduler.request(state, observations, () -> { throw expected; });
            await(drained);
            assertThat(state.cancelled().get()).isTrue();
            assertThatThrownBy(state.completion()::join).hasCause(expected);
            assertThat(preparations(registry, state)).singleElement().satisfies(work -> {
                assertThat(work.outcome()).isEqualTo(PreparationOutcome.FAILED);
                assertThat(work.startedNanos()).isGreaterThanOrEqualTo(work.submittedNanos());
                assertThat(work.finishedNanos()).isGreaterThanOrEqualTo(work.startedNanos());
            });
        }
    }

    private static List<DownstreamPreparation> preparations(QueryOperatorStatsRegistry registry,
            DAGExecutionState state) {
        return registry.executionGraph(state.queryId()).downstreamPreparations();
    }

    private static DAGExecutionState open(CoordinatorTaskTracker tracker, String queryId) {
        tracker.openQueryIssuance(queryId);
        return DAGExecutionState.create(new StageExecutionContext.Live(
                SourceFreeQueryFixture.session(queryId)),
                StageDAG.fromNodes(List.of(), Map.of(), List.of()), null, null, null, ignored -> { });
    }

    private static CountDownLatch drained(CoordinatorTaskTracker tracker, DAGExecutionState state) {
        var latch = new CountDownLatch(1);
        tracker.addQueryDrainProgressListener(queryId -> {
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
