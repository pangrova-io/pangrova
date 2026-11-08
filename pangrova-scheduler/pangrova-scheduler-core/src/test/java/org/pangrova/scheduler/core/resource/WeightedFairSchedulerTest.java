package org.pangrova.scheduler.core.resource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests for {@link WeightedFairScheduler}.
 *
 * <p><b>Validates: Requirements 11.2</b></p>
 */
class WeightedFairSchedulerTest {

    private WeightedFairScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new WeightedFairScheduler();
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    void computeShareReturnsZeroForEmptyActiveQueues() {
        double share = scheduler.computeShare(QueueType.INTERACTIVE, Set.of());
        assertThat(share).isEqualTo(0.0);
    }

    @Test
    void computeShareReturnsZeroWhenTypeNotInActiveQueues() {
        double share = scheduler.computeShare(QueueType.INTERACTIVE, Set.of(QueueType.BATCH));
        assertThat(share).isEqualTo(0.0);
    }

    @Test
    void computeShareReturnsFractionByWeight() {
        Set<QueueType> active = Set.of(QueueType.INTERACTIVE, QueueType.BATCH);
        double interactiveShare = scheduler.computeShare(QueueType.INTERACTIVE, active);
        double batchShare = scheduler.computeShare(QueueType.BATCH, active);
        assertThat(interactiveShare + batchShare).isCloseTo(1.0, within(0.001));
        assertThat(interactiveShare).isGreaterThan(0.0);
        assertThat(batchShare).isGreaterThan(0.0);
    }

    @Test
    void dispatcherExecutesSubmittedTasks() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        scheduler.submitTask(latch::countDown, QueueType.INTERACTIVE);
        scheduler.submitTask(latch::countDown, QueueType.INTERACTIVE);
        scheduler.submitTask(latch::countDown, QueueType.BATCH);

        assertThat(latch.await(5, TimeUnit.SECONDS))
                .as("All tasks should be dispatched within timeout")
                .isTrue();
    }

    @Test
    void dispatcherExecutesAllTasksAcrossMultipleQueues() throws InterruptedException {
        int tasksPerQueue = 10;
        int totalTasks = tasksPerQueue * 2;
        AtomicInteger interactiveCount = new AtomicInteger(0);
        AtomicInteger batchCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(totalTasks);

        for (int i = 0; i < tasksPerQueue; i++) {
            scheduler.submitTask(() -> {
                interactiveCount.incrementAndGet();
                latch.countDown();
            }, QueueType.INTERACTIVE);
            scheduler.submitTask(() -> {
                batchCount.incrementAndGet();
                latch.countDown();
            }, QueueType.BATCH);
        }

        assertThat(latch.await(10, TimeUnit.SECONDS))
                .as("All tasks across both queues should be dispatched")
                .isTrue();
        assertThat(interactiveCount.get()).isEqualTo(tasksPerQueue);
        assertThat(batchCount.get()).isEqualTo(tasksPerQueue);
    }

    @Test
    void metricsReflectsDispatchedCount() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        scheduler.submitTask(latch::countDown, QueueType.INTERACTIVE);
        scheduler.submitTask(latch::countDown, QueueType.BATCH);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        WeightedFairScheduler.SchedulerMetrics metrics = scheduler.getMetrics();
        assertThat(metrics.totalDispatched()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void closeShutdownsDispatcherGracefully() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        scheduler.submitTask(latch::countDown, QueueType.INTERACTIVE);
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        scheduler.close();

        assertThatThrownBy(() -> scheduler.submitTask(() -> {}, QueueType.INTERACTIVE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shut down");
    }

    @Test
    void closeIsIdempotent() {
        scheduler.close();
        scheduler.close(); // should not throw
    }

    @Test
    void closeDrainsPreviouslyAcceptedTasksBeforeDispatcherActuallyExits() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var queuedRan = new CountDownLatch(1);
        var dispatcher = new AtomicReference<Thread>();
        var failure = new AtomicReference<Throwable>();
        scheduler.submitTask(() -> {
            dispatcher.set(Thread.currentThread());
            entered.countDown();
            awaitRelease(release);
        }, QueueType.INTERACTIVE);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        scheduler.submitTask(queuedRan::countDown, QueueType.BATCH);
        var closer = new Thread(() -> {
            try {
                scheduler.close();
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "actual-fair-scheduler-close");
        closer.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    closer.getState() == Thread.State.TIMED_WAITING);
            assertThat(dispatcher.get().isAlive()).isTrue();
            assertThat(queuedRan.getCount()).isEqualTo(1);
            assertThatThrownBy(() -> scheduler.submitTask(() -> {}, QueueType.BATCH))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            release.countDown();
            closer.join(5_000);
        }
        assertThat(closer.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(queuedRan.getCount()).isZero();
        assertThat(dispatcher.get().isAlive()).isFalse();
        assertThat(scheduler.getMetrics().totalPendingTasks()).isZero();
    }

    @Test
    void interruptedCloseRetainsActualDispatcherOwnershipForRetry() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var dispatcher = new AtomicReference<Thread>();
        scheduler.submitTask(() -> {
            dispatcher.set(Thread.currentThread());
            entered.countDown();
            awaitRelease(release);
        }, QueueType.INTERACTIVE);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(scheduler::close).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Interrupted awaiting actual executor termination");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(dispatcher.get().isAlive()).isTrue();
        } finally {
            Thread.interrupted();
            release.countDown();
            scheduler.close();
        }
        // close() joins executor termination; the worker thread's visible exit can lag the
        // join slightly under load, so wait for the observable state before asserting.
        dispatcher.get().join(5_000);
        assertThat(dispatcher.get().isAlive()).isFalse();
    }

    @Test
    void dispatcherCannotAcknowledgeItsOwnExit() throws Exception {
        var checked = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        scheduler.submitTask(() -> {
            try {
                assertThatThrownBy(scheduler::close).isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("cannot join its own owner");
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                checked.countDown();
            }
        }, QueueType.BATCH);
        assertThat(checked.await(5, TimeUnit.SECONDS)).isTrue();
        scheduler.close();
        assertThat(failure.get()).isNull();
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Test did not release the actual entered callback");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Actual callback was interrupted instead of being joined", interrupted);
        }
    }

    @Test
    void dispatcherActuallyConsumesFromQueue() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        scheduler.submitTask(() -> {
            started.countDown();
            try {
                finish.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, QueueType.INTERACTIVE);

        // Wait for task to start executing (proves dispatcher consumed it)
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        finish.countDown();
    }

    @Test
    void getQueueReturnsEmptyForUnknownType() {
        var queue = scheduler.getQueue(QueueType.BATCH);
        assertThat(queue).isEmpty();
    }

    @Test
    void taskExceptionDoesNotKillDispatcher() throws InterruptedException {
        CountDownLatch afterError = new CountDownLatch(1);

        scheduler.submitTask(() -> { throw new RuntimeException("intentional"); }, QueueType.INTERACTIVE);
        scheduler.submitTask(afterError::countDown, QueueType.INTERACTIVE);

        assertThat(afterError.await(5, TimeUnit.SECONDS))
                .as("Dispatcher should continue after task exception")
                .isTrue();
    }

    @Test
    void aCompletedCallbacksInterruptDoesNotCancelAnotherAcceptedCallback() throws Exception {
        var next = new CountDownLatch(1);
        var inheritedInterrupt = new AtomicReference<Boolean>();
        scheduler.submitTask(() -> {
            scheduler.submitTask(() -> {
                inheritedInterrupt.set(Thread.currentThread().isInterrupted());
                next.countDown();
            }, QueueType.INTERACTIVE);
            Thread.currentThread().interrupt();
        }, QueueType.INTERACTIVE);

        assertThat(next.await(5, TimeUnit.SECONDS)).isTrue();
        scheduler.close();
        assertThat(inheritedInterrupt.get()).isFalse();
        assertThat(scheduler.getMetrics().totalPendingTasks()).isZero();
    }

    @Test
    void fatalDispatcherExitNeverAcknowledgesUnexecutedAcceptedCallbacks() throws Exception {
        scheduler.close();
        var owner = new WeightedFairScheduler();
        var exited = new CountDownLatch(1);
        owner.submitTask(() -> {
            owner.submitTask(() -> { throw new AssertionError("Unexecuted callback must remain pending"); },
                    QueueType.BATCH);
            exited.countDown();
            throw new AssertionError("Actual fatal dispatcher failure");
        }, QueueType.INTERACTIVE);

        assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("accepted callbacks still pending");
        assertThat(owner.getMetrics().totalPendingTasks()).isEqualTo(1);
        assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("accepted callbacks still pending");
        assertThatThrownBy(() -> owner.submitTask(() -> {}, QueueType.BATCH))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void fatalDispatcherExitRejectsNewAdmissionWithoutWaitingForOwnerShutdown() throws Exception {
        var owner = new WeightedFairScheduler();
        var failing = new CountDownLatch(1);
        try {
            owner.submitTask(() -> {
                failing.countDown();
                throw new AssertionError("Actual fatal dispatcher failure before owner shutdown");
            }, QueueType.INTERACTIVE);
            assertThat(failing.await(5, TimeUnit.SECONDS)).isTrue();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThatThrownBy(() -> owner.submitTask(() -> {}, QueueType.BATCH))
                            .isInstanceOf(IllegalStateException.class));
        } finally {
            assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void terminalDispatcherFailureWithoutPendingCallbacksRetainsTheOriginalCause() throws Exception {
        var owner = new WeightedFairScheduler();
        var failing = new CountDownLatch(1);
        var original = new AssertionError("Original fatal callback failure");
        owner.submitTask(() -> {
            failing.countDown();
            throw original;
        }, QueueType.INTERACTIVE);
        try {
            assertThat(failing.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class).hasCause(original);
        }

        assertThat(owner.getMetrics().totalPendingTasks()).isZero();
        assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class).hasCause(original);
    }

    @Test
    void interruptOutsideACompletedCallbackSealsAdmissionAndFailsTheActualDrain() {
        var owner = new WeightedFairScheduler();
        var dispatcher = new AtomicReference<Thread>();
        owner.submitTask(() -> dispatcher.set(Thread.currentThread()), QueueType.INTERACTIVE);
        try {
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    dispatcher.get() != null && dispatcher.get().getState() == Thread.State.TIMED_WAITING);
            dispatcher.get().interrupt();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThatThrownBy(() -> owner.submitTask(() -> {}, QueueType.BATCH))
                            .isInstanceOf(IllegalStateException.class));
        } finally {
            assertThatThrownBy(owner::close).isInstanceOf(IllegalStateException.class)
                    .hasRootCauseInstanceOf(InterruptedException.class);
        }

        assertThat(dispatcher.get().isAlive()).isFalse();
    }
}
