package org.pangrova.resource.scheduler;

import org.pangrova.resource.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

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
}
