package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ControlEpochFenceTest {

    @Test
    void standaloneModeDoesNotRequireAnEpoch() {
        ControlEpochFence fence = new ControlEpochFence();

        fence.verify(null);
    }

    @Test
    void requiredFenceRejectsUntilControlHandshakeCompletes() {
        ControlEpochFence fence = requiredFence();

        assertRejected(fence, null,
                ControlEpochFenceException.Reason.CONTROL_PLANE_NOT_READY);
    }

    @Test
    void rejectsMissingInvalidStaleAndFabricatedFutureEpochs() {
        ControlEpochFence fence = requiredFence();
        fence.observe(12);

        assertRejected(fence, null, ControlEpochFenceException.Reason.MISSING_EPOCH);
        assertRejected(fence, 0L, ControlEpochFenceException.Reason.INVALID_EPOCH);
        assertRejected(fence, 11L, ControlEpochFenceException.Reason.EPOCH_MISMATCH);
        assertRejected(fence, 13L, ControlEpochFenceException.Reason.EPOCH_AHEAD);
        fence.verify(12L);
    }

    @Test
    void onlyControlChannelCanAdvanceAndEpochNeverRollsBack() {
        ControlEpochFence fence = requiredFence();
        fence.observe(8);
        fence.observe(7);

        fence.verify(8L);
        fence.observe(9);

        assertThat(fence.currentEpoch()).hasValue(9L);
        assertRejected(fence, 8L, ControlEpochFenceException.Reason.EPOCH_MISMATCH);
        fence.verify(9L);
    }

    @Test
    void concurrentControlUpdatesConvergeOnMaximumEpoch() throws Exception {
        ControlEpochFence fence = requiredFence();
        int updates = 64;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(updates);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            for (int epoch = 1; epoch <= updates; epoch++) {
                long observed = epoch;
                executor.execute(() -> {
                    try {
                        start.await();
                        fence.observe(observed);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }

        assertThat(fence.currentEpoch()).hasValue(updates);
        fence.verify((long) updates);
    }

    @Test
    void invalidControlEpochCannotArmFence() {
        ControlEpochFence fence = requiredFence();

        assertThatThrownBy(() -> fence.observe(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(fence.currentEpoch()).isEmpty();
    }

    private static ControlEpochFence requiredFence() {
        ControlEpochFence fence = new ControlEpochFence();
        fence.requireHandshake();
        return fence;
    }

    private static void assertRejected(
            ControlEpochFence fence, Long epoch,
            ControlEpochFenceException.Reason expectedReason) {
        assertThatThrownBy(() -> fence.verify(epoch))
                .isInstanceOfSatisfying(ControlEpochFenceException.class,
                        error -> assertThat(error.reason()).isEqualTo(expectedReason));
    }
}
