package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskEpochFenceTest {

    @Test
    void unstampedEpochZero_alwaysAcceptedAndDoesNotAdvanceFence() {
        TaskEpochFence fence = new TaskEpochFence();
        assertThat(fence.accept(0L)).isTrue();
        assertThat(fence.accept(0L)).isTrue();
        assertThat(fence.currentEpoch()).isEqualTo(0L);
    }

    @Test
    void freshEpoch_advancesFence() {
        TaskEpochFence fence = new TaskEpochFence();
        assertThat(fence.accept(5L)).isTrue();
        assertThat(fence.currentEpoch()).isEqualTo(5L);
        assertThat(fence.accept(7L)).isTrue();
        assertThat(fence.currentEpoch()).isEqualTo(7L);
    }

    @Test
    void sameEpoch_acceptedWithoutChange() {
        TaskEpochFence fence = new TaskEpochFence();
        assertThat(fence.accept(5L)).isTrue();
        assertThat(fence.accept(5L)).isTrue();
        assertThat(fence.currentEpoch()).isEqualTo(5L);
    }

    @Test
    void staleEpochFromZombie_rejected() {
        TaskEpochFence fence = new TaskEpochFence();
        fence.accept(10L);
        assertThat(fence.accept(3L)).isFalse();
        assertThat(fence.currentEpoch()).isEqualTo(10L);
    }

    @Test
    void unstampedZeroAfterRealEpoch_stillAccepted() {
        TaskEpochFence fence = new TaskEpochFence();
        fence.accept(10L);
        assertThat(fence.accept(0L)).isTrue();
        assertThat(fence.currentEpoch()).isEqualTo(10L);
    }

    @Test
    void concurrentAccept_neverRegressesFence() throws InterruptedException {
        TaskEpochFence fence = new TaskEpochFence();
        int threads = 16;
        int perThread = 500;
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final long base = (t + 1) * 1000L;
            workers[t] = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    fence.accept(base + i);
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }
        // Highest epoch any thread submitted is (threads*1000 + perThread - 1).
        assertThat(fence.currentEpoch()).isEqualTo(threads * 1000L + perThread - 1);
    }
}
