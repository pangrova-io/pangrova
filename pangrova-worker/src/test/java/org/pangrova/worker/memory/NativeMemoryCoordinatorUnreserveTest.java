package org.pangrova.worker.memory;

import org.pangrova.resource.memory.MemoryArbitrator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A-MEM-02: {@code unreserve} is invoked from the native JNI allocation callback. If it propagates
 * an exception, the C++ layer re-accumulates cross-thread unreserve debt, which under repeated
 * failure grows unbounded and self-locks memory accounting (native OOM). These tests verify
 * {@code unreserve} is non-failing: an arbitrator error during release is absorbed as accounting
 * drift and never propagated to the JNI caller.
 */
class NativeMemoryCoordinatorUnreserveTest {

    @Test
    void unreserveNeverPropagatesArbitratorFailure() {
        long max = 1_000_000L;
        MemoryArbitrator throwingArbitrator = new MemoryArbitrator(max, 0) {
            @Override
            public long reserve(long bytes) {
                return bytes; // grant so the coordinator tracks a reservation to release
            }

            @Override
            public void releaseReservation(long bytes) {
                throw new IllegalStateException("simulated accounting failure during release");
            }
        };
        NativeMemoryCoordinator coordinator =
                new NativeMemoryCoordinator(max, throwingArbitrator, null);
        coordinator.reserve(1_000);

        assertThatCode(() -> coordinator.unreserve(1_000))
                .as("unreserve must absorb arbitrator failures, never propagate to the JNI caller")
                .doesNotThrowAnyException();
    }

    @Test
    void unreserveSucceedsNormallyWithoutFailure() {
        long max = 1_000_000L;
        NativeMemoryCoordinator coordinator = new NativeMemoryCoordinator(max);
        coordinator.reserve(2_000);
        assertThatCode(() -> coordinator.unreserve(2_000)).doesNotThrowAnyException();
    }
}
