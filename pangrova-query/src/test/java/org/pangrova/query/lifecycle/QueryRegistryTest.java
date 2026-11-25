package org.pangrova.query.lifecycle;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryRegistryTest {
    @Test
    void authoritySetupFailureReleasesTheReservationWithoutPublishingOrCounting() {
        var registry = new QueryRegistry();
        var state = new QueryStateMachine();
        assertThatThrownBy(() -> registry.register("q", state, () -> {
            throw new IllegalStateException("authority lost");
        })).hasMessage("authority lost");
        assertThat(registry.get("q")).isNull();
        assertThat(registry.outstandingCount()).isZero();
        assertThat(registry.register("q", state, () -> {})).isNull();
        assertThat(registry.outstandingIds()).containsExactly("q");
        registry.complete("q", state);
        registry.complete("q", state);
        assertThat(registry.outstandingCount()).isZero();
        assertThat(registry.awaitDrain(0, () -> {})).isTrue();
    }

    @Test
    void pendingSubmissionReservationBlocksDrainUntilReleased() {
        var registry = new QueryRegistry();
        assertThat(registry.reserveSubmission("candidate")).isTrue();
        assertThat(registry.reserveSubmission("candidate")).isFalse();
        assertThat(registry.outstandingCount()).isOne();
        assertThat(registry.awaitDrain(0, () -> {})).isFalse();
        registry.releaseSubmission("candidate");
        assertThat(registry.outstandingCount()).isZero();
        assertThat(registry.awaitDrain(0, () -> {})).isTrue();
    }

    @Test
    void registrationTransfersPendingReservationWithoutDrainGap() {
        var registry = new QueryRegistry();
        var state = new QueryStateMachine();
        assertThat(registry.reserveSubmission("candidate")).isTrue();
        assertThat(registry.register("candidate", state, () -> {})).isNull();
        registry.releaseSubmission("candidate");
        assertThat(registry.outstandingCount()).isOne();
        assertThat(registry.awaitDrain(0, () -> {})).isFalse();
        registry.complete("candidate", state);
        assertThat(registry.awaitDrain(0, () -> {})).isTrue();
    }

    @Test
    void readersCannotCancelHalfBoundQueryAndACompetitorCannotReopenIssuance() throws Exception {
        var registry = new QueryRegistry();
        var state = new QueryStateMachine();
        var started = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var first = executor.submit(() -> registry.register("q", state, () -> {
                started.countDown();
                try {
                    if (!finish.await(10, TimeUnit.SECONDS)) throw new AssertionError("initialization timed out");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }));
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(registry.get("q")).isNull();
            assertThat(registry.register("q", new QueryStateMachine(), () -> {
                throw new AssertionError("duplicate acquired authority");
            })).isSameAs(state);
            finish.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isNull();
            assertThat(registry.get("q")).isSameAs(state);
            assertThat(registry.outstandingCount()).isOne();
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }
}
