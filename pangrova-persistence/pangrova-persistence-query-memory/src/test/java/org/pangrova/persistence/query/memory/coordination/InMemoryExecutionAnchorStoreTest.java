package org.pangrova.persistence.query.memory.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.ownership.ExecutionAnchor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryExecutionAnchorStoreTest {

    @Test
    void exactIncarnationAndTokenFenceRelease() {
        MutableClock clock = new MutableClock();
        var store = new InMemoryExecutionAnchorStore(clock);
        ExecutionAnchor first = store.tryAcquire(
                "query-1", "coordinator-a", "process-a",
                Duration.ofSeconds(30)).orElseThrow();

        assertThat(store.tryAcquire(
                "query-1", "coordinator-a", "process-b",
                Duration.ofSeconds(30))).isEmpty();
        assertThat(store.release(new ExecutionAnchor(
                first.executionId(), first.ownerId(), "process-b",
                first.fencingToken(), first.acquiredAt(), first.expiresAt())))
                .isFalse();
        assertThat(store.release(first)).isTrue();

        ExecutionAnchor replacement = store.tryAcquire(
                "query-1", "coordinator-b", "process-b",
                Duration.ofSeconds(30)).orElseThrow();
        assertThat(replacement.fencingToken()).isGreaterThan(first.fencingToken());
    }

    @Test
    void renewsOneProcessInOneBatchAndNeverRevivesExpiredAnchor() {
        MutableClock clock = new MutableClock();
        var store = new InMemoryExecutionAnchorStore(clock);
        store.tryAcquire("q-1", "a", "a-1", Duration.ofSeconds(10)).orElseThrow();
        store.tryAcquire("q-2", "a", "a-1", Duration.ofSeconds(1)).orElseThrow();
        store.tryAcquire("q-3", "a", "a-2", Duration.ofSeconds(10)).orElseThrow();
        clock.advance(Duration.ofSeconds(2));

        assertThat(store.renewOwned("a", "a-1", Duration.ofSeconds(10)))
                .containsExactly(store.current("q-1").orElseThrow());
        assertThat(store.current("q-1")).isPresent();
        assertThat(store.current("q-2")).isEmpty();
        assertThat(store.current("q-3")).isPresent();
    }

    @Test
    void claimsExpiredExecutionsInBoundedOrder() {
        MutableClock clock = new MutableClock();
        var store = new InMemoryExecutionAnchorStore(clock);
        ExecutionAnchor first = store.tryAcquire(
                "q-1", "a", "a-1", Duration.ofSeconds(1)).orElseThrow();
        ExecutionAnchor second = store.tryAcquire(
                "q-2", "a", "a-1", Duration.ofSeconds(1)).orElseThrow();
        clock.advance(Duration.ofSeconds(2));

        var batchOne = store.claimExpired(
                "b", "b-1", Duration.ofSeconds(30), 1);
        var batchTwo = store.claimExpired(
                "b", "b-1", Duration.ofSeconds(30), 1);

        assertThat(batchOne).hasSize(1);
        assertThat(batchTwo).hasSize(1);
        assertThat(batchOne.get(0).executionId()).isNotEqualTo(batchTwo.get(0).executionId());
        assertThat(batchOne.get(0).fencingToken())
                .isGreaterThan(Math.min(first.fencingToken(), second.fencingToken()));
    }

    @Test
    void gracefulYieldMakesOnlyExactProcessEligibleForTakeover() {
        var store = new InMemoryExecutionAnchorStore();
        store.tryAcquire("q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        store.tryAcquire("q-2", "a", "a-2", Duration.ofMinutes(1)).orElseThrow();

        assertThat(store.yieldOwned("a", "a-1")).isEqualTo(1);
        assertThat(store.claimExpired(
                "b", "b-1", Duration.ofMinutes(1), 10))
                .extracting(ExecutionAnchor::executionId)
                .containsExactly("q-1");
        assertThat(store.current("q-2")).isPresent();
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-09-07T00:00:00Z");

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
