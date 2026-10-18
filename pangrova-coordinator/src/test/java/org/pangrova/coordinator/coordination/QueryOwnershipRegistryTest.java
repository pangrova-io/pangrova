package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.inmemory.InMemoryExecutionAnchorStore;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.ExecutionAnchorStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exact interleavings use actual fenced backend operations, not simulated receipt counts. */
class QueryOwnershipRegistryTest {
    private static final String OWNER = "owner";
    private static final String PROCESS = "process";
    private static final Duration LEASE = Duration.ofMinutes(1);

    @Test
    void normalReleaseDuringBatchDoesNotLoseRemainingOwnership() {
        var fixture = new Fixture();
        fixture.acquire("q1");
        fixture.acquire("q2");
        fixture.store.beforeRenew = () -> assertThat(fixture.registry.release("q1", OWNER, PROCESS)).isTrue();

        assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isTrue();
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
        assertThat(fixture.registry.localAnchor("q2")).isPresent();
        assertThat(fixture.store.currentReads).isEqualTo(0);
    }

    @Test
    void newAcquisitionDuringBatchDoesNotInvalidateSnapshot() {
        var fixture = new Fixture();
        fixture.acquire("q1");
        fixture.store.beforeRenew = () -> fixture.acquire("q2");

        assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isTrue();
        assertThat(fixture.registry.localAnchor("q1")).isPresent();
        assertThat(fixture.registry.localAnchor("q2")).isPresent();
    }

    @Test
    void actualMissingFenceIsRemovedAndCannotBeStamped() {
        var fixture = new Fixture();
        var lost = fixture.acquire("q1");
        fixture.acquire("q2");
        fixture.store.beforeRenew = () -> assertThat(fixture.store.backend.release(lost)).isTrue();

        assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isFalse();
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
        assertThat(fixture.registry.isCurrentToken("q1", lost.fencingToken())).isFalse();
        assertThat(fixture.registry.localAnchor("q2")).isPresent();
    }

    @Test
    void newAnchorCannotCounterbalanceLostFenceWithTheSameBatchCount() {
        var fixture = new Fixture();
        var lost = fixture.acquire("q1");
        fixture.acquire("q2");
        fixture.store.beforeRenew = () -> {
            assertThat(fixture.store.backend.release(lost)).isTrue();
            fixture.acquire("q3");
        };

        assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isFalse();
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
        assertThat(fixture.registry.localAnchor("q2")).isPresent();
        assertThat(fixture.registry.localAnchor("q3")).isPresent();
    }

    @Test
    void terminalReleaseIsNotStampableDuringBackendDeleteWindow() {
        var fixture = new Fixture();
        var first = fixture.acquire("q1");
        fixture.acquire("q2");
        fixture.store.afterRelease = () -> {
            assertThat(fixture.registry.localAnchor("q1")).isEmpty();
            assertThat(fixture.registry.isCurrentToken("q1", first.fencingToken())).isFalse();
            assertThat(fixture.registry.acquire("q1", OWNER, PROCESS)).isFalse();
            assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isTrue();
        };

        assertThat(fixture.registry.release("q1", OWNER, PROCESS)).isTrue();
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
        fixture.store.afterRelease = () -> { };
        assertThat(fixture.acquire("q1").fencingToken()).isGreaterThan(first.fencingToken());
    }

    @Test
    void failedReleaseRetainsOwnerForExplicitRetry() {
        var fixture = new Fixture();
        var first = fixture.acquire("q1");
        fixture.store.beforeRelease = () -> { throw new IllegalStateException("backend failure"); };

        assertThatThrownBy(() -> fixture.registry.release("q1", OWNER, PROCESS))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("backend failure");
        assertThat(fixture.registry.localAnchor("q1")).contains(first);
        fixture.store.beforeRelease = () -> { };
        assertThat(fixture.registry.release("q1", OWNER, PROCESS)).isTrue();
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
    }

    @Test
    void renewalPublishesAuthoritativeDeadlineWithoutChangingFence() {
        var fixture = new Fixture();
        var first = fixture.acquire("q1");
        fixture.store.clock.advance(Duration.ofSeconds(10));

        assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isTrue();
        var renewed = fixture.registry.localAnchor("q1").orElseThrow();
        assertThat(renewed.fencingToken()).isEqualTo(first.fencingToken());
        assertThat(renewed.acquiredAt()).isEqualTo(first.acquiredAt());
        assertThat(renewed.expiresAt()).isAfter(first.expiresAt());
        assertThat(fixture.store.backend.current("q1")).contains(renewed);
        assertThat(fixture.store.currentReads).isEqualTo(0);
    }

    @Test
    void sameProcessWithAHigherBackendTokenDoesNotRenewTheOldLocalFence() {
        var fixture = new Fixture();
        var first = fixture.acquire("q1");
        fixture.store.beforeRenew = () -> {
            assertThat(fixture.store.backend.yield(first)).isTrue();
            assertThat(fixture.store.backend.claimExpired(OWNER, PROCESS, LEASE, 1)).hasSize(1);
        };

        assertThat(fixture.registry.renewOwned(OWNER, PROCESS)).isFalse();
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
        assertThat(fixture.store.backend.current("q1").orElseThrow().fencingToken())
                .isGreaterThan(first.fencingToken());
    }

    @Test
    void staleReceiptCannotReinsertReleasedAnchorOrOverwriteReacquisition() {
        var fixture = new Fixture();
        var first = fixture.acquire("q1");
        var snapshot = fixture.registry.renewalSnapshot(OWNER, PROCESS);
        var receipt = fixture.store.backend.renewOwned(OWNER, PROCESS, LEASE);
        assertThat(fixture.registry.release("q1", OWNER, PROCESS)).isTrue();
        var replacement = fixture.acquire("q1");

        assertThat(fixture.registry.applyRenewal(snapshot, receipt)).isTrue();
        assertThat(fixture.registry.localAnchor("q1")).contains(replacement);
        assertThat(replacement.fencingToken()).isGreaterThan(first.fencingToken());
    }

    @Test
    void batchYieldMarksEntriesUnusableBeforeBackendIoAndRetainsRetryOnFailure() {
        var fixture = new Fixture();
        var first = fixture.acquire("q1");
        fixture.store.beforeYieldOwned = () -> {
            assertThat(fixture.registry.localAnchor("q1")).isEmpty();
            throw new IllegalStateException("yield failed");
        };

        assertThatThrownBy(() -> fixture.registry.yieldOwned(OWNER, PROCESS))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("yield failed");
        assertThat(fixture.registry.localAnchor("q1")).contains(first);
        fixture.store.beforeYieldOwned = () -> assertThat(fixture.registry.localAnchor("q1")).isEmpty();
        assertThat(fixture.registry.yieldOwned(OWNER, PROCESS)).isEqualTo(1);
        assertThat(fixture.registry.localAnchor("q1")).isEmpty();
    }

    private static final class Fixture {
        private final BoundaryStore store = new BoundaryStore();
        private final QueryOwnershipRegistry registry = new QueryOwnershipRegistry(10, LEASE, store);

        private ExecutionAnchor acquire(String id) {
            assertThat(registry.acquire(id, OWNER, PROCESS)).isTrue();
            return registry.localAnchor(id).orElseThrow();
        }
    }

    private static final class BoundaryStore implements ExecutionAnchorStore {
        private final MutableClock clock = new MutableClock();
        private final InMemoryExecutionAnchorStore backend = new InMemoryExecutionAnchorStore(clock);
        private Runnable beforeRenew = () -> { };
        private Runnable beforeRelease = () -> { };
        private Runnable afterRelease = () -> { };
        private Runnable beforeYieldOwned = () -> { };
        private int currentReads;

        @Override
        public Optional<ExecutionAnchor> tryAcquire(String executionId, String ownerId,
                String incarnation, Duration lease) {
            return backend.tryAcquire(executionId, ownerId, incarnation, lease);
        }

        @Override
        public List<ExecutionAnchor> renewOwned(String ownerId, String incarnation, Duration lease) {
            beforeRenew.run();
            return backend.renewOwned(ownerId, incarnation, lease);
        }

        @Override
        public boolean release(ExecutionAnchor anchor) {
            beforeRelease.run();
            boolean released = backend.release(anchor);
            afterRelease.run();
            return released;
        }

        @Override
        public boolean yield(ExecutionAnchor anchor) {
            return backend.yield(anchor);
        }

        @Override
        public int yieldOwned(String ownerId, String incarnation) {
            beforeYieldOwned.run();
            return backend.yieldOwned(ownerId, incarnation);
        }

        @Override
        public List<ExecutionAnchor> claimExpired(String ownerId, String incarnation, Duration lease, int maxBatch) {
            return backend.claimExpired(ownerId, incarnation, lease, maxBatch);
        }

        @Override
        public Optional<ExecutionAnchor> current(String executionId) {
            currentReads++;
            return backend.current(executionId);
        }
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-10-08T00:00:00Z");

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
