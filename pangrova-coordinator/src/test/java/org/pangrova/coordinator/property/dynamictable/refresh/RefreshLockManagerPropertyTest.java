package org.pangrova.coordinator.property.dynamictable.refresh;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Duration;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link RefreshLockManager}.
 *
 * <p><b>Validates: Requirements 21</b></p>
 */
class RefreshLockManagerPropertyTest {

    /**
     * Property 1: Same dtName cannot be acquired twice by different coordinators
     * without release.
     *
     * <p>Once coordinator A holds a lock for a dtName at a given epoch,
     * coordinator B at the same or lower epoch must be rejected.</p>
     */
    @Property(tries = 10)
    void sameDtNameCannotBeAcquiredTwiceWithoutRelease(
            @ForAll("lockParams") LockParams params) {

        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        Duration timeout = Duration.ofMinutes(10);

        boolean first = manager.tryAcquire(
                params.dtName(), params.coordA(), params.epoch(), timeout);
        assertThat(first).isTrue();

        // Different coordinator, same or lower epoch → must be rejected
        boolean second = manager.tryAcquire(
                params.dtName(), params.coordB(), params.epoch(), timeout);
        assertThat(second).isFalse();
    }

    /**
     * Property 2: After release, the same dtName can be re-acquired.
     */
    @Property(tries = 10)
    void afterReleaseCanReacquire(
            @ForAll("lockParams") LockParams params) {

        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        Duration timeout = Duration.ofMinutes(10);

        boolean first = manager.tryAcquire(
                params.dtName(), params.coordA(), params.epoch(), timeout);
        assertThat(first).isTrue();

        manager.release(params.dtName(), params.coordA());

        boolean reacquired = manager.tryAcquire(
                params.dtName(), params.coordB(), params.epoch(), timeout);
        assertThat(reacquired).isTrue();
    }

    /**
     * Property 3: Expired locks are cleaned by expireStale.
     *
     * <p>A lock acquired with zero-duration timeout is immediately expired.
     * After expireStale(), the lock count should drop to zero.</p>
     */
    @Property(tries = 10)
    void expiredLocksCleanedByExpireStale(
            @ForAll("dtNames") String dtName) {

        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        // Acquire with zero timeout → immediately expired
        boolean acquired = manager.tryAcquire(
                dtName, "coord-1", 1L, Duration.ZERO);
        assertThat(acquired).isTrue();
        assertThat(manager.lockCount()).hasValue(1);

        manager.expireStale();

        assertThat(manager.lockCount()).hasValue(0);
        assertThat(manager.getLock(dtName)).isNull();
    }

    /**
     * Property 4: Renewal by the current holder succeeds and preserves the lock.
     *
     * <p>After acquiring a lock, the same coordinator can renew it, and the
     * lock remains held by that coordinator.</p>
     *
     * <p><b>Validates: Requirements FR-4</b></p>
     */
    @Property(tries = 10)
    void renewalByHolderSucceedsAndPreservesLock(
            @ForAll("lockParams") LockParams params) {

        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        Duration timeout = Duration.ofMinutes(10);

        boolean acquired = manager.tryAcquire(
                params.dtName(), params.coordA(), params.epoch(), timeout);
        assertThat(acquired).isTrue();

        boolean renewed = manager.tryRenew(
                params.dtName(), params.coordA(), timeout);
        assertThat(renewed).isTrue();

        // Lock is still held by the same coordinator
        assertThat(manager.getLock(params.dtName())).isNotNull();
        assertThat(manager.getLock(params.dtName()).coordinatorId())
                .isEqualTo(params.coordA());
    }

    /**
     * Property 5: Renewal by a non-holder always fails.
     *
     * <p>If coordinator A holds the lock, coordinator B cannot renew it.</p>
     *
     * <p><b>Validates: Requirements FR-4</b></p>
     */
    @Property(tries = 10)
    void renewalByNonHolderAlwaysFails(
            @ForAll("lockParams") LockParams params) {

        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        Duration timeout = Duration.ofMinutes(10);

        boolean acquired = manager.tryAcquire(
                params.dtName(), params.coordA(), params.epoch(), timeout);
        assertThat(acquired).isTrue();

        boolean renewed = manager.tryRenew(
                params.dtName(), params.coordB(), timeout);
        assertThat(renewed).isFalse();

        // Lock is still held by the original coordinator
        assertThat(manager.getLock(params.dtName()).coordinatorId())
                .isEqualTo(params.coordA());
    }

    /**
     * Property 6: Renewal of a non-existent lock always fails.
     *
     * <p>Renewing a lock that was never acquired returns false.</p>
     *
     * <p><b>Validates: Requirements FR-4</b></p>
     */
    @Property(tries = 10)
    void renewalOfNonExistentLockFails(
            @ForAll("dtNames") String dtName) {

        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        boolean renewed = manager.tryRenew(dtName, "coord-1", Duration.ofMinutes(10));
        assertThat(renewed).isFalse();
    }

    // ── Generators ──────────────────────────────────────────────────

    record LockParams(String dtName, String coordA, String coordB, long epoch) {}

    @Provide
    Arbitrary<LockParams> lockParams() {
        Arbitrary<String> dtNames = Arbitraries.of(
                "sales_summary", "orders_agg", "inventory", "analytics", "metrics");
        Arbitrary<String> coordAs = Arbitraries.of("coord-1", "coord-2", "coord-3");
        Arbitrary<String> coordBs = Arbitraries.of("coord-4", "coord-5", "coord-6");
        Arbitrary<Long> epochs = Arbitraries.longs().between(1, 100);

        return Combinators.combine(dtNames, coordAs, coordBs, epochs)
                .as(LockParams::new);
    }

    @Provide
    Arbitrary<String> dtNames() {
        return Arbitraries.of(
                "dt_alpha", "dt_beta", "dt_gamma", "dt_delta", "dt_epsilon");
    }
}
