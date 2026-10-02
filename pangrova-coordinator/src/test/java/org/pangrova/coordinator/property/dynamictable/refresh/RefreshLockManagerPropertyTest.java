package org.pangrova.coordinator.property.dynamictable.refresh;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.spi.coordination.LeaseEntry;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RefreshLockManagerPropertyTest {

    private static final Duration TIMEOUT = Duration.ofMinutes(10);

    @Property(tries = 25)
    void activeResourceHasExactlyOneHolder(@ForAll("lockParams") LockParams params) {
        RefreshLockManager manager = manager();

        LeaseEntry first = manager.tryAcquire(
                params.resource(), params.firstOwner(), TIMEOUT).orElseThrow();

        assertThat(manager.tryAcquire(
                params.resource(), params.secondOwner(), TIMEOUT)).isEmpty();
        assertThat(manager.getLock(params.resource()).epoch()).isEqualTo(first.epoch());
    }

    @Property(tries = 25)
    void ownershipChangeStrictlyAdvancesEpoch(@ForAll("lockParams") LockParams params) {
        RefreshLockManager manager = manager();
        LeaseEntry first = manager.tryAcquire(
                params.resource(), params.firstOwner(), TIMEOUT).orElseThrow();

        manager.release(first);
        LeaseEntry second = manager.tryAcquire(
                params.resource(), params.secondOwner(), TIMEOUT).orElseThrow();

        assertThat(second.epoch()).isGreaterThan(first.epoch());
        assertThat(manager.tryRenew(first, TIMEOUT)).isFalse();
    }

    @Property(tries = 25)
    void exactTokenCanRenew(@ForAll("lockParams") LockParams params) {
        RefreshLockManager manager = manager();
        LeaseEntry held = manager.tryAcquire(
                params.resource(), params.firstOwner(), TIMEOUT).orElseThrow();

        assertThat(manager.tryRenew(held, TIMEOUT)).isTrue();
        assertThat(manager.getLock(params.resource()).coordinatorId())
                .isEqualTo(params.firstOwner());
    }

    record LockParams(String resource, String firstOwner, String secondOwner) {}

    @Provide
    Arbitrary<LockParams> lockParams() {
        Arbitrary<String> resources = Arbitraries.of(
                "sales_summary", "orders_agg", "inventory", "analytics", "metrics");
        Arbitrary<String> firstOwners = Arbitraries.of("coord-1", "coord-2", "coord-3");
        Arbitrary<String> secondOwners = Arbitraries.of("coord-4", "coord-5", "coord-6");
        return Combinators.combine(resources, firstOwners, secondOwners).as(LockParams::new);
    }

    private static RefreshLockManager manager() {
        return new RefreshLockManager(new InMemoryDistributedLease());
    }
}
