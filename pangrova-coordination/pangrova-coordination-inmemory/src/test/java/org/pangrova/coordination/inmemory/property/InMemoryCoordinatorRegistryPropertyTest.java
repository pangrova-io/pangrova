package org.pangrova.coordination.inmemory.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.spi.coordination.CoordinatorRole;
import org.pangrova.spi.coordination.CoordinatorSnapshot;
import org.pangrova.coordination.inmemory.InMemoryCoordinatorRegistry;

/**
 * Property-based tests for {@link InMemoryCoordinatorRegistry}.
 *
 * <p>Validates: Requirements 3.2</p>
 */
class InMemoryCoordinatorRegistryPropertyTest {

    @Provide
    Arbitrary<CoordinatorSnapshot> coordinatorSnapshots() {
        long now = System.currentTimeMillis();
        return Combinators.combine(
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(12).map(s -> "c-" + s),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).map(s -> "host-" + s),
                Arbitraries.integers().between(1024, 65535),
                Arbitraries.of("LEADER", "STANDBY", "STOPPED"),
                Arbitraries.longs().between(0L, 1000L),
                Arbitraries.longs().between(now - 300_000, now),
                Arbitraries.longs().between(now - 600_000, now - 300_000)
        ).as(CoordinatorSnapshot::new);
    }

    /**
     * For any valid CoordinatorSnapshot, after register(), getCoordinator()
     * returns the same snapshot.
     */
    @Property(tries = 10)
    void registerThenGetReturnsRegisteredCoordinator(
            @ForAll("coordinatorSnapshots") CoordinatorSnapshot coordinator) {
        var registry = new InMemoryCoordinatorRegistry();
        registry.register(coordinator);

        assertThat(registry.getCoordinator(coordinator.coordinatorId()))
                .isPresent()
                .contains(coordinator);
    }

    /**
     * For any registered coordinator and any role string, after updateRole(),
     * the coordinator's role matches the new role.
     */
    @Property(tries = 10)
    void updateRoleTakesEffect(
            @ForAll("coordinatorSnapshots") CoordinatorSnapshot coordinator,
            @ForAll("roles") CoordinatorRole newRole) {
        var registry = new InMemoryCoordinatorRegistry();
        registry.register(coordinator);

        registry.updateRole(coordinator.coordinatorId(), newRole);

        var updated = registry.getCoordinator(coordinator.coordinatorId()).orElseThrow();
        assertThat(updated.role()).isEqualTo(newRole.name());
        // identity fields unchanged
        assertThat(updated.coordinatorId()).isEqualTo(coordinator.coordinatorId());
        assertThat(updated.host()).isEqualTo(coordinator.host());
        assertThat(updated.httpPort()).isEqualTo(coordinator.httpPort());
        assertThat(updated.epoch()).isEqualTo(coordinator.epoch());
        assertThat(updated.lastHeartbeatMs()).isEqualTo(coordinator.lastHeartbeatMs());
        assertThat(updated.registeredAtMs()).isEqualTo(coordinator.registeredAtMs());
    }

    @Provide
    Arbitrary<CoordinatorRole> roles() {
        return Arbitraries.of(CoordinatorRole.values());
    }

    /**
     * Register multiple coordinators with various roles. getLeader() either
     * returns empty or returns a coordinator with role="LEADER".
     */
    @Property(tries = 10)
    void getLeaderOnlyReturnsLeaderRole(
            @ForAll("coordinatorSnapshotLists") List<CoordinatorSnapshot> coordinators) {
        var registry = new InMemoryCoordinatorRegistry();
        for (CoordinatorSnapshot c : coordinators) {
            registry.register(c);
        }

        var leader = registry.getLeader();
        if (leader.isPresent()) {
            assertThat(leader.get().role()).isEqualTo("LEADER");
        }
    }

    @Provide
    Arbitrary<List<CoordinatorSnapshot>> coordinatorSnapshotLists() {
        return uniqueCoordinatorSnapshots().list().ofMinSize(1).ofMaxSize(8);
    }

    @Provide
    Arbitrary<CoordinatorSnapshot> uniqueCoordinatorSnapshots() {
        long now = System.currentTimeMillis();
        return Combinators.combine(
                Arbitraries.integers().between(1, 10000).map(i -> "c-" + i),
                Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).map(s -> "host-" + s),
                Arbitraries.integers().between(1024, 65535),
                Arbitraries.of("LEADER", "STANDBY", "STOPPED"),
                Arbitraries.longs().between(0L, 1000L),
                Arbitraries.longs().between(now - 300_000, now),
                Arbitraries.longs().between(now - 600_000, now - 300_000)
        ).as(CoordinatorSnapshot::new);
    }

    /**
     * Register coordinators with varying timestamps. After expireStale(threshold),
     * only stale ones are removed.
     */
    @Property(tries = 10)
    void expireStaleOnlyRemovesOldEntries(
            @ForAll("coordinatorSnapshots") CoordinatorSnapshot template) {
        var registry = new InMemoryCoordinatorRegistry();
        long now = System.currentTimeMillis();
        long thresholdMs = 60_000;

        var fresh = new CoordinatorSnapshot(
                "fresh-" + template.coordinatorId(),
                template.host(),
                template.httpPort(),
                template.role(),
                template.epoch(),
                now,
                template.registeredAtMs());

        var stale = new CoordinatorSnapshot(
                "stale-" + template.coordinatorId(),
                template.host(),
                template.httpPort(),
                template.role(),
                template.epoch(),
                now - thresholdMs - 10_000,
                template.registeredAtMs());

        registry.register(fresh);
        registry.register(stale);

        int expired = registry.expireStale(Duration.ofMillis(thresholdMs));

        assertThat(expired).isEqualTo(1);
        assertThat(registry.getCoordinator(fresh.coordinatorId())).isPresent();
        assertThat(registry.getCoordinator(stale.coordinatorId())).isEmpty();
    }
}
