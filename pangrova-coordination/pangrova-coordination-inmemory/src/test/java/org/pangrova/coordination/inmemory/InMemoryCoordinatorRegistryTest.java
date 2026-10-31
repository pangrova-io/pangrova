package org.pangrova.coordination.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.pangrova.coordination.api.CoordinatorRole;
import org.pangrova.coordination.api.CoordinatorSnapshot;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InMemoryCoordinatorRegistry}.
 *
 * <p>Validates: Requirements 3.2</p>
 */
class InMemoryCoordinatorRegistryTest {

    private final InMemoryCoordinatorRegistry registry = new InMemoryCoordinatorRegistry();

    private CoordinatorSnapshot coordinator(String id, String role, long lastHeartbeatMs) {
        return new CoordinatorSnapshot(id, "host-" + id, 8080, role, 1L, lastHeartbeatMs, lastHeartbeatMs - 10_000);
    }

    @Test
    void registerAndGet() {
        CoordinatorSnapshot c = coordinator("c1", "STANDBY", System.currentTimeMillis());
        registry.register(c);

        assertThat(registry.getCoordinator("c1")).isPresent().contains(c);
    }

    @Test
    void registerIsIdempotent() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot first = coordinator("c1", "STANDBY", now - 5000);
        CoordinatorSnapshot second = coordinator("c1", "LEADER", now);

        registry.register(first);
        registry.register(second);

        assertThat(registry.coordinatorCount()).isEqualTo(1);
        assertThat(registry.getCoordinator("c1")).isPresent().contains(second);
    }

    @Test
    void heartbeatUpdatesTimestamp() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot c = new CoordinatorSnapshot("c1", "host-a", 8080, "STANDBY", 5L, now - 5000, now - 60_000);
        registry.register(c);

        registry.heartbeat("c1");

        CoordinatorSnapshot updated = registry.getCoordinator("c1").orElseThrow();
        // timestamp updated
        assertThat(updated.lastHeartbeatMs()).isGreaterThanOrEqualTo(now);
        // identity fields unchanged
        assertThat(updated.coordinatorId()).isEqualTo("c1");
        assertThat(updated.host()).isEqualTo("host-a");
        assertThat(updated.httpPort()).isEqualTo(8080);
        assertThat(updated.role()).isEqualTo("STANDBY");
        assertThat(updated.epoch()).isEqualTo(5L);
        assertThat(updated.registeredAtMs()).isEqualTo(now - 60_000);
    }

    @Test
    void heartbeatForUnregisteredCoordinatorIsNoOp() {
        registry.heartbeat("unknown");

        assertThat(registry.getCoordinator("unknown")).isEmpty();
        assertThat(registry.coordinatorCount()).isZero();
    }

    @Test
    void unregisterRemovesCoordinator() {
        registry.register(coordinator("c1", "STANDBY", System.currentTimeMillis()));
        registry.unregister("c1");

        assertThat(registry.getCoordinator("c1")).isEmpty();
        assertThat(registry.coordinatorCount()).isZero();
    }

    @Test
    void unregisterUnknownCoordinatorIsNoOp() {
        registry.unregister("ghost");

        assertThat(registry.coordinatorCount()).isZero();
    }

    @Test
    void updateRoleChangesRole() {
        long now = System.currentTimeMillis();
        registry.register(coordinator("c1", "STANDBY", now));

        registry.updateRole("c1", CoordinatorRole.LEADER);

        CoordinatorSnapshot updated = registry.getCoordinator("c1").orElseThrow();
        assertThat(updated.role()).isEqualTo("LEADER");
    }

    @Test
    void updateRoleForUnregisteredIsNoOp() {
        registry.updateRole("ghost", CoordinatorRole.LEADER);

        assertThat(registry.coordinatorCount()).isZero();
    }

    @Test
    void getLeaderReturnsLeader() {
        long now = System.currentTimeMillis();
        registry.register(coordinator("c1", "LEADER", now));
        registry.register(coordinator("c2", "STANDBY", now));

        assertThat(registry.getLeader()).isPresent();
        assertThat(registry.getLeader().get().coordinatorId()).isEqualTo("c1");
    }

    @Test
    void getLeaderReturnsEmptyWhenNoLeader() {
        long now = System.currentTimeMillis();
        registry.register(coordinator("c1", "STANDBY", now));
        registry.register(coordinator("c2", "STANDBY", now));

        assertThat(registry.getLeader()).isEmpty();
    }

    @Test
    void getAllCoordinatorsFiltersStale() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot fresh = coordinator("fresh", "STANDBY", now);
        CoordinatorSnapshot stale = coordinator("stale", "STANDBY", now - 60_000);

        registry.register(fresh);
        registry.register(stale);

        List<CoordinatorSnapshot> active = registry.getAllCoordinators(Duration.ofSeconds(30));
        assertThat(active).hasSize(1);
        assertThat(active.get(0).coordinatorId()).isEqualTo("fresh");
    }

    @Test
    void expireStaleRemovesOldEntries() {
        long now = System.currentTimeMillis();
        registry.register(coordinator("old1", "STANDBY", now - 120_000));
        registry.register(coordinator("old2", "STANDBY", now - 90_000));
        registry.register(coordinator("fresh", "LEADER", now));

        int expired = registry.expireStale(Duration.ofSeconds(60));

        assertThat(expired).isEqualTo(2);
        assertThat(registry.coordinatorCount()).isEqualTo(1);
        assertThat(registry.getCoordinator("fresh")).isPresent();
        assertThat(registry.getCoordinator("old1")).isEmpty();
        assertThat(registry.getCoordinator("old2")).isEmpty();
    }

    @Test
    void expireStaleReturnsZeroWhenNoneStale() {
        long now = System.currentTimeMillis();
        registry.register(coordinator("c1", "LEADER", now));
        registry.register(coordinator("c2", "STANDBY", now));

        int expired = registry.expireStale(Duration.ofSeconds(60));

        assertThat(expired).isZero();
        assertThat(registry.coordinatorCount()).isEqualTo(2);
    }
}
