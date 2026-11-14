package org.pangrova.persistence.coordination.postgresql.membership;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link JdbcCoordinatorRegistry} using TestContainers PostgreSQL.
 * Validates register, updateRole, getLeader, heartbeat, expireStale, and unregister.
 *
 * <p>Validates: Requirements 4.2</p>
 */
class JdbcCoordinatorRegistryIntegrationTest extends ContainerTestBase {

    private DataSource dataSource;
    private JdbcCoordinatorRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = org.pangrova.testing.testkit.postgres.MigratedPostgres.create();
        registry = new JdbcCoordinatorRegistry(dataSource);
    }

    @Test
    void registerAndGetCoordinator() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot coordinator = newCoordinator("c-1", "STANDBY", now);
        registry.register(coordinator);

        Optional<CoordinatorSnapshot> result = registry.getCoordinator("c-1");
        assertThat(result).isPresent();
        CoordinatorSnapshot got = result.get();
        assertThat(got.coordinatorId()).isEqualTo("c-1");
        assertThat(got.host()).isEqualTo("host-1");
        assertThat(got.httpPort()).isEqualTo(8080);
        assertThat(got.role()).isEqualTo("STANDBY");
        assertThat(got.epoch()).isEqualTo(1L);
        assertThat(got.lastHeartbeatMs()).isEqualTo(now);
        assertThat(got.registeredAtMs()).isEqualTo(now);
    }

    @Test
    void registerIsIdempotentUpsert() throws Exception {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot first = new CoordinatorSnapshot(
                "c-dup", "host-old", 8080, "STANDBY", 1L, now, now);
        registry.register(first);

        CoordinatorSnapshot second = new CoordinatorSnapshot(
                "c-dup", "host-new", 9999, "LEADER", 2L, now + 1000, now + 1000);
        registry.register(second);

        // Verify only one row exists
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM coordinator_registry WHERE coordinator_id='c-dup'")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }

        // Verify second registration overwrote the first
        Optional<CoordinatorSnapshot> result = registry.getCoordinator("c-dup");
        assertThat(result).isPresent();
        CoordinatorSnapshot got = result.get();
        assertThat(got.host()).isEqualTo("host-new");
        assertThat(got.httpPort()).isEqualTo(9999);
        assertThat(got.role()).isEqualTo("LEADER");
        assertThat(got.epoch()).isEqualTo(2L);
    }

    @Test
    void heartbeatUpdatesTimestamp() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot coordinator = newCoordinator("c-hb", "STANDBY", now);
        registry.register(coordinator);

        registry.heartbeat("c-hb");

        CoordinatorSnapshot got = registry.getCoordinator("c-hb").orElseThrow();
        assertThat(got.lastHeartbeatMs()).isGreaterThanOrEqualTo(now);
        // Static fields unchanged
        assertThat(got.coordinatorId()).isEqualTo("c-hb");
        assertThat(got.host()).isEqualTo("host-1");
        assertThat(got.httpPort()).isEqualTo(8080);
        assertThat(got.role()).isEqualTo("STANDBY");
        assertThat(got.epoch()).isEqualTo(1L);
    }

    @Test
    void updateRoleChangesRole() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot coordinator = newCoordinator("c-role", "STANDBY", now);
        registry.register(coordinator);

        registry.updateRole("c-role", org.pangrova.coordination.api.enums.CoordinatorRole.LEADER);

        CoordinatorSnapshot got = registry.getCoordinator("c-role").orElseThrow();
        assertThat(got.role()).isEqualTo("LEADER");
        // Other fields unchanged
        assertThat(got.coordinatorId()).isEqualTo("c-role");
        assertThat(got.host()).isEqualTo("host-1");
        assertThat(got.httpPort()).isEqualTo(8080);
        assertThat(got.epoch()).isEqualTo(1L);
    }

    @Test
    void getLeaderReturnsActiveLeader() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot leader = new CoordinatorSnapshot(
                "c-leader", "host-leader", 8080, "LEADER", 1L, now, now);
        registry.register(leader);

        Optional<CoordinatorSnapshot> result = registry.getLeader();
        assertThat(result).isPresent();
        assertThat(result.get().coordinatorId()).isEqualTo("c-leader");
        assertThat(result.get().role()).isEqualTo("LEADER");
    }

    @Test
    void getLeaderReturnsEmptyWhenNoLeader() {
        long now = System.currentTimeMillis();
        CoordinatorSnapshot standby1 = new CoordinatorSnapshot(
                "c-s1", "host-1", 8080, "STANDBY", 1L, now, now);
        CoordinatorSnapshot standby2 = new CoordinatorSnapshot(
                "c-s2", "host-2", 8081, "STANDBY", 1L, now, now);
        registry.register(standby1);
        registry.register(standby2);

        Optional<CoordinatorSnapshot> result = registry.getLeader();
        assertThat(result).isEmpty();
    }

    @Test
    void getLeaderReturnsEmptyWhenLeaderStale() {
        long now = System.currentTimeMillis();
        // Leader with heartbeat older than 30s (the default stale threshold)
        CoordinatorSnapshot staleLeader = new CoordinatorSnapshot(
                "c-stale-leader", "host-1", 8080, "LEADER", 1L,
                now - Duration.ofSeconds(60).toMillis(), now - Duration.ofSeconds(60).toMillis());
        registry.register(staleLeader);

        Optional<CoordinatorSnapshot> result = registry.getLeader();
        assertThat(result).isEmpty();
    }

    @Test
    void getAllCoordinatorsFiltersStale() {
        long now = System.currentTimeMillis();
        // Fresh coordinator
        CoordinatorSnapshot fresh = new CoordinatorSnapshot(
                "c-fresh", "host-1", 8080, "STANDBY", 1L, now, now);
        registry.register(fresh);

        // Stale coordinator: heartbeat 10 minutes ago
        CoordinatorSnapshot stale = new CoordinatorSnapshot(
                "c-stale", "host-2", 8081, "STANDBY", 1L,
                now - Duration.ofMinutes(10).toMillis(), now - Duration.ofMinutes(10).toMillis());
        registry.register(stale);

        List<CoordinatorSnapshot> active = registry.getAllCoordinators(Duration.ofMinutes(5));
        assertThat(active).hasSize(1);
        assertThat(active.get(0).coordinatorId()).isEqualTo("c-fresh");
    }

    @Test
    void expireStaleRemovesOldEntries() {
        long now = System.currentTimeMillis();
        // Old coordinator
        CoordinatorSnapshot old = new CoordinatorSnapshot(
                "c-old", "host-1", 8080, "STANDBY", 1L,
                now - Duration.ofHours(1).toMillis(), now - Duration.ofHours(1).toMillis());
        registry.register(old);

        // Recent coordinator
        CoordinatorSnapshot recent = new CoordinatorSnapshot(
                "c-recent", "host-2", 8081, "LEADER", 1L, now, now);
        registry.register(recent);

        int expired = registry.expireStale(Duration.ofMinutes(30));
        assertThat(expired).isEqualTo(1);
        assertThat(registry.getCoordinator("c-old")).isEmpty();
        assertThat(registry.getCoordinator("c-recent")).isPresent();
    }

    @Test
    void unregisterRemovesCoordinator() {
        long now = System.currentTimeMillis();
        registry.register(newCoordinator("c-rm", "STANDBY", now));
        assertThat(registry.getCoordinator("c-rm")).isPresent();

        registry.unregister("c-rm");
        assertThat(registry.getCoordinator("c-rm")).isEmpty();
    }

    // --- helpers ---

    private static CoordinatorSnapshot newCoordinator(String id, String role, long lastHeartbeatMs) {
        return new CoordinatorSnapshot(id, "host-1", 8080, role, 1L, lastHeartbeatMs, lastHeartbeatMs);
    }




}
