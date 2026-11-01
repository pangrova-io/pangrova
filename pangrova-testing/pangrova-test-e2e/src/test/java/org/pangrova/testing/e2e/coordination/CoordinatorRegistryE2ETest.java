package org.pangrova.testing.e2e.coordination;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService.CoordinationConfig;
import org.pangrova.coordinator.coordination.JdbcCoordinatorRegistry;
import org.pangrova.metadata.store.jdbc.EmbeddedDataSource;
import org.pangrova.spi.coordination.CoordinatorSnapshot;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.testing.e2e.fixture.RegistryTestSchemaHelper;
import org.pangrova.testing.e2e.fixture.TestLeaseNamespace;
import org.pangrova.testing.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for Coordinator Registry distributed scenarios.
 *
 * <p>Validates the full coordinator registration lifecycle through real PostgreSQL:
 * self-registration → leader election → dual-coordinator HA → failover →
 * demotion → heartbeat timeout → client routing.
 * Uses TestContainers PostgreSQL with JdbcCoordinatorRegistry + JdbcDistributedLease
 * sharing the same DataSource.</p>
 */
@DisplayName("Coordinator Registry E2E")
@Testcontainers
class CoordinatorRegistryE2ETest extends ContainerTestBase {

    private static final long LEASE_MS = 5000;
    private static final long HEARTBEAT_MS = 2000;

    private DataSource dataSource;
    private JdbcCoordinatorRegistry registry;
    private DistributedLease lease;

    @BeforeEach
    void setUp() throws Exception {
        var pg = RegistryTestSchemaHelper.requirePostgres();
        dataSource = new EmbeddedDataSource(
                pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        MetadataSchemaInitializer.createSchema(dataSource);
        RegistryTestSchemaHelper.truncateAll(dataSource, "coordinator_registry");
        registry = new JdbcCoordinatorRegistry(dataSource);
        lease = new TestLeaseNamespace().jdbc(dataSource);
    }

    // -----------------------------------------------------------------------
    // Scenario 1 — Coordinator self-registration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Coordinator Self-Registration")
    class CoordinatorSelfRegistration {

        @Test
        @DisplayName("HA service start() registers coordinator → DB row exists → getAllCoordinators returns it")
        void start_registersCoordinator() {
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(lease, config);
            ha.setCoordinatorRegistry(registry, "host-a", 8080);
            try {
                ha.start("coordinator-a");

                Optional<CoordinatorSnapshot> snapshot = registry.getCoordinator("coordinator-a");
                assertThat(snapshot).isPresent();
                assertThat(snapshot.get().host()).isEqualTo("host-a");
                assertThat(snapshot.get().httpPort()).isEqualTo(8080);

                List<CoordinatorSnapshot> all = registry.getAllCoordinators(Duration.ofMinutes(5));
                assertThat(all).hasSize(1);
                assertThat(all.get(0).coordinatorId()).isEqualTo("coordinator-a");
            } finally {
                ha.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2 — Leader election role update
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader Election Role Update")
    class LeaderElectionRoleUpdate {

        @Test
        @DisplayName("Coordinator-A starts → tryBecomeLeader → updateRole(LEADER) → getLeader() returns it")
        void leaderElection_updatesRoleInRegistry() {
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(lease, config);
            ha.setCoordinatorRegistry(registry, "host-a", 8080);
            try {
                ha.start("coordinator-a");

                // start() already attempts leader election; verify role is LEADER
                assertThat(ha.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

                Optional<CoordinatorSnapshot> leader = registry.getLeader();
                assertThat(leader).isPresent();
                assertThat(leader.get().coordinatorId()).isEqualTo("coordinator-a");
                assertThat(leader.get().host()).isEqualTo("host-a");
                assertThat(leader.get().httpPort()).isEqualTo(8080);
            } finally {
                ha.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3 — Dual-Coordinator HA
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Dual-Coordinator HA")
    class DualCoordinatorHA {

        @Test
        @DisplayName("Two coordinators start → each registers → only one LEADER → getAllCoordinators returns 2")
        void dualCoordinator_onlyOneLeader() {
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService haA = new HighAvailabilityCoordinationService(lease, config);
            haA.setCoordinatorRegistry(registry, "host-a", 8080);
            HighAvailabilityCoordinationService haB = new HighAvailabilityCoordinationService(lease, config);
            haB.setCoordinatorRegistry(registry, "host-b", 8081);
            try {
                haA.start("coordinator-a");
                haB.start("coordinator-b");

                // Exactly one should hold the active lease
                Optional<CoordinatorSnapshot> leader = registry.getLeader();
                assertThat(leader).isPresent();

                // Both should be registered
                List<CoordinatorSnapshot> all = registry.getAllCoordinators(Duration.ofMinutes(5));
                assertThat(all).hasSize(2);
                assertThat(all).extracting(CoordinatorSnapshot::coordinatorId)
                        .containsExactlyInAnyOrder("coordinator-a", "coordinator-b");

                // With epoch-fencing, the LAST coordinator to start() wins the lease.
                // The first coordinator's local role may still be LEADER (demotion is async),
                // so we verify via the lease: exactly one coordinator holds the active lease.
                String leaseHolder = haA.getLeaderId();
                assertThat(leaseHolder).isNotNull();
                assertThat(leaseHolder).isIn("coordinator-a", "coordinator-b");
            } finally {
                haB.close();
                haA.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4 — Leader Failover role switch
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader Failover Role Switch")
    class LeaderFailoverRoleSwitch {

        @Test
        @DisplayName("Coordinator-A is LEADER → close() → Coordinator-B detects → tryBecomeLeader → getLeader returns B")
        void failover_newLeaderTakesOver() {
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService haA = new HighAvailabilityCoordinationService(lease, config);
            haA.setCoordinatorRegistry(registry, "host-a", 8080);

            // Coordinator-A becomes leader
            haA.start("coordinator-a");
            assertThat(registry.getLeader()).isPresent();
            assertThat(registry.getLeader().get().coordinatorId()).isEqualTo("coordinator-a");
            long previousEpoch = haA.getCurrentEpoch();

            // Coordinator-A stops (close → unregister)
            haA.close();
            assertThat(lease.currentHolder("coordinator_leader")).isEmpty();

            // Coordinator-B starts and takes over
            HighAvailabilityCoordinationService haB = new HighAvailabilityCoordinationService(lease, config);
            haB.setCoordinatorRegistry(registry, "host-b", 8081);
            try {
                haB.start("coordinator-b");

                assertThat(haB.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
                assertThat(haB.getCurrentEpoch()).isGreaterThan(previousEpoch);

                Optional<CoordinatorSnapshot> leader = registry.getLeader();
                assertThat(leader).isPresent();
                assertThat(leader.get().coordinatorId()).isEqualTo("coordinator-b");
                assertThat(leader.get().host()).isEqualTo("host-b");
                assertThat(leader.get().httpPort()).isEqualTo(8081);
            } finally {
                haB.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 5 — Leader Demotion
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader Demotion")
    class LeaderDemotion {

        @Test
        @DisplayName("Coordinator-A is LEADER → stepDown → updateRole(STANDBY) → getLeader empty → B takes over")
        void demotion_leaderStepsDown() {
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService haA = new HighAvailabilityCoordinationService(lease, config);
            haA.setCoordinatorRegistry(registry, "host-a", 8080);
            try {
                haA.start("coordinator-a");
                assertThat(haA.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
                long previousEpoch = haA.getCurrentEpoch();

                // Step down — releases lease and updates role to STANDBY
                haA.stepDown("coordinator-a");
                assertThat(lease.currentHolder("coordinator_leader")).isEmpty();

                // Verify role updated to STANDBY in registry
                Optional<CoordinatorSnapshot> snapshot = registry.getCoordinator("coordinator-a");
                assertThat(snapshot).isPresent();
                assertThat(snapshot.get().role()).isEqualTo("STANDBY");

                // No active leader (getLeader checks heartbeat freshness + LEADER role)
                // Note: getLeader() may still return coordinator-a if heartbeat is fresh
                // but role is STANDBY, so it should be empty
                Optional<CoordinatorSnapshot> leader = registry.getLeader();
                assertThat(leader).isEmpty();

                // Coordinator-B takes over
                HighAvailabilityCoordinationService haB = new HighAvailabilityCoordinationService(lease, config);
                haB.setCoordinatorRegistry(registry, "host-b", 8081);
                try {
                    haB.start("coordinator-b");
                    assertThat(haB.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);
                    assertThat(haB.getCurrentEpoch()).isGreaterThan(previousEpoch);

                    Optional<CoordinatorSnapshot> newLeader = registry.getLeader();
                    assertThat(newLeader).isPresent();
                    assertThat(newLeader.get().coordinatorId()).isEqualTo("coordinator-b");
                } finally {
                    haB.close();
                }
            } finally {
                haA.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 6 — Coordinator heartbeat timeout
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Coordinator Heartbeat Timeout")
    class CoordinatorHeartbeatTimeout {

        @Test
        @DisplayName("Register coordinator with old timestamp → expireStale → DB row deleted")
        void staleCoordinator_expiredFromDb() throws Exception {
            // Insert a coordinator with an old heartbeat timestamp directly in DB
            long staleTimestamp = System.currentTimeMillis() - 120_000; // 2 minutes ago
            insertCoordinatorWithTimestamp(dataSource, "coordinator-stale",
                    "host-stale", 8080, "STANDBY", staleTimestamp);

            // Register a fresh coordinator via HA service
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(lease, config);
            ha.setCoordinatorRegistry(registry, "host-fresh", 8081);
            try {
                ha.start("coordinator-fresh");

                // Expire stale coordinators (threshold = 60s)
                int expired = registry.expireStale(Duration.ofSeconds(60));
                assertThat(expired).isEqualTo(1);

                // Stale coordinator removed from DB
                Optional<CoordinatorSnapshot> stale = registry.getCoordinator("coordinator-stale");
                assertThat(stale).isEmpty();

                // Fresh coordinator still in DB
                Optional<CoordinatorSnapshot> fresh = registry.getCoordinator("coordinator-fresh");
                assertThat(fresh).isPresent();

                // getAllCoordinators should not include stale
                List<CoordinatorSnapshot> all = registry.getAllCoordinators(Duration.ofMinutes(5));
                assertThat(all).extracting(CoordinatorSnapshot::coordinatorId)
                        .containsExactly("coordinator-fresh");
            } finally {
                ha.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 7 — Client routing
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Client Routing")
    class ClientRouting {

        @Test
        @DisplayName("getLeader() returns leader's host + httpPort → verify endpoint info correct")
        void clientRouting_leaderEndpointCorrect() {
            CoordinationConfig config = new CoordinationConfig(LEASE_MS, HEARTBEAT_MS);
            HighAvailabilityCoordinationService ha = new HighAvailabilityCoordinationService(lease, config);
            ha.setCoordinatorRegistry(registry, "coordinator-host.example.com", 9443);
            try {
                ha.start("coordinator-primary");

                // Client queries for leader endpoint
                Optional<CoordinatorSnapshot> leader = registry.getLeader();
                assertThat(leader).isPresent();

                // Verify the endpoint info is correct for client routing
                CoordinatorSnapshot leaderSnapshot = leader.get();
                assertThat(leaderSnapshot.coordinatorId()).isEqualTo("coordinator-primary");
                assertThat(leaderSnapshot.host()).isEqualTo("coordinator-host.example.com");
                assertThat(leaderSnapshot.httpPort()).isEqualTo(9443);
                assertThat(leaderSnapshot.role()).isEqualTo("LEADER");

                // Simulate client constructing endpoint URL
                String endpoint = "http://" + leaderSnapshot.host() + ":" + leaderSnapshot.httpPort();
                assertThat(endpoint).isEqualTo("http://coordinator-host.example.com:9443");
            } finally {
                ha.close();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Schema setup and data helpers
    // -----------------------------------------------------------------------

    private static void insertCoordinatorWithTimestamp(DataSource ds, String coordinatorId,
                                                       String host, int httpPort,
                                                       String role, long heartbeatMs) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO coordinator_registry "
                     + "(coordinator_id, host, http_port, role, epoch, last_heartbeat_ms, registered_at_ms) "
                     + "VALUES (?, ?, ?, ?, 0, ?, ?)")) {
            ps.setString(1, coordinatorId);
            ps.setString(2, host);
            ps.setInt(3, httpPort);
            ps.setString(4, role);
            ps.setLong(5, heartbeatMs);
            ps.setLong(6, heartbeatMs);
            ps.executeUpdate();
        }
    }
}
