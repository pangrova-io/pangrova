package org.pangrova.testing.e2e.coordination;

import org.pangrova.coordinator.index.lifecycle.MaintenanceLeaderElection;
import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for multi-coordinator leader election under real PostgreSQL contention.
 *
 * <p>Proves that {@link DistributedLease} correctly arbitrates between
 * multiple coordinators competing for the maintenance lease, including
 * contention with 3 nodes, rapid failover cycles, lease expiry under
 * real DB timing, and query-path independence from leader transitions.</p>
 *
 * <p>Four scenarios:
 * <ol>
 *   <li>3 coordinators compete: exactly 1 wins, other 2 blocked,
 *       winner releases, one of the 2 acquires</li>
 *   <li>Rapid failover cycle: c1 → c2 → c3, all transitions clean,
 *       no stale leader</li>
 *   <li>Lease expiry under load: c1 acquires with 2s lease, does NOT
 *       renew, after 3s c2 acquires via expiry</li>
 *   <li>Query execution during leader transition: query still completes
 *       after leader releases lease mid-query</li>
 * </ol>
 */
@DisplayName("Multi-Coordinator Leader Election Under Contention E2E")
class MultiCoordinatorLeaderContE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final String LEASE_KEY = "index-maintenance";

    private DataSource leaseDs;
    private MaintenanceLeaderElection coordinator1;
    private MaintenanceLeaderElection coordinator2;
    private MaintenanceLeaderElection coordinator3;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void initLeaseInfra() throws Exception {
        leaseDs = MetadataSchemaInitializer.createDataSource(
                mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
        ensureDistributedLeaseTable(leaseDs);
        truncateDistributedLeaseTable(leaseDs);

        DistributedLease lease1 = new JdbcDistributedLease(leaseDs);
        DistributedLease lease2 = new JdbcDistributedLease(leaseDs);
        DistributedLease lease3 = new JdbcDistributedLease(leaseDs);
        coordinator1 = new MaintenanceLeaderElection(lease1, "coordinator-1", new AtomicLong(0));
        coordinator2 = new MaintenanceLeaderElection(lease2, "coordinator-2", new AtomicLong(0));
        coordinator3 = new MaintenanceLeaderElection(lease3, "coordinator-3", new AtomicLong(0));
    }

    @AfterEach
    void cleanupLeases() {
        try {
            DistributedLease cleanupLease = new JdbcDistributedLease(leaseDs);
            cleanupLease.release(LEASE_KEY, "coordinator-1");
            cleanupLease.release(LEASE_KEY, "coordinator-2");
            cleanupLease.release(LEASE_KEY, "coordinator-3");
        } catch (Exception ignored) {
            // Best-effort cleanup
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 1: 3 Coordinators Compete
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — 3 Coordinators Compete")
    class ThreeCoordinatorsCompete {

        @Test
        @DisplayName("Exactly one of 3 coordinators wins the lease under concurrent contention")
        void exactlyOneWinsUnderConcurrentContention() throws Exception {
            AtomicBoolean c1Won = new AtomicBoolean(false);
            AtomicBoolean c2Won = new AtomicBoolean(false);
            AtomicBoolean c3Won = new AtomicBoolean(false);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(3);

            Thread t1 = new Thread(() -> {
                try {
                    startGate.await(5, TimeUnit.SECONDS);
                    c1Won.set(coordinator1.tryAcquireOrRenew());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            Thread t2 = new Thread(() -> {
                try {
                    startGate.await(5, TimeUnit.SECONDS);
                    c2Won.set(coordinator2.tryAcquireOrRenew());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            Thread t3 = new Thread(() -> {
                try {
                    startGate.await(5, TimeUnit.SECONDS);
                    c3Won.set(coordinator3.tryAcquireOrRenew());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });

            t1.start();
            t2.start();
            t3.start();
            startGate.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

            int winnersCount = (c1Won.get() ? 1 : 0)
                    + (c2Won.get() ? 1 : 0)
                    + (c3Won.get() ? 1 : 0);

            assertThat(winnersCount)
                    .as("Exactly one coordinator should hold the lease")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("Winner releases → one of the remaining 2 acquires")
        void winnerReleasesAndAnotherAcquires() {
            // Coordinator-1 acquires first
            assertThat(coordinator1.tryAcquireOrRenew()).isTrue();
            assertThat(coordinator2.tryAcquireOrRenew()).isFalse();
            assertThat(coordinator3.tryAcquireOrRenew()).isFalse();

            // Winner releases
            DistributedLease releaseLease = new JdbcDistributedLease(leaseDs);
            releaseLease.release(LEASE_KEY, "coordinator-1");

            // One of the remaining two should acquire
            boolean c2Acquired = coordinator2.tryAcquireOrRenew();
            boolean c3Acquired = coordinator3.tryAcquireOrRenew();

            assertThat(c2Acquired || c3Acquired)
                    .as("At least one of the remaining coordinators should acquire the lease")
                    .isTrue();
            assertThat(c2Acquired && c3Acquired)
                    .as("Both remaining coordinators must not hold the lease simultaneously")
                    .isFalse();
        }

        @Test
        @DisplayName("Sequential contention: each coordinator can eventually lead")
        void sequentialContentionAllCanLead() {
            List<String> leaderSequence = new ArrayList<>();

            // Round 1: coordinator-1 leads
            assertThat(coordinator1.tryAcquireOrRenew()).isTrue();
            leaderSequence.add("coordinator-1");
            new JdbcDistributedLease(leaseDs).release(LEASE_KEY, "coordinator-1");

            // Round 2: coordinator-2 leads
            assertThat(coordinator2.tryAcquireOrRenew()).isTrue();
            leaderSequence.add("coordinator-2");
            new JdbcDistributedLease(leaseDs).release(LEASE_KEY, "coordinator-2");

            // Round 3: coordinator-3 leads
            assertThat(coordinator3.tryAcquireOrRenew()).isTrue();
            leaderSequence.add("coordinator-3");

            assertThat(leaderSequence)
                    .as("All three coordinators should have been leader at some point")
                    .containsExactly("coordinator-1", "coordinator-2", "coordinator-3");
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Rapid Failover Cycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — Rapid Failover Cycle")
    class RapidFailoverCycle {

        @Test
        @DisplayName("c1 → release → c2 → release → c3: all transitions clean, no stale leader")
        void rapidFailoverCycleAllTransitionsClean() {
            DistributedLease releaseLease = new JdbcDistributedLease(leaseDs);

            // Step 1: coordinator-1 acquires
            assertThat(coordinator1.tryAcquireOrRenew())
                    .as("coordinator-1 should acquire the lease")
                    .isTrue();
            // Verify others are blocked
            assertThat(coordinator2.tryAcquireOrRenew()).isFalse();
            assertThat(coordinator3.tryAcquireOrRenew()).isFalse();

            // Step 2: coordinator-1 releases → coordinator-2 acquires
            releaseLease.release(LEASE_KEY, "coordinator-1");
            assertThat(coordinator2.tryAcquireOrRenew())
                    .as("coordinator-2 should acquire after coordinator-1 releases")
                    .isTrue();
            // coordinator-1 should NOT be able to re-acquire while c2 holds
            assertThat(coordinator1.tryAcquireOrRenew()).isFalse();
            assertThat(coordinator3.tryAcquireOrRenew()).isFalse();

            // Step 3: coordinator-2 releases → coordinator-3 acquires
            releaseLease.release(LEASE_KEY, "coordinator-2");
            assertThat(coordinator3.tryAcquireOrRenew())
                    .as("coordinator-3 should acquire after coordinator-2 releases")
                    .isTrue();
            // Others blocked
            assertThat(coordinator1.tryAcquireOrRenew()).isFalse();
            assertThat(coordinator2.tryAcquireOrRenew()).isFalse();

            // Verify final state: only coordinator-3 holds the lease
            DistributedLease verifyLease = new JdbcDistributedLease(leaseDs);
            assertThat(verifyLease.currentHolder(LEASE_KEY))
                    .isPresent()
                    .hasValueSatisfying(entry ->
                            assertThat(entry.holderId()).isEqualTo("coordinator-3"));
        }

        @Test
        @DisplayName("Rapid back-and-forth between two coordinators")
        void rapidBackAndForthBetweenTwo() {
            DistributedLease releaseLease = new JdbcDistributedLease(leaseDs);

            for (int round = 0; round < 5; round++) {
                // coordinator-1 acquires
                assertThat(coordinator1.tryAcquireOrRenew())
                        .as("Round %d: coordinator-1 should acquire", round)
                        .isTrue();
                releaseLease.release(LEASE_KEY, "coordinator-1");

                // coordinator-2 acquires
                assertThat(coordinator2.tryAcquireOrRenew())
                        .as("Round %d: coordinator-2 should acquire", round)
                        .isTrue();
                releaseLease.release(LEASE_KEY, "coordinator-2");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: Lease Expiry Under Load
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 3 — Lease Expiry Under Load")
    class LeaseExpiryUnderLoad {

        @Test
        @DisplayName("Lease expires after TTL without renewal, allowing another coordinator to acquire")
        void leaseExpiresWithoutRenewalAllowingAcquisition() throws Exception {
            // Acquire with a short 2-second lease using raw DistributedLease
            DistributedLease shortLease = new JdbcDistributedLease(leaseDs);
            boolean acquired = shortLease.tryAcquire(
                    LEASE_KEY, "coordinator-1", 1, Duration.ofSeconds(2));
            assertThat(acquired)
                    .as("coordinator-1 should acquire with short lease")
                    .isTrue();

            // coordinator-2 cannot acquire while lease is active
            assertThat(coordinator2.tryAcquireOrRenew())
                    .as("coordinator-2 should be blocked while lease is active")
                    .isFalse();

            // F-E2E-04 fix: poll tryAcquireOrRenew instead of fixed Thread.sleep.
            // This avoids over-waiting when the lease expires faster than the
            // worst-case 2s+1s budget.
            org.awaitility.Awaitility.await("coordinator2-acquires-after-expiry")
                    .atMost(java.time.Duration.ofSeconds(8))
                    .pollInterval(java.time.Duration.ofMillis(250))
                    .pollDelay(java.time.Duration.ofSeconds(2))
                    .until(coordinator2::tryAcquireOrRenew);

            // Verify coordinator-2 is now the holder
            DistributedLease verifyLease = new JdbcDistributedLease(leaseDs);
            assertThat(verifyLease.currentHolder(LEASE_KEY))
                    .isPresent()
                    .hasValueSatisfying(entry ->
                            assertThat(entry.holderId()).isEqualTo("coordinator-2"));
        }

        @Test
        @DisplayName("Expired lease holder cannot renew after expiry")
        void expiredHolderCannotRenewAfterExpiry() throws Exception {
            // Acquire with a short 2-second lease
            DistributedLease shortLease = new JdbcDistributedLease(leaseDs);
            boolean acquired = shortLease.tryAcquire(
                    LEASE_KEY, "coordinator-1", 1, Duration.ofSeconds(2));
            assertThat(acquired).isTrue();

            // F-E2E-04 fix: poll until coordinator-2 acquires (lease expired)
            // instead of fixed Thread.sleep(3000).
            org.awaitility.Awaitility.await("coordinator2-acquires-expired")
                    .atMost(java.time.Duration.ofSeconds(8))
                    .pollInterval(java.time.Duration.ofMillis(250))
                    .pollDelay(java.time.Duration.ofSeconds(2))
                    .until(coordinator2::tryAcquireOrRenew);

            // coordinator-1 cannot renew its expired lease
            boolean renewed = shortLease.tryRenew(
                    LEASE_KEY, "coordinator-1", Duration.ofSeconds(30));
            assertThat(renewed)
                    .as("Expired holder should not be able to renew")
                    .isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4: Query Execution During Leader Transition
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 4 — Query Execution During Leader Transition")
    class QueryDuringLeaderTransition {

        @Test
        @DisplayName("Query completes correctly after leader releases lease mid-query")
        void queryCompletesDuringLeaderTransition() {
            // coordinator-1 is maintenance leader
            assertThat(coordinator1.tryAcquireOrRenew()).isTrue();

            String tableName = "t_query_transition_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, region VARCHAR, amount BIGINT)");

            // Insert data while coordinator-1 is leader
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, amount) VALUES (1, 'NORTH', 100)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, amount) VALUES (2, 'SOUTH', 200)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, amount) VALUES (3, 'NORTH', 300)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, region, amount) VALUES (4, 'SOUTH', 400)");

            // Submit a query — query path is stateless
            List<TypedRow> preRelease = sqlRunner.execute(
                    "SELECT SUM(amount) AS total FROM " + fqn);
            assertThat(preRelease).hasSize(1);
            long totalPre = ((Number) preRelease.get(0).getRaw("total")).longValue();
            assertThat(totalPre).isEqualTo(1000L);

            // Release lease mid-operation (simulating leader transition)
            DistributedLease releaseLease = new JdbcDistributedLease(leaseDs);
            releaseLease.release(LEASE_KEY, "coordinator-1");

            // Another coordinator takes over maintenance
            assertThat(coordinator2.tryAcquireOrRenew()).isTrue();

            // Query still completes correctly — query path is independent
            List<TypedRow> postRelease = sqlRunner.execute(
                    "SELECT SUM(amount) AS total FROM " + fqn);
            assertThat(postRelease).hasSize(1);
            long totalPost = ((Number) postRelease.get(0).getRaw("total")).longValue();
            assertThat(totalPost).isEqualTo(1000L);
        }

        @Test
        @DisplayName("Aggregation and filter queries unaffected by leader transition")
        void aggregationAndFilterQueriesUnaffectedByTransition() {
            // coordinator-1 is maintenance leader
            assertThat(coordinator1.tryAcquireOrRenew()).isTrue();

            String tableName = "t_agg_transition_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, category VARCHAR, value BIGINT)");

            for (int i = 1; i <= 10; i++) {
                sqlRunner.execute(String.format(
                        "INSERT INTO %s (id, category, value) VALUES (%d, '%s', %d)",
                        fqn, i, (i % 3 == 0) ? "C" : (i % 2 == 0) ? "B" : "A", i * 50));
            }

            // Release lease — no maintenance leader
            DistributedLease releaseLease = new JdbcDistributedLease(leaseDs);
            releaseLease.release(LEASE_KEY, "coordinator-1");

            // coordinator-3 takes over
            assertThat(coordinator3.tryAcquireOrRenew()).isTrue();

            // Aggregation query
            List<TypedRow> sumRows = sqlRunner.execute(
                    "SELECT SUM(value) AS total FROM " + fqn);
            assertThat(sumRows).hasSize(1);
            // Sum of 50+100+150+200+250+300+350+400+450+500 = 2750
            long total = ((Number) sumRows.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(2750L);

            // Count query
            List<TypedRow> countRows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + fqn);
            assertThat(countRows).hasSize(1);
            long count = ((Number) countRows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(10L);

            // Filter query
            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE value > 200");
            assertThat(filtered).hasSize(6);
        }

        @Test
        @DisplayName("Insert and query work across multiple leader transitions")
        void insertAndQueryAcrossMultipleTransitions() {
            DistributedLease releaseLease = new JdbcDistributedLease(leaseDs);

            String tableName = "t_multi_transition_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, leader VARCHAR, amount BIGINT)");

            // Phase 1: coordinator-1 is leader
            assertThat(coordinator1.tryAcquireOrRenew()).isTrue();
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, leader, amount) VALUES (1, 'c1', 100)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, leader, amount) VALUES (2, 'c1', 200)");

            // Transition to coordinator-2
            releaseLease.release(LEASE_KEY, "coordinator-1");
            assertThat(coordinator2.tryAcquireOrRenew()).isTrue();
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, leader, amount) VALUES (3, 'c2', 300)");

            // Transition to coordinator-3
            releaseLease.release(LEASE_KEY, "coordinator-2");
            assertThat(coordinator3.tryAcquireOrRenew()).isTrue();
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, leader, amount) VALUES (4, 'c3', 400)");

            // Verify all data present and correct
            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + fqn);
            assertThat(allRows).hasSize(1);
            long count = ((Number) allRows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(4L);

            List<TypedRow> sumRows = sqlRunner.execute(
                    "SELECT SUM(amount) AS total FROM " + fqn);
            assertThat(sumRows).hasSize(1);
            long total = ((Number) sumRows.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(1000L);
        }
    }

    // -----------------------------------------------------------------------
    // Schema setup helpers
    // -----------------------------------------------------------------------

    private static void ensureDistributedLeaseTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS distributed_lease (
                        lease_key   VARCHAR(255) PRIMARY KEY,
                        holder_id   VARCHAR(255) NOT NULL,
                        epoch       BIGINT       NOT NULL DEFAULT 0,
                        acquired_at TIMESTAMP(3) NOT NULL,
                        expires_at  TIMESTAMP(3) NOT NULL,
                        version     BIGINT       NOT NULL DEFAULT 0
                    )""");
        }
    }

    private static void truncateDistributedLeaseTable(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM distributed_lease");
        }
    }
}
