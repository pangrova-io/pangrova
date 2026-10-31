package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.coordinator.coordination.LeaseBackedLeaderElection;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.coordination.api.DistributedLease;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.TestLeaseNamespace;
import org.pangrova.testing.integration.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for HA leader election and failover via unified DistributedLease.
 *
 * <p>Validates the full leader election lifecycle through real PostgreSQL:
 * acquire → renew → release → re-acquire by standby.
 * Uses TestContainers PostgreSQL shared with the metadata store.</p>
 */
@DisplayName("HA Leader Election Failover E2E")
class LeaderElectionFailoverE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final long LEASE_MS = 5000;

    private DataSource leaderDs;
    private FencedLeaderElectionTestDriver backend;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void initLeaderElection() throws Exception {
        leaderDs = MetadataSchemaInitializer.createDataSource(
                mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
        MetadataSchemaInitializer.createSchema(leaderDs);
        DistributedLease lease = new TestLeaseNamespace().jdbc(leaderDs);
        backend = new FencedLeaderElectionTestDriver(
                new LeaseBackedLeaderElection(lease));
    }

    @AfterEach
    void cleanupLeaderElection() {
        if (backend != null) {
            backend.release("coordinator-1");
            backend.release("coordinator-2");
        }
    }

    // -----------------------------------------------------------------------
    // Leader acquisition
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader Acquisition")
    class LeaderAcquisition {

        @Test
        @DisplayName("First candidate acquires leadership successfully")
        void firstCandidateAcquiresLeadership() {
            boolean acquired = backend.tryAcquire("coordinator-1", LEASE_MS);

            assertThat(acquired).isTrue();
            assertThat(backend.currentLeader()).hasValue("coordinator-1");
        }

        @Test
        @DisplayName("Second candidate cannot acquire while first holds lease")
        void secondCandidateBlockedByActiveLease() {
            backend.tryAcquire("coordinator-1", LEASE_MS);

            boolean acquired = backend.tryAcquire("coordinator-2", LEASE_MS);

            assertThat(acquired).isFalse();
            assertThat(backend.currentLeader()).hasValue("coordinator-1");
        }
    }

    // -----------------------------------------------------------------------
    // Lease renewal
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Lease Renewal")
    class LeaseRenewal {

        @Test
        @DisplayName("Leader can renew its own lease")
        void leaderRenewsOwnLease() {
            backend.tryAcquire("coordinator-1", LEASE_MS);

            boolean renewed = backend.tryRenew("coordinator-1", LEASE_MS);

            assertThat(renewed).isTrue();
            assertThat(backend.currentLeader()).hasValue("coordinator-1");
        }

        @Test
        @DisplayName("Non-leader cannot renew lease")
        void nonLeaderCannotRenew() {
            backend.tryAcquire("coordinator-1", LEASE_MS);

            boolean renewed = backend.tryRenew("coordinator-2", LEASE_MS);

            assertThat(renewed).isFalse();
        }
    }

    // -----------------------------------------------------------------------
    // Failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader Failover")
    class LeaderFailover {

        @Test
        @DisplayName("Standby acquires leadership after leader releases")
        void standbyAcquiresAfterRelease() {
            backend.tryAcquire("coordinator-1", LEASE_MS);
            backend.release("coordinator-1");

            boolean acquired = backend.tryAcquire("coordinator-2", LEASE_MS);

            assertThat(acquired).isTrue();
            assertThat(backend.currentLeader()).hasValue("coordinator-2");
        }

        @Test
        @DisplayName("No leader after release and before re-acquire")
        void noLeaderAfterRelease() {
            backend.tryAcquire("coordinator-1", LEASE_MS);
            backend.release("coordinator-1");

            Optional<String> leader = backend.currentLeader();

            assertThat(leader).isEmpty();
        }

        @Test
        @DisplayName("Standby acquires after lease expiry")
        void standbyAcquiresAfterLeaseExpiry() {
            // F-E2E-04 fix: poll tryAcquire instead of fixed Thread.sleep.
            backend.tryAcquire("coordinator-1", 1000);

            org.awaitility.Awaitility.await("standby-acquires-after-expiry")
                    .atMost(java.time.Duration.ofSeconds(5))
                    .pollInterval(java.time.Duration.ofMillis(200))
                    .pollDelay(java.time.Duration.ofMillis(1000))
                    .until(() -> backend.tryAcquire("coordinator-2", LEASE_MS));

            assertThat(backend.currentLeader()).hasValue("coordinator-2");
        }
    }

    // -----------------------------------------------------------------------
    // Query execution after failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Query After Failover")
    class QueryAfterFailover {

        @Test
        @DisplayName("Query executes successfully after leader failover")
        void querySucceedsAfterFailover() {
            backend.tryAcquire("coordinator-1", LEASE_MS);
            backend.release("coordinator-1");
            backend.tryAcquire("coordinator-2", LEASE_MS);

            String tableName = "ha_test_" + Long.toHexString(System.nanoTime());
            dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                    "id BIGINT, value BIGINT");
            for (int i = 1; i <= 5; i++) {
                sqlRunner.execute(String.format(
                        "INSERT INTO %s.%s.%s (id, value) VALUES (%d, %d)",
                        CATALOG, SCHEMA, tableName, i, i * 100));
            }

            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT SUM(value) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, tableName));

            assertThat(rows).hasSize(1);
            long total = rows.get(0).getLong("total");
            assertThat(total).isEqualTo(1500L);
        }
    }

    // -----------------------------------------------------------------------
    // Schema setup helpers
    // -----------------------------------------------------------------------

}
