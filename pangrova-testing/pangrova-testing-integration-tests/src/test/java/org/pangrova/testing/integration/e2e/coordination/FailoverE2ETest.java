package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.coordinator.coordination.LeaseBackedLeaderElection;
import org.pangrova.types.value.TypedRow;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.TestLeaseNamespace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Fenced leadership transfer with PostgreSQL")
class FailoverE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final long LEASE_MS = 5000;

    private FencedLeaderElectionTestDriver leaderElection;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void initFailoverInfra() {
        DistributedLease lease = new TestLeaseNamespace().jdbc(stateStoreDataSource());
        leaderElection = new FencedLeaderElectionTestDriver(
                new LeaseBackedLeaderElection(lease));
    }

    @AfterEach
    void cleanup() {
        if (leaderElection != null) {
            leaderElection.release("coordinator-1");
            leaderElection.release("coordinator-2");
        }
    }

    // -----------------------------------------------------------------------
    // Leader election and failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader release and standby takeover")
    class LeaderKillAndRecovery {

        @Test
        @DisplayName("Coordinator-1 acquires leadership, coordinator-2 blocked")
        void leaderAcquiredStandbyBlocked() {
            boolean c1 = leaderElection.tryAcquire("coordinator-1", LEASE_MS);
            boolean c2 = leaderElection.tryAcquire("coordinator-2", LEASE_MS);

            assertThat(c1).isTrue();
            assertThat(c2).isFalse();
            assertThat(leaderElection.currentLeader()).hasValue("coordinator-1");
        }

        @Test
        @DisplayName("Standby takes over after leader releases its lease")
        void standbyTakesOverAfterLeaderRelease() {
            leaderElection.tryAcquire("coordinator-1", LEASE_MS);

            leaderElection.release("coordinator-1");

            boolean acquired = leaderElection.tryAcquire("coordinator-2", LEASE_MS);
            assertThat(acquired).isTrue();
            assertThat(leaderElection.currentLeader()).hasValue("coordinator-2");
        }
    }

    // -----------------------------------------------------------------------
    // Real query after failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Query Execution After Failover")
    class QueryAfterFailover {

        @Test
        @DisplayName("SQL remains available after fenced lease transfer")
        void sqlRemainsAvailableAfterLeaseTransfer() {
            assertThat(leaderElection.tryAcquire("coordinator-1", LEASE_MS)).isTrue();
            leaderElection.release("coordinator-1");
            assertThat(leaderElection.tryAcquire("coordinator-2", LEASE_MS)).isTrue();

            String tableName = "failover_data_" + Long.toHexString(System.nanoTime());
            dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                    "id BIGINT, value BIGINT");
            for (int i = 1; i <= 10; i++) {
                sqlRunner.execute(String.format(
                        "INSERT INTO %s.%s.%s (id, value) VALUES (%d, %d)",
                        CATALOG, SCHEMA, tableName, i, i * 100));
            }

            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT SUM(value) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, tableName));

            assertThat(rows).hasSize(1);
            long total = ((Number) rows.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(5500L);
        }
    }
}
