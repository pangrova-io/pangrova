package org.pangrova.testing.integration.e2e.coordination;

import org.pangrova.coordinator.coordination.LeaseBackedLeaderElection;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.query.core.recovery.JournalRecoveryService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.persistence.query.postgresql.state.JdbcQueryStateJournal;
import org.pangrova.types.value.TypedRow;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.query.api.journal.TaskStateEvent;
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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for coordinator failover with JDBC state journal.
 *
 * <p>Simulates 2 Coordinators using JDBC-backed leader election and
 * query state journal. Validates:
 * <ul>
 *   <li>Leader acquires leadership via distributed lease</li>
 *   <li>Standby takes over after leader release (simulated kill)</li>
 *   <li>In-flight queries are recovered from journal by new leader</li>
 * </ul>
 *
 * <p>Implements: R9.3
 */
@DisplayName("Failover E2E — 2 Coordinators with JDBC Journal")
class FailoverE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final long LEASE_MS = 5000;

    private DataSource journalDs;
    private JdbcQueryStateJournal journal;
    private FencedLeaderElectionTestDriver leaderElection;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void initFailoverInfra() throws Exception {
        journalDs = MetadataSchemaInitializer.createDataSource(
                mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
        MetadataSchemaInitializer.createStateStoreTables(journalDs);
        truncateStateTables(journalDs);

        StateJournalConfig config = new StateJournalConfig("JDBC", mysqlJdbcUrl(), 50, 20, 24, 50_000);
        journal = new JdbcQueryStateJournal(journalDs, config);

        DistributedLease lease = new TestLeaseNamespace().jdbc(journalDs);
        leaderElection = new FencedLeaderElectionTestDriver(
                new LeaseBackedLeaderElection(lease));
    }

    @AfterEach
    void cleanup() {
        if (leaderElection != null) {
            leaderElection.release("coordinator-1");
            leaderElection.release("coordinator-2");
        }
        if (journal != null) {
            journal.close();
        }
    }

    // -----------------------------------------------------------------------
    // Leader election and failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Leader Kill and Standby Takeover")
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
        @DisplayName("Standby takes over after leader kill (release)")
        void standbyTakesOverAfterLeaderKill() {
            leaderElection.tryAcquire("coordinator-1", LEASE_MS);

            // Simulate leader kill
            leaderElection.release("coordinator-1");

            boolean acquired = leaderElection.tryAcquire("coordinator-2", LEASE_MS);
            assertThat(acquired).isTrue();
            assertThat(leaderElection.currentLeader()).hasValue("coordinator-2");
        }
    }

    // -----------------------------------------------------------------------
    // Journal recovery after failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("In-Flight Query Recovery from Journal")
    class InFlightQueryRecovery {

        @Test
        @DisplayName("Active queries recovered by new leader from journal")
        void activeQueriesRecoveredFromJournal() {
            leaderElection.tryAcquire("coordinator-1", LEASE_MS);

            // Coordinator-1 journals in-flight queries
            String q1 = "failover-q1-" + System.nanoTime();
            String q2 = "failover-q2-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now, null), q1, "SELECT * FROM t1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 1, null), q1, null, null, "RUNNING"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 2, null), q1, "task-1", "worker-1", "RUNNING", 0, 1));

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 3, null), q2, "SELECT * FROM t2", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 4, null), q2, null, null, "SCHEDULING"));

            // Simulate leader kill
            leaderElection.release("coordinator-1");
            leaderElection.tryAcquire("coordinator-2", LEASE_MS);

            // New leader recovers from journal
            CoordinatorTaskTracker recoveredTracker = new CoordinatorTaskTracker();
            QueryManager recoveredManager = org.pangrova.testing.integration.e2e.RecoveryQueryManagerFixture.create(recoveredTracker);
            JournalRecoveryService recovery = new JournalRecoveryService(
                    journal, recoveredManager, recoveredTracker, null);
            int recovered = recovery.recover();

            assertThat(recovered).isEqualTo(2);
            Set<String> activeIds = recoveredManager.getActiveQueryIds();
            assertThat(activeIds).contains(q1, q2);
        }

        @Test
        @DisplayName("Task state recovered with correct worker assignment")
        void taskStateRecoveredWithWorkerAssignment() {
            leaderElection.tryAcquire("coordinator-1", LEASE_MS);

            String queryId = "failover-task-" + System.nanoTime();
            String taskId = "task-recover-1";
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now, null), queryId, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 1, null), queryId, null, null, "RUNNING"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 2, null), queryId, taskId, "worker-2", "RUNNING", 0, 1));

            // Failover
            leaderElection.release("coordinator-1");
            leaderElection.tryAcquire("coordinator-2", LEASE_MS);

            CoordinatorTaskTracker recoveredTracker = new CoordinatorTaskTracker();
            QueryManager recoveredManager = org.pangrova.testing.integration.e2e.RecoveryQueryManagerFixture.create(recoveredTracker);
            new JournalRecoveryService(journal, recoveredManager, recoveredTracker, null)
                    .recover();

            assertThat(recoveredTracker.getTaskIds(queryId)).contains(taskId);
            assertThat(recoveredTracker.getWorkerForTask(taskId))
                    .isEqualTo("worker-2");
        }

        @Test
        @DisplayName("Completed queries not recovered after failover")
        void completedQueriesNotRecovered() {
            leaderElection.tryAcquire("coordinator-1", LEASE_MS);

            String active = "failover-active-" + System.nanoTime();
            String finished = "failover-done-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now, null), active, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 1, null), active, null, null, "RUNNING"));

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 2, null), finished, "SELECT 2", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(now + 3, null), finished, null, null, "FINISHED"));

            leaderElection.release("coordinator-1");
            leaderElection.tryAcquire("coordinator-2", LEASE_MS);

            CoordinatorTaskTracker recoveredTracker = new CoordinatorTaskTracker();
            QueryManager recoveredManager = org.pangrova.testing.integration.e2e.RecoveryQueryManagerFixture.create(recoveredTracker);
            int recovered = new JournalRecoveryService(
                    journal, recoveredManager, recoveredTracker, null).recover();

            assertThat(recovered).isEqualTo(1);
            assertThat(recoveredManager.getActiveQueryIds()).contains(active);
            assertThat(recoveredManager.getActiveQueryIds()).doesNotContain(finished);
        }
    }

    // -----------------------------------------------------------------------
    // Real query after failover
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Query Execution After Failover")
    class QueryAfterFailover {

        @Test
        @DisplayName("Real query succeeds after leader failover")
        void realQuerySucceedsAfterFailover() {
            leaderElection.tryAcquire("coordinator-1", LEASE_MS);
            leaderElection.release("coordinator-1");
            leaderElection.tryAcquire("coordinator-2", LEASE_MS);

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

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static void truncateStateTables(DataSource ds) throws Exception {
        try (var conn = ds.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE query_state_journal");
            stmt.execute("TRUNCATE TABLE task_state_journal");
        }
    }
}
