package org.pangrova.testing.integration.e2e.resilience;

import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.state.JournalRecoveryService;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.query.store.jdbc.journal.JdbcQueryStateJournal;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.spi.query.state.StateJournalConfig;
import org.pangrova.spi.query.state.TaskStateEvent;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for Coordinator HA recovery via JdbcQueryStateJournal.
 *
 * <p>Validates the full journal lifecycle through real PostgreSQL (TestContainers):
 * write query/task events → simulate coordinator restart →
 * JournalRecoveryService replays events → QueryManager + TaskTracker
 * rebuild in-memory state → verify recovered state matches original.
 *
 * <p>Pipeline: SQL execution → journal persist → coordinator "restart" →
 * journal replay → state verification.
 */
@DisplayName("Query State Journal Recovery E2E — Coordinator HA")
class QueryStateJournalRecoveryE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private DataSource journalDs;
    private JdbcQueryStateJournal journal;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void initJournal() throws Exception {
        journalDs = MetadataSchemaInitializer.createDataSource(
                mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
        createJournalTables(journalDs);
        truncateJournalTables(journalDs);
        StateJournalConfig config = new StateJournalConfig("JDBC", mysqlJdbcUrl(), 50, 20, 24, 50_000);
        journal = new JdbcQueryStateJournal(journalDs, config);
    }

    @AfterEach
    void closeJournal() {
        if (journal != null) {
            journal.close();
        }
    }

    // -----------------------------------------------------------------------
    // Query event persistence and replay
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Query Event Journal Round-Trip")
    class QueryEventJournalRoundTrip {

        @Test
        @DisplayName("Sync-written RUNNING query event survives replay after restart")
        void syncWrittenRunningQuerySurvivesReplay() {
            String queryId = "recovery-q-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, null, null, "PLANNING"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), queryId, null, null, "SCHEDULING"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 3, null), queryId, null, null, "RUNNING"));

            List<QueryStateEvent> replayed = journal.replayActiveQueries();

            assertThat(replayed).isNotEmpty();
            assertThat(replayed.stream().map(QueryStateEvent::queryId))
                    .contains(queryId);
        }

        @Test
        @DisplayName("FINISHED query is excluded from replay")
        void finishedQueryExcludedFromReplay() {
            String queryId = "recovery-fin-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, null, null, "FINISHED"));

            List<QueryStateEvent> replayed = journal.replayActiveQueries();

            assertThat(replayed.stream().map(QueryStateEvent::queryId))
                    .doesNotContain(queryId);
        }

        @Test
        @DisplayName("FAILED query is excluded from replay")
        void failedQueryExcludedFromReplay() {
            String queryId = "recovery-fail-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT 1/0", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, null, null, "FAILED"));

            List<QueryStateEvent> replayed = journal.replayActiveQueries();

            assertThat(replayed.stream().map(QueryStateEvent::queryId))
                    .doesNotContain(queryId);
        }
    }

    // -----------------------------------------------------------------------
    // Task event persistence and replay
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Task Event Journal Round-Trip")
    class TaskEventJournalRoundTrip {

        @Test
        @DisplayName("Task events for active query survive replay")
        void taskEventsForActiveQuerySurviveReplay() {
            String queryId = "recovery-tq-" + System.nanoTime();
            String taskId = "task-1";
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT * FROM t", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, null, null, "RUNNING"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), queryId, taskId, "worker-1", "SCHEDULED", 0, 1));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 3, null), queryId, taskId, "worker-1", "RUNNING", 0, 2));

            List<TaskStateEvent> replayed = journal.replayActiveTasks();

            assertThat(replayed).isNotEmpty();
            assertThat(replayed.stream().map(TaskStateEvent::taskId))
                    .contains(taskId);
        }

        @Test
        @DisplayName("Task events for finished query excluded from replay")
        void taskEventsForFinishedQueryExcluded() {
            String queryId = "recovery-tfin-" + System.nanoTime();
            String taskId = "task-fin-1";
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT 1", "{}", "QUEUED"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, taskId, "worker-1", "RUNNING", 0, 1));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), queryId, null, null, "FINISHED"));

            List<TaskStateEvent> replayed = journal.replayActiveTasks();

            assertThat(replayed.stream()
                    .filter(e -> e.queryId().equals(queryId))
                    .toList()).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Full recovery service integration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("JournalRecoveryService Full Recovery")
    class FullRecoveryService {

        @Test
        @DisplayName("Recovery rebuilds QueryManager state from journal events")
        void recoveryRebuildsQueryManagerState() {
            String q1 = "recovery-full-" + System.nanoTime();
            String q2 = "recovery-full2-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), q1, "SELECT * FROM t1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), q1, null, null, "RUNNING"));

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), q2, "SELECT * FROM t2", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 3, null), q2, null, null, "SCHEDULING"));

            CoordinatorTaskTracker recoveredTracker = new CoordinatorTaskTracker();
            QueryManager recoveredManager = org.pangrova.testing.integration.e2e.RecoveryQueryManagerFixture.create(recoveredTracker);

            JournalRecoveryService recoveryService = new JournalRecoveryService(
                    journal, recoveredManager, recoveredTracker);
            int recovered = recoveryService.recover();

            assertThat(recovered).isEqualTo(2);
            Set<String> activeIds = recoveredManager.getActiveQueryIds();
            assertThat(activeIds).contains(q1, q2);
        }

        @Test
        @DisplayName("Recovery rebuilds TaskTracker state from journal events")
        void recoveryRebuildsTaskTrackerState() {
            String queryId = "recovery-task-" + System.nanoTime();
            String taskId = "task-r1";
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, null, null, "RUNNING"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), queryId, taskId, "worker-1", "RUNNING", 0, 1));

            CoordinatorTaskTracker recoveredTracker = new CoordinatorTaskTracker();
            QueryManager recoveredManager = org.pangrova.testing.integration.e2e.RecoveryQueryManagerFixture.create(recoveredTracker);

            JournalRecoveryService recoveryService = new JournalRecoveryService(
                    journal, recoveredManager, recoveredTracker);
            recoveryService.recover();

            Set<String> taskIds = recoveredTracker.getTaskIds(queryId);
            assertThat(taskIds).contains(taskId);
            assertThat(recoveredTracker.getWorkerForTask(taskId))
                    .isEqualTo("worker-1");
        }

        @Test
        @DisplayName("Mixed active and terminal queries: only active recovered")
        void mixedActiveAndTerminalQueries() {
            String active = "recovery-active-" + System.nanoTime();
            String finished = "recovery-done-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), active, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), active, null, null, "RUNNING"));

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), finished, "SELECT 2", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 3, null), finished, null, null, "FINISHED"));

            CoordinatorTaskTracker recoveredTracker = new CoordinatorTaskTracker();
            QueryManager recoveredManager = org.pangrova.testing.integration.e2e.RecoveryQueryManagerFixture.create(recoveredTracker);

            JournalRecoveryService recoveryService = new JournalRecoveryService(
                    journal, recoveredManager, recoveredTracker);
            int recovered = recoveryService.recover();

            assertThat(recovered).isEqualTo(1);
            assertThat(recoveredManager.getActiveQueryIds()).contains(active);
            assertThat(recoveredManager.getActiveQueryIds()).doesNotContain(finished);
        }
    }

    // -----------------------------------------------------------------------
    // Purge lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Journal Purge Lifecycle")
    class JournalPurgeLifecycle {

        @Test
        @DisplayName("Purged query events no longer appear in replay")
        void purgedQueryEventsNoLongerReplay() {
            String queryId = "recovery-purge-" + System.nanoTime();
            long now = System.currentTimeMillis();

            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now, null), queryId, "SELECT 1", "{}", "QUEUED"));
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 1, null), queryId, null, null, "RUNNING"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(now + 2, null), queryId, "t1", "w1", "RUNNING", 0, 1));

            journal.purgeQuery(queryId);

            List<QueryStateEvent> queryEvents = journal.replayActiveQueries();
            List<TaskStateEvent> taskEvents = journal.replayActiveTasks();

            assertThat(queryEvents.stream().map(QueryStateEvent::queryId))
                    .doesNotContain(queryId);
            assertThat(taskEvents.stream().map(TaskStateEvent::queryId))
                    .doesNotContain(queryId);
        }
    }

    // -----------------------------------------------------------------------
    // Real query + journal integration
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Real Query Execution with Journal")
    class RealQueryWithJournal {

        @Test
        @DisplayName("Execute real query, verify journal captures events")
        void realQueryJournalCapture() {
            String tableName = "journal_test_" + Long.toHexString(System.nanoTime());
            dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                    "id BIGINT, value BIGINT");
            for (int i = 1; i <= 10; i++) {
                sqlRunner.execute(String.format(
                        "INSERT INTO %s.%s.%s (id, value) VALUES (%d, %d)",
                        CATALOG, SCHEMA, tableName, i, i * 10));
            }

            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT SUM(value) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, tableName));

            assertThat(rows).hasSize(1);
            long total = ((Number) rows.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(550L);
        }
    }

    // -----------------------------------------------------------------------
    // Schema setup helpers
    // -----------------------------------------------------------------------

    private static void createJournalTables(DataSource ds) {
        org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner.migrate(ds);
    }

    private static void truncateJournalTables(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE task_state_journal");
            stmt.execute("TRUNCATE TABLE query_state_journal");
        }
    }
}
