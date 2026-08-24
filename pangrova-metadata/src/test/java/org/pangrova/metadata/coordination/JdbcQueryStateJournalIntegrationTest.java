package org.pangrova.metadata.coordination;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.metadata.coordination.JdbcQueryStateJournal;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.StateJournalConfig;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration test for {@link JdbcQueryStateJournal} using TestContainers PostgreSQL.
 * Validates the full write → replay → purge cycle.
 */
@Testcontainers
class JdbcQueryStateJournalIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine").withDatabaseName("pangrova_test");

    private DataSource dataSource;
    private JdbcQueryStateJournal journal;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        initSchema(dataSource);
        cleanTables(dataSource);
        var config = new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24);
        journal = new JdbcQueryStateJournal(dataSource, config);
    }

    @AfterEach
    void tearDown() {
        if (journal != null) {
            journal.close();
        }
    }

    @Test
    void syncWriteAndReplayQueryEvents() {
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", null, null, "RUNNING", 2000L));

        List<QueryStateEvent> events = journal.replayActiveQueries();
        assertEquals(2, events.size());
        assertEquals("QUEUED", events.get(0).state());
        assertEquals("RUNNING", events.get(1).state());
    }

    @Test
    void syncWriteAndReplayTaskEvents() {
        // Need a non-terminal query for tasks to be replayed
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        journal.appendTaskEventSync(
                new TaskStateEvent("q-1", "t-1", "w-1", "RUNNING", 0, 1L, 1000L));
        journal.appendTaskEventSync(
                new TaskStateEvent("q-1", "t-2", "w-2", "RUNNING", 1, 1L, 1100L));

        List<TaskStateEvent> events = journal.replayActiveTasks();
        assertEquals(2, events.size());
        assertEquals("t-1", events.get(0).taskId());
        assertEquals("t-2", events.get(1).taskId());
    }

    @Test
    void terminalQueryExcludedFromReplay() {
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", null, null, "FINISHED", 2000L));

        List<QueryStateEvent> events = journal.replayActiveQueries();
        assertTrue(events.isEmpty(), "Finished query should not be replayed");
    }

    @Test
    void purgeRemovesAllEventsForQuery() {
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        journal.appendTaskEventSync(
                new TaskStateEvent("q-1", "t-1", "w-1", "RUNNING", 0, 1L, 1000L));

        journal.purgeQuery("q-1");

        assertTrue(journal.replayActiveQueries().isEmpty());
        assertTrue(journal.replayActiveTasks().isEmpty());
    }

    @Test
    void purgeDoesNotAffectOtherQueries() {
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        journal.appendQueryEventSync(
                new QueryStateEvent("q-2", "SELECT 2", "{}", "QUEUED", 1100L));

        journal.purgeQuery("q-1");

        List<QueryStateEvent> events = journal.replayActiveQueries();
        assertEquals(1, events.size());
        assertEquals("q-2", events.get(0).queryId());
    }

    @Test
    void asyncFlushBatchingBehavior() throws InterruptedException {
        // Need a non-terminal query for tasks to be replayed
        journal.appendQueryEventSync(
                new QueryStateEvent("q-async", "SELECT 1", "{}", "QUEUED", 1000L));

        // Append async events
        for (int i = 0; i < 5; i++) {
            journal.appendTaskEvent(
                    new TaskStateEvent("q-async", "t-" + i, "w-1", "RUNNING", 0, i, 1000L + i));
        }

        // Wait for flush (config interval is 50ms, give it 2s)
        await().atMost(2, SECONDS).until(() -> journal.replayActiveTasks().size() == 5);

        List<TaskStateEvent> events = journal.replayActiveTasks();
        assertEquals(5, events.size(), "All async events should be flushed");
    }

    @Test
    void closeFlushesRemainingEvents() throws InterruptedException {
        journal.appendQueryEventSync(
                new QueryStateEvent("q-close", "SELECT 1", "{}", "QUEUED", 1000L));

        // Append async events
        for (int i = 0; i < 3; i++) {
            journal.appendTaskEvent(
                    new TaskStateEvent("q-close", "t-" + i, "w-1", "RUNNING", 0, i, 1000L + i));
        }

        // Close immediately (should flush remaining)
        journal.close();

        // Create a new journal to verify data was persisted
        var config2 = new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24);
        try (var journal2 = new JdbcQueryStateJournal(dataSource, config2)) {
            List<TaskStateEvent> events = journal2.replayActiveTasks();
            assertEquals(3, events.size(), "Close should flush remaining async events");
        }
    }

    @Test
    void multipleQueriesMixedStates() {
        // q-1: active (QUEUED → RUNNING)
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", "SELECT 1", "{}", "QUEUED", 1000L));
        journal.appendQueryEventSync(
                new QueryStateEvent("q-1", null, null, "RUNNING", 2000L));

        // q-2: terminal (QUEUED → FAILED)
        journal.appendQueryEventSync(
                new QueryStateEvent("q-2", "SELECT 2", "{}", "QUEUED", 1100L));
        journal.appendQueryEventSync(
                new QueryStateEvent("q-2", null, null, "FAILED", 2100L));

        // q-3: active (QUEUED only)
        journal.appendQueryEventSync(
                new QueryStateEvent("q-3", "SELECT 3", "{}", "QUEUED", 1200L));

        List<QueryStateEvent> events = journal.replayActiveQueries();
        // Only q-1 and q-3 events should be replayed
        long distinctQueries = events.stream()
                .map(QueryStateEvent::queryId).distinct().count();
        assertEquals(2, distinctQueries, "Only active queries should be replayed");
        assertTrue(events.stream().noneMatch(e -> "q-2".equals(e.queryId())),
                "Terminal query q-2 should not be replayed");
    }

    private static void initSchema(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS query_state_journal (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        query_id VARCHAR(64) NOT NULL,
                        sql_text TEXT,
                        session_json TEXT,
                        state VARCHAR(32) NOT NULL,
                        timestamp_ms BIGINT NOT NULL,
                        coordinator_id VARCHAR(128),
                        epoch BIGINT NOT NULL DEFAULT 0
                    )""");
            stmt.execute(
                    "CREATE INDEX IF NOT EXISTS idx_qsj_query_state ON query_state_journal (query_id, state)");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS task_state_journal (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        query_id VARCHAR(64) NOT NULL,
                        task_id VARCHAR(64) NOT NULL,
                        worker_id VARCHAR(64),
                        state VARCHAR(32) NOT NULL,
                        stage_id INT NOT NULL,
                        report_sequence BIGINT NOT NULL DEFAULT 0,
                        timestamp_ms BIGINT NOT NULL,
                        coordinator_id VARCHAR(128),
                        epoch BIGINT NOT NULL DEFAULT 0
                    )""");
            stmt.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tsj_task_query ON task_state_journal (query_id, task_id)");
        }
    }

    private static void cleanTables(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM task_state_journal");
            stmt.execute("DELETE FROM query_state_journal");
        }
    }
}
