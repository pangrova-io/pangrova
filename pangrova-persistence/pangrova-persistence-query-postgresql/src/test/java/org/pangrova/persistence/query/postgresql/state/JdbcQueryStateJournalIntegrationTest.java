package org.pangrova.persistence.query.postgresql.state;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.testing.testkit.PostgresCommitWireFault;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.query.api.journal.TaskStateEvent;
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
    private boolean stickyConflict;

    @BeforeEach
    void setUp() throws Exception {
        var source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        dataSource = source;
        initSchema(dataSource);
        cleanTables(dataSource);
        var config = new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24, 50_000);
        journal = new JdbcQueryStateJournal(dataSource, config);
    }

    @AfterEach
    void tearDown() {
        if (journal != null) {
            if (stickyConflict) {
                assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, journal::close);
                assertTrue(journal.isDrained());
            } else { journal.close(); }
        }
    }

    @Test
    void syncWriteAndReplayQueryEvents() {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(2000L, null), "q-1", null, null, "RUNNING"));

        List<QueryStateEvent> events = journal.replayActiveQueries();
        assertEquals(2, events.size());
        assertEquals("QUEUED", events.get(0).state());
        assertEquals("RUNNING", events.get(1).state());
    }

    @Test
    void syncWriteAndReplayTaskEvents() {
        // Need a non-terminal query for tasks to be replayed
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendTaskEventSync(
                new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 1L));
        journal.appendTaskEventSync(
                new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1100L, null), "q-1", "t-2", "w-2", "RUNNING", 1, 1L));

        List<TaskStateEvent> events = journal.replayActiveTasks();
        assertEquals(2, events.size());
        assertEquals("t-1", events.get(0).taskId());
        assertEquals("t-2", events.get(1).taskId());
    }

    @Test
    void terminalQueryExcludedFromReplay() {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(2000L, null), "q-1", null, null, "FINISHED"));

        List<QueryStateEvent> events = journal.replayActiveQueries();
        assertTrue(events.isEmpty(), "Finished query should not be replayed");
    }

    @Test
    void purgeRemovesAllEventsForQuery() {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendTaskEventSync(
                new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 1L));

        journal.purgeQuery("q-1");

        assertTrue(journal.replayActiveQueries().isEmpty());
        assertTrue(journal.replayActiveTasks().isEmpty());
    }

    @Test
    void purgeDoesNotAffectOtherQueries() {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1100L, null), "q-2", "SELECT 2", "{}", "QUEUED"));

        journal.purgeQuery("q-1");

        List<QueryStateEvent> events = journal.replayActiveQueries();
        assertEquals(1, events.size());
        assertEquals("q-2", events.get(0).queryId());
    }

    @Test
    void purgePermissionFailureRollsBackTaskDeletionAndSameQueryCanRetry() throws Exception {
        String role = "purge_" + java.util.UUID.randomUUID().toString().replace("-", "");
        String password = java.util.UUID.randomUUID().toString();
        executeSql("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'");
        try {
            seedPurgeQueries();
            executeSql("GRANT USAGE ON SCHEMA public TO " + role);
            executeSql("GRANT SELECT, DELETE ON task_state_journal TO " + role);
            executeSql("GRANT SELECT ON query_state_journal TO " + role);
            var restrictedSource = restrictedSource(POSTGRES.getJdbcUrl(), role, password);
            try (var restricted = new JdbcQueryStateJournal(restrictedSource,
                    new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24, 50_000))) {
                var failure = assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                        () -> restricted.purgeQuery("q-owned"));
                assertTrue(failure.getMessage().contains("q-owned"));
                assertInstanceOf(java.sql.SQLException.class, failure.getCause());
                assertEquals(1, countQueryRows("task_state_journal", "q-owned"));
                assertEquals(1, countQueryRows("query_state_journal", "q-owned"));
                executeSql("GRANT DELETE ON query_state_journal TO " + role);
                restricted.purgeQuery("q-owned");
                restricted.purgeQuery("q-owned");
                assertEquals(0, countQueryRows("task_state_journal", "q-owned"));
                assertEquals(0, countQueryRows("query_state_journal", "q-owned"));
                assertEquals(1, countQueryRows("task_state_journal", "q-other"));
                assertEquals(1, countQueryRows("query_state_journal", "q-other"));
            }
        } finally {
            executeSql("DROP OWNED BY " + role);
            executeSql("DROP ROLE " + role);
        }
    }

    @Test
    void closedJournalCannotAcknowledgePurgeWithoutRemovingRows() throws Exception {
        seedPurgeQueries();
        journal.close();
        assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, () -> journal.purgeQuery("q-owned"));
        assertEquals(1, countQueryRows("task_state_journal", "q-owned"));
        assertEquals(1, countQueryRows("query_state_journal", "q-owned"));
    }

    private void seedPurgeQueries() {
        for (String queryId : List.of("q-owned", "q-other")) {
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), queryId, "SELECT 1", "{}", "RUNNING"));
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), queryId, queryId + "-task", "worker", "RUNNING", 0, 1L));
        }
    }

    private void executeSql(String sql) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private int countQueryRows(String table, String queryId) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("SELECT count(*) FROM " + table + " WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                return rows.getInt(1);
            }
        }
    }

    @Test
    void asyncFlushBatchingBehavior() throws InterruptedException {
        // Need a non-terminal query for tasks to be replayed
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-async", "SELECT 1", "{}", "QUEUED"));

        // Append async events
        for (int i = 0; i < 5; i++) {
            journal.appendTaskEvent(
                    new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L + i, null), "q-async", "t-" + i, "w-1", "RUNNING", 0, i));
        }

        // Wait for flush (config interval is 50ms, give it 2s)
        await().atMost(2, SECONDS).until(() -> journal.replayActiveTasks().size() == 5);

        List<TaskStateEvent> events = journal.replayActiveTasks();
        assertEquals(5, events.size(), "All async events should be flushed");
    }

    @Test
    void closeFlushesRemainingEvents() throws InterruptedException {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-close", "SELECT 1", "{}", "QUEUED"));

        // Append async events
        for (int i = 0; i < 3; i++) {
            journal.appendTaskEvent(
                    new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L + i, null), "q-close", "t-" + i, "w-1", "RUNNING", 0, i));
        }

        // Close immediately (should flush remaining)
        journal.close();

        // Create a new journal to verify data was persisted
        var config2 = new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24, 50_000);
        try (var journal2 = new JdbcQueryStateJournal(dataSource, config2)) {
            List<TaskStateEvent> events = journal2.replayActiveTasks();
            assertEquals(3, events.size(), "Close should flush remaining async events");
        }
    }

    @Test
    void multipleQueriesMixedStates() {
        // q-1: active (QUEUED → RUNNING)
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(2000L, null), "q-1", null, null, "RUNNING"));

        // q-2: terminal (QUEUED → FAILED)
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1100L, null), "q-2", "SELECT 2", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(2100L, null), "q-2", null, null, "FAILED"));

        // q-3: active (QUEUED only)
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1200L, null), "q-3", "SELECT 3", "{}", "QUEUED"));

        List<QueryStateEvent> events = journal.replayActiveQueries();
        // Only q-1 and q-3 events should be replayed
        long distinctQueries = events.stream()
                .map(QueryStateEvent::queryId).distinct().count();
        assertEquals(2, distinctQueries, "Only active queries should be replayed");
        assertTrue(events.stream().noneMatch(e -> "q-2".equals(e.queryId())),
                "Terminal query q-2 should not be replayed");
    }

    private static void initSchema(DataSource source) {
        try (Connection conn = source.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS query_state_journal (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
                        query_id VARCHAR(64) NOT NULL,
                        sql_text TEXT,
                        session_json TEXT,
                        state VARCHAR(32) NOT NULL,
                        timestamp_ms BIGINT NOT NULL,
                        coordinator_id VARCHAR(128),
                        epoch BIGINT NOT NULL DEFAULT 0,
                        lineage_payload JSONB
                    )""");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS task_state_journal (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
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
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void exactEventRetryReconcilesOriginalPayloadWithoutCreatingAnotherOrderedRow() throws Exception {
        var event = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(123, "issuer"),
                "q-exact", "SELECT 3", "{\"user\":\"alice\"}", "QUEUED");
        journal.appendQueryEventSync(event);
        long rowId = eventRowId(event.context().eventId());
        journal.appendQueryEventSync(event);
        assertEquals(event, journal.latestQueryEvent("q-exact").orElseThrow());
        assertEquals(1, countQueryRows("query_state_journal", "q-exact"));
        assertEquals(rowId, eventRowId(event.context().eventId()));
    }

    @Test
    void changedPayloadCannotReplaceCommittedEventAndStickyFailureCannotMasqueradeAsCloseSuccess() throws Exception {
        var context = org.pangrova.query.api.journal.JournalEventContext.create(456, "issuer");
        var event = new QueryStateEvent(context, "q-conflict", "SELECT 1", "{}", "QUEUED");
        journal.appendQueryEventSync(event);
        var changed = new QueryStateEvent(context, "q-conflict", "SELECT 2", "{}", "QUEUED");
        assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                () -> journal.appendQueryEventSync(changed));
        stickyConflict = true;
        assertEquals(event, journal.latestQueryEvent("q-conflict").orElseThrow());
        assertEquals(1, countQueryRows("query_state_journal", "q-conflict"));
        assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                () -> journal.appendQueryEvent(new QueryStateEvent(
                        org.pangrova.query.api.journal.JournalEventContext.create(999, "issuer"),
                        "q-after-conflict", "SELECT 1", "{}", "QUEUED")));
        assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, journal::close);
        assertTrue(journal.isDrained(), "Actual writer and SQL owners have exited, not a successful commit claim");
        assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, journal::close);
        assertEquals(0, countQueryRows("query_state_journal", "q-after-conflict"));
    }

    @Test
    void actualBlockedCommitConsumesResidentCapacityUntilSqlCommit() throws Exception {
        journal.close();
        journal = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 10, 1, 24, 1));
        var context = org.pangrova.query.api.journal.JournalEventContext.create(700, "issuer");
        var event = new QueryStateEvent(context, "q-bounded", "SELECT 1", "{}", "QUEUED");
        try (Connection blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (Statement statement = blocker.createStatement()) {
                statement.execute("LOCK TABLE query_state_journal IN ACCESS EXCLUSIVE MODE");
            }
            journal.appendQueryEvent(event);
            await().atMost(5, SECONDS).until(this::actualJournalInsertWaitsForTableLock);
            assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.appendQueryEvent(new QueryStateEvent(
                            org.pangrova.query.api.journal.JournalEventContext.create(701, "issuer"),
                            "q-overflow", "SELECT 2", "{}", "QUEUED")));
            blocker.commit();
        }
        await().atMost(5, SECONDS).until(() -> journal.latestQueryEvent("q-bounded").isPresent());
        assertEquals(event, journal.latestQueryEvent("q-bounded").orElseThrow());
        assertEquals(1, countQueryRows("query_state_journal", "q-bounded"));
        assertEquals(0, countQueryRows("query_state_journal", "q-overflow"));
    }

    @Test
    void rejectedQueryDoesNotRollbackHealthyQueryBatchAndItsOwnLaterTaskCannotOvertake() throws Exception {
        verifyQueryIsolation(20);
    }

    @Test
    void singleEventBatchesRemainFairWhileOneQueryCannotCommit() throws Exception {
        verifyQueryIsolation(1);
    }

    private void verifyQueryIsolation(int batchSize) throws Exception {
        journal.close();
        journal = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 20, batchSize, 24, 10));
        var denied = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(800, "issuer"),
                "q-fair-bad", "SELECT 42", "{}", "FINISHED");
        var later = new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(801, "issuer"),
                denied.queryId(), "t-bad", "worker", "FINISHED", 0, 2);
        installQueryIsolationFault();
        try {
            var failure = assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.appendQueryEventSync(denied));
            assertEquals(org.pangrova.persistence.query.api.commit.JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED,
                    failure.admissionOutcome());
            requireQueryIsolationFault(failure);
            journal.appendTaskEvent(later);
            var healthy = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(802, "issuer"),
                    "q-fair-good", "SELECT 43", "{}", "RUNNING");
            var healthyTask = new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(803, "issuer"),
                    healthy.queryId(), "t-good", "worker", "FINISHED", 0, 2);
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
                journal.appendQueryEventSync(healthy);
                journal.appendTaskEventSync(healthyTask);
            });
            assertEquals(healthy, journal.latestQueryEvent(healthy.queryId()).orElseThrow());
            assertEquals(healthyTask, storedTaskEvent(healthyTask.context().eventId()));
            assertEquals(0, countQueryRows("query_state_journal", denied.queryId()));
            assertEquals(0, countQueryRows("task_state_journal", denied.queryId()),
                    "A later task must not bypass its query's uncommitted earlier event");
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.purgeQuery(denied.queryId())));
            removeQueryIsolationFault();
            journal.appendQueryEventSync(denied);
            journal.appendTaskEventSync(later);
            assertEquals(denied, journal.latestQueryEvent(denied.queryId()).orElseThrow());
            assertEquals(later, storedTaskEvent(later.context().eventId()));
            long queryRow = eventRowId(denied.context().eventId());
            long taskRow = eventRowId("task_state_journal", later.context().eventId());
            journal.appendQueryEventSync(denied);
            journal.appendTaskEventSync(later);
            assertEquals(queryRow, eventRowId(denied.context().eventId()));
            assertEquals(taskRow, eventRowId("task_state_journal", later.context().eventId()));
            assertEquals(1, countQueryRows("query_state_journal", denied.queryId()));
            assertEquals(1, countQueryRows("task_state_journal", denied.queryId()));
            journal.purgeQuery(denied.queryId());
            assertEquals(0, countQueryRows("query_state_journal", denied.queryId()));
            assertEquals(0, countQueryRows("task_state_journal", denied.queryId()));
            assertEquals(1, countQueryRows("query_state_journal", healthy.queryId()));
            assertEquals(1, countQueryRows("task_state_journal", healthy.queryId()));
        } finally { removeQueryIsolationFault(); }
    }

    @Test
    void rejectedQueryRetainsCapacityAndCloseOwnershipUntilExactFactsActuallyCommit() throws Exception {
        journal.close();
        journal = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 20, 1, 24, 2));
        var denied = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(810, "issuer"),
                "q-fair-bad", "SELECT 42", "{}", "FINISHED");
        var later = new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(811, "issuer"),
                denied.queryId(), "t-bad", "worker", "FINISHED", 0, 2);
        installQueryIsolationFault();
        try {
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.appendQueryEventSync(denied)));
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.appendQueryEventSync(denied)));
            // Exact retries join one resident owner; the second distinct event still fits.
            journal.appendTaskEvent(later);
            var overflow = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(812, "issuer"),
                    "q-overflow", "SELECT 44", "{}", "QUEUED");
            var rejected = assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.appendQueryEvent(overflow));
            assertEquals(org.pangrova.persistence.query.api.commit.JournalWriteException.AdmissionOutcome.REJECTED_BEFORE_TRANSFER,
                    rejected.admissionOutcome());
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.purgeQuery(denied.queryId())));
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, journal::close));
            assertFalse(journal.isDrained());
            assertEquals(0, countQueryRows("query_state_journal", denied.queryId()));
            assertEquals(0, countQueryRows("task_state_journal", denied.queryId()));
            removeQueryIsolationFault();
            journal.close();
            assertTrue(journal.isDrained());
            assertEquals(1, countQueryRows("query_state_journal", denied.queryId()));
            assertEquals(1, countQueryRows("task_state_journal", denied.queryId()));
            assertEquals(0, countQueryRows("query_state_journal", overflow.queryId()));
            assertEquals(denied, storedQueryEvent(denied.context().eventId()));
            assertEquals(later, storedTaskEvent(later.context().eventId()));
            long queryRow = eventRowId(denied.context().eventId());
            long taskRow = eventRowId("task_state_journal", later.context().eventId());
            journal.close();
            assertEquals(queryRow, eventRowId(denied.context().eventId()));
            assertEquals(taskRow, eventRowId("task_state_journal", later.context().eventId()));
        } finally { removeQueryIsolationFault(); }
    }

    private void installQueryIsolationFault() throws java.sql.SQLException {
        executeSql("CREATE FUNCTION reject_isolated_query_insert() RETURNS trigger LANGUAGE plpgsql AS $$"
                + "BEGIN IF NEW.query_id = 'q-fair-bad' AND NEW.state = 'FINISHED' THEN "
                + "RAISE EXCEPTION 'Actual isolated query FINISHED insert denied' USING ERRCODE = '42501'; "
                + "END IF; RETURN NEW; END $$");
        executeSql("CREATE TRIGGER reject_isolated_query_insert BEFORE INSERT ON query_state_journal "
                + "FOR EACH ROW EXECUTE FUNCTION reject_isolated_query_insert()");
    }

    private void removeQueryIsolationFault() throws java.sql.SQLException {
        executeSql("DROP TRIGGER IF EXISTS reject_isolated_query_insert ON query_state_journal");
        executeSql("DROP FUNCTION IF EXISTS reject_isolated_query_insert()");
    }

    private static void requireQueryIsolationFault(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) cause = cause.getCause();
        var actual = assertInstanceOf(java.sql.SQLException.class, cause);
        assertEquals("42501", actual.getSQLState());
        assertTrue(actual.getMessage().contains("Actual isolated query FINISHED insert denied"));
    }

    private TaskStateEvent storedTaskEvent(java.util.UUID eventId) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT " + JournalEventSql.TASK_COLUMNS + " FROM task_state_journal WHERE event_id = ?")) {
            statement.setObject(1, eventId);
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                var event = JournalEventSql.readTask(rows);
                assertFalse(rows.next());
                return event;
            }
        }
    }

    private QueryStateEvent storedQueryEvent(java.util.UUID eventId) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT " + JournalEventSql.QUERY_COLUMNS + " FROM query_state_journal WHERE event_id = ?")) {
            statement.setObject(1, eventId);
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                var event = JournalEventSql.readQuery(rows);
                assertFalse(rows.next());
                return event;
            }
        }
    }

    @Test
    void failedActualWriterJoinRetainsEventAndCloseRetriesAfterDatabaseUnblocks() throws Exception {
        var event = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(900, "issuer"),
                "q-held-close", "SELECT 1", "{}", "QUEUED");
        try (Connection blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (Statement statement = blocker.createStatement()) {
                statement.execute("LOCK TABLE query_state_journal IN ACCESS EXCLUSIVE MODE");
            }
            journal.appendQueryEvent(event);
            await().atMost(5, SECONDS).until(this::actualJournalInsertWaitsForTableLock);
            assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, journal::close);
            assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                    () -> journal.appendQueryEvent(event));
            blocker.commit();
        }
        journal.close();
        try (var reopened = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24, 50_000))) {
            assertEquals(event, reopened.latestQueryEvent("q-held-close").orElseThrow());
            assertEquals(1, countQueryRows("query_state_journal", "q-held-close"));
        }
    }

    private long eventRowId(java.util.UUID eventId) throws java.sql.SQLException {
        return eventRowId("query_state_journal", eventId);
    }

    private long eventRowId(String table, java.util.UUID eventId) throws java.sql.SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("SELECT id FROM " + table + " WHERE event_id = ?")) {
            statement.setObject(1, eventId);
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                long id = rows.getLong(1);
                assertFalse(rows.next());
                return id;
            }
        }
    }

    private boolean actualJournalInsertWaitsForTableLock() throws java.sql.SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        + "WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO query_state_journal%')")) {
            assertTrue(rows.next());
            return rows.getBoolean(1);
        }
    }

    private static void cleanTables(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM task_state_journal");
            stmt.execute("DELETE FROM query_state_journal");
        }
    }

    @Test
    void lostActualPostgresCommitResponseReconcilesSameFrozenEventAndRetainsOrderedIdentity() throws Exception {
        journal.close();
        try (var wire = new PostgresCommitWireFault(POSTGRES.getHost(), POSTGRES.getMappedPort(5432))) {
            var source = new org.postgresql.ds.PGSimpleDataSource();
            source.setServerNames(new String[] {java.net.InetAddress.getLoopbackAddress().getHostAddress()});
            source.setPortNumbers(new int[] {wire.port()});
            source.setDatabaseName(POSTGRES.getDatabaseName());
            source.setUser(POSTGRES.getUsername()); source.setPassword(POSTGRES.getPassword());
            // Only the local fault-observation connection is plaintext, matching this PostgreSQL container.
            source.setSslMode("disable"); source.setGssEncMode("disable");
            journal = new JdbcQueryStateJournal(source,
                    new StateJournalConfig("JDBC", source.getURL(), 20, 1, 24, 1));
            var event = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(222, "issuer"),
                    "q-lost-commit", "SELECT 222", "{\"original\":true}", "QUEUED");
            wire.armCommitResponseLoss();
            try {
                var failed = assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class,
                        () -> journal.appendQueryEventSync(event));
                assertEquals(org.pangrova.persistence.query.api.commit.JournalWriteException.AdmissionOutcome.ACCEPTED_UNESTABLISHED,
                        failed.admissionOutcome());
                assertTrue(wire.awaitActualCommit());
                assertEquals(1, countQueryRows("query_state_journal", event.queryId()));
                long originalRow = eventRowId(event.context().eventId());
                Thread.currentThread().interrupt();
                try {
                    assertThrows(org.pangrova.persistence.query.api.commit.JournalWriteException.class, journal::close);
                    assertTrue(Thread.currentThread().isInterrupted());
                    assertFalse(journal.isDrained());
                } finally { Thread.interrupted(); }
                wire.allowReconciliation();
                journal.close();
                try (var reader = new JdbcQueryStateJournal(dataSource,
                        new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 20, 1, 24, 1))) {
                    assertEquals(event, reader.latestQueryEvent(event.queryId()).orElseThrow());
                }
                assertEquals(1, countQueryRows("query_state_journal", event.queryId()));
                assertEquals(originalRow, eventRowId(event.context().eventId()));
            } finally { wire.allowReconciliation(); journal.close(); }
        }
    }

    private static PGSimpleDataSource restrictedSource(String url, String user, String password) {
        var source = new PGSimpleDataSource();
        source.setURL(url);
        source.setUser(user);
        source.setPassword(password);
        return source;
    }
}
