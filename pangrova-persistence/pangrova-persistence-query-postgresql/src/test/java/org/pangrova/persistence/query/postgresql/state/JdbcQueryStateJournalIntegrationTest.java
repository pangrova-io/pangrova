package org.pangrova.persistence.query.postgresql.state;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.common.client.query.QueryOwnerEndpoint;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.query.api.journal.QueryTerminalOutcome;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceRequest;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.lineage.QueryLineageFact;
import org.pangrova.query.api.lineage.QueryLineageMetadata;
import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.testing.testkit.PostgresCommitWireFault;
import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.common.lifecycle.ProcessIncarnationStore;
import org.pangrova.persistence.query.postgresql.coordination.JdbcCoordinatorWorkRecovery;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore;
import org.pangrova.query.api.journal.TaskIssuance;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainOutcome;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainReceipt;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration test for {@link JdbcQueryStateJournal} using TestContainers PostgreSQL.
 * Validates the full write → lookup → purge cycle.
 */
@Testcontainers
class JdbcQueryStateJournalIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine").withDatabaseName("pangrova_test")
                    .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements");

    private DataSource dataSource;
    private JdbcQueryStateJournal journal;
    private boolean stickyConflict;

    @org.junit.jupiter.api.BeforeAll
    static void initializeJournalDatabase() {
        org.flywaydb.core.Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @BeforeEach
    void setUp() throws Exception {
        var source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        dataSource = source;
        cleanTables(dataSource);
        var config = new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24, 50_000);
        journal = new JdbcQueryStateJournal(dataSource, config);
    }

    @AfterEach
    void tearDown() {
        if (journal != null) {
            if (stickyConflict) {
                assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, journal::close);
                assertTrue(journal.isDrained());
            } else { journal.close(); }
        }
    }

    @Test
    void syncWritePreservesQueryFacts() throws Exception {
        var queued = new QueryStateEvent(JournalEventContext.create(2000L, null), "q-1", "SELECT 1", "{}", "QUEUED");
        var running = new QueryStateEvent(JournalEventContext.create(1000L, null), "q-1", null, null, "RUNNING");
        journal.appendQueryEventSync(queued);
        assertEquals(queued, journal.latestQueryEvent("q-1").orElseThrow());
        journal.appendQueryEventSync(running);
        assertEquals(running, journal.latestQueryEvent("q-1").orElseThrow());
        assertEquals(2, countQueryRows("query_state_journal", "q-1"));
    }

    @Test
    void persistsNamespacedGenerationTaskIdentitiesExactly() throws Exception {
        var context = org.pangrova.query.api.journal.JournalEventContext.create(1000L, "coordinator");
        var event = new TaskStateEvent(context, "artifact:" + "a".repeat(64),
                "artifact-task:" + "b".repeat(64), "worker", "FINISHED", 0, 2L);

        journal.appendTaskEventSync(event);

        assertEquals(event, storedTaskEvent(context.eventId()));
    }

    @Test
    void syncWritePersistsTaskEventsInOrder() throws Exception {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendTaskEventSync(
                new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 1L));
        journal.appendTaskEventSync(
                new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1100L, null), "q-1", "t-2", "w-2", "RUNNING", 1, 1L));

        List<TaskStateEvent> events = storedTasks("q-1");
        assertEquals(2, events.size());
        assertEquals("t-1", events.get(0).taskId());
        assertEquals("t-2", events.get(1).taskId());
    }

    @Test
    void claimedRecoveryReturnsOnlyMatchingIdsInOneStatement() throws Exception {
        executeSql("""
                INSERT INTO query_state_journal (event_id, query_id, state, timestamp_ms)
                SELECT gen_random_uuid(), 'peer-' || i, 'RUNNING', 100
                FROM generate_series(1, 4096) i
                """);
        executeSql("""
                INSERT INTO task_state_journal (event_id, query_id, task_id, state, stage_id, timestamp_ms)
                SELECT gen_random_uuid(), 'peer-' || i, 'peer-task-' || i, 'RUNNING', 0, 100
                FROM generate_series(1, 4096) i
                """);
        var admission = acceptanceEvent("q-claimed", "alice", "a", "b", 60_000);
        journal.acceptQuerySync(admission);
        var running = new QueryStateEvent(JournalEventContext.create(100, "successor"),
                "q-claimed", null, null, "RUNNING");
        var task = new TaskStateEvent(JournalEventContext.create(99, "predecessor"),
                "q-claimed", "t-claimed", "worker", "RUNNING", 0, 1);
        journal.appendQueryEventSync(running);
        journal.appendTaskEventSync(task);
        journal.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(101, "predecessor"),
                "q-terminal", null, null, "FINISHED"));
        for (String query : List.of("q-terminal", "q-orphan")) {
            journal.appendTaskEventSync(new TaskStateEvent(JournalEventContext.create(100, "predecessor"),
                    query, "t-" + query, "worker", "RUNNING", 0, 1));
        }
        executeSql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        executeSql("SELECT pg_stat_statements_reset()");

        assertThat(journal.observedRecoveryExecutions(Set.of())).isEmpty();
        var observed = journal.observedRecoveryExecutions(Set.of("q-claimed", "q-terminal", "q-orphan", "q-absent"));
        assertThat(observed).containsExactlyInAnyOrder("q-claimed", "q-terminal", "q-orphan");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT calls, rows FROM pg_stat_statements
                        WHERE (query LIKE 'SELECT %' OR query LIKE 'WITH %')
                          AND (query LIKE '%query_state_journal%' OR query LIKE '%task_state_journal%')
                          AND query NOT LIKE '%FROM pg_stat_statements%'
                        """)) {
            assertTrue(rows.next());
            assertEquals(1L, rows.getLong("calls"));
            assertEquals(3L, rows.getLong("rows"));
            assertFalse(rows.next());
        }
    }

    @Test
    void exactIssuanceUsesOneStatementAndSurvivesTakeover() throws Exception {
        String id = "issuance-cohort";
        var store = new JdbcExecutionAnchorStore(dataSource);
        var duration = Duration.ofMinutes(1);
        var old = store.tryAcquire(id, "owner", "boot", duration).orElseThrow();
        journal.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(100, "owner"),
                id, "SELECT 1", "{}", "RUNNING"));
        var issuance = new TaskIssuance("worker-host", 8081, "boot", old.fencingToken(), new byte[] {1, 2, 3});
        var tasks = java.util.stream.IntStream.range(0, 16).mapToObj(partition ->
                new TaskStateEvent(JournalEventContext.create(105, "owner"), id, "task-" + partition,
                        "worker-id", "SCHEDULED", 1, 0, issuance)).toList();
        executeSql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        executeSql("SELECT pg_stat_statements_reset()");

        journal.appendTaskIssuancesSync(tasks);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT calls FROM pg_stat_statements
                        WHERE query LIKE 'WITH locked AS MATERIALIZED (%INSERT INTO task_state_journal%'
                        """)) {
            assertTrue(rows.next());
            assertEquals(1, rows.getLong(1));
            assertFalse(rows.next());
        }
        assertEquals(0L, eventReadbacks());
        journal.appendTaskIssuancesSync(tasks);
        assertThat(storedTasks(id)).containsExactlyInAnyOrderElementsOf(tasks);

        var event = tasks.get(0);
        var changed = new TaskStateEvent(event.context(), id, event.taskId(), event.workerId(), event.state(),
                event.stageId(), event.reportSequence(),
                new TaskIssuance("different-worker", 8081, "boot", old.fencingToken(), issuance.admissionSeal()));
        var candidate = new TaskStateEvent(JournalEventContext.create(106, "owner"), id, "task-extra",
                "worker-id", "SCHEDULED", 1, 0, issuance);
        assertThrows(JournalWriteException.class, () -> journal.appendTaskIssuancesSync(List.of(candidate, changed)));
        assertEquals(16, countQueryRows("task_state_journal", id));

        assertTrue(store.yield(old));
        store.tryAcquire(id, "successor", "new-boot", duration).orElseThrow();
        var rejected = assertThrows(JournalWriteException.class,
                () -> journal.appendTaskIssuancesSync(List.of(candidate)));
        assertEquals(JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER, rejected.admissionOutcome());
        assertThat(storedTasks(id)).containsExactlyInAnyOrderElementsOf(tasks);
    }

    @Test
    void issuanceStatementShapesPersistOnlyActualRowsAtEveryCohortSize() throws Exception {
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var duration = Duration.ofMinutes(1);
        executeSql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        executeSql("SELECT pg_stat_statements_reset()");
        for (int size = 1; size <= 16; size++) {
            String id = "issuance-size-" + size;
            var owner = anchors.tryAcquire(id, "owner", "boot", duration).orElseThrow();
            journal.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(100, "owner"),
                    id, "SELECT 1", "{}", "RUNNING"));
            var issuance = new TaskIssuance("worker-host", 8081, "boot", owner.fencingToken(),
                    new byte[] {1, 2, 3});
            var tasks = java.util.stream.IntStream.range(0, size).mapToObj(partition ->
                    new TaskStateEvent(JournalEventContext.create(105, "owner"), id, id + "-task-" + partition,
                            "worker-id", "SCHEDULED", 1, 0, issuance)).toList();
            journal.appendTaskIssuancesSync(tasks);
            assertEquals(size, countQueryRows("task_state_journal", id));
            assertThat(storedTasks(id)).containsExactlyInAnyOrderElementsOf(tasks);
        }
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COUNT(*), SUM(calls) FROM pg_stat_statements
                        WHERE query LIKE 'WITH locked AS MATERIALIZED (%INSERT INTO task_state_journal%'
                        """)) {
            assertTrue(rows.next());
            assertEquals(16, rows.getLong(1));
            assertEquals(16, rows.getLong(2));
        }
        assertEquals(0L, eventReadbacks());
    }

    @Test
    void successorPagesTerminalAndOrphanIssuancesAndFencesBatchedReceipts() throws Exception {
        var store = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var terminal = store.tryAcquire("terminal-issuance", "owner", "boot", lease).orElseThrow();
        var orphan = store.tryAcquire("orphan-issuance", "owner", "boot", lease).orElseThrow();
        journal.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(101, "owner"),
                terminal.executionId(), "SELECT 1", "{}", "FINISHED"));
        int pageSize = org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.PAGE_SIZE;
        journal.appendTaskIssuancesSync(recoveryIssuances(terminal, 0, pageSize));
        journal.appendTaskIssuancesSync(recoveryIssuances(terminal, pageSize, 3));
        journal.appendTaskIssuancesSync(recoveryIssuances(orphan, 0, 1));
        assertThat(journal.pendingTaskDrains(terminal, 0)).isEmpty();
        assertThat(journal.pendingTaskDrains(orphan, 0)).isEmpty();

        assertTrue(store.yield(terminal));
        assertTrue(store.yield(orphan));
        var recoveredTerminal = store.tryAcquire(terminal.executionId(), "successor", "next-boot", lease)
                .orElseThrow();
        var recoveredOrphan = store.tryAcquire(orphan.executionId(), "successor", "next-boot", lease).orElseThrow();
        journal.appendTaskIssuancesSync(recoveryIssuances(recoveredTerminal, 30, 1));
        assertThat(journal.observedRecoveryExecutions(Set.of(terminal.executionId(), orphan.executionId())))
                .containsExactlyInAnyOrder(terminal.executionId(), orphan.executionId());
        assertThat(journal.pendingTaskDrains(terminal, 0)).isEmpty();
        var first = journal.pendingTaskDrains(recoveredTerminal, 0);
        assertThat(first).hasSize(pageSize);
        var later = journal.pendingTaskDrains(recoveredTerminal, first.get(first.size() - 1).rowId());
        assertThat(later).hasSize(3);
        var orphanPage = journal.pendingTaskDrains(recoveredOrphan, 0);
        assertThat(orphanPage).hasSize(1);
        var receipts = first.stream().map(row ->
                new org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.DrainReceipt(
                        row.rowId(), new byte[] {17, 18})).toList();

        assertFalse(journal.retainTaskDrains(terminal, receipts));
        executeSql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        executeSql("SELECT pg_stat_statements_reset()");
        assertTrue(journal.retainTaskDrains(recoveredTerminal, receipts));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT calls FROM pg_stat_statements
                        WHERE query LIKE 'WITH locked AS MATERIALIZED (%SET recovery_drain_receipt%'
                        """)) {
            assertTrue(rows.next());
            assertEquals(1L, rows.getLong(1));
            assertFalse(rows.next());
        }
        assertThat(journal.pendingTaskDrains(recoveredTerminal, 0)).containsExactlyElementsOf(later);
        assertThat(journal.pendingTaskDrains(recoveredOrphan, 0)).containsExactlyElementsOf(orphanPage);

        assertTrue(store.yield(recoveredTerminal));
        var laterOwner = store.tryAcquire(terminal.executionId(), "third", "third-boot", lease).orElseThrow();
        assertFalse(journal.retainTaskDrains(recoveredTerminal, receipts));
        assertThat(journal.pendingTaskDrains(laterOwner, 0)).hasSize(4);
    }

    @Test
    void terminatesOnlyCoveredPredecessorsAndClosesSameClaimIssuance() throws Exception {
        var store = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var original = store.tryAcquire("proof-terminal", "prior", "prior-boot", lease).orElseThrow();
        var missing = store.tryAcquire("proof-missing", "prior", "prior-boot", lease).orElseThrow();
        var active = store.tryAcquire("proof-active", "prior", "prior-boot", lease).orElseThrow();
        for (var anchor : List.of(original, missing, active)) {
            String state = anchor.equals(active) ? "RUNNING" : "FAILED";
            journal.appendQueryEventSync(new QueryStateEvent(JournalEventContext.create(101, "prior"),
                    anchor.executionId(), "SELECT 1", "{}", state));
            journal.appendTaskIssuancesSync(recoveryIssuances(anchor, 0, 1));
            assertTrue(store.yield(anchor));
        }
        var claim = store.tryAcquire(original.executionId(), "successor", "next-boot", lease).orElseThrow();
        var missingClaim = store.tryAcquire(missing.executionId(), "successor", "next-boot", lease).orElseThrow();
        var activeClaim = store.tryAcquire(active.executionId(), "successor", "next-boot", lease).orElseThrow();
        assertEquals(DrainOutcome.LOST_CLAIM, journal.confirmRecoveredTaskDrains(original));
        assertEquals(DrainOutcome.PENDING_RECEIPTS, journal.confirmRecoveredTaskDrains(claim));
        assertEquals(DrainOutcome.PENDING_RECEIPTS, journal.confirmRecoveredTaskDrains(activeClaim));
        assertEquals("RUNNING", journal.latestQueryEvent(active.executionId()).orElseThrow().state());
        assertEquals(Set.of(original.executionId(), missing.executionId(), active.executionId()),
                journal.observedRecoveryExecutions(Set.of(
                        original.executionId(), missing.executionId(), active.executionId(), "no-evidence")));
        for (var recovered : List.of(claim, missingClaim, activeClaim)) {
            var attempt = journal.pendingTaskDrains(recovered, 0).get(0);
            assertTrue(journal.retainTaskDrains(recovered, List.of(new DrainReceipt(attempt.rowId(), new byte[] {1}))));
        }
        journal.appendTaskEventSync(new TaskStateEvent(JournalEventContext.create(106, "prior"),
                original.executionId(), "task-0", "worker-id", "FINISHED", 1, 1));
        journal.appendTaskEventSync(new TaskStateEvent(JournalEventContext.create(106, "prior"),
                missing.executionId(), "unbound-report", "worker-id", "FINISHED", 1, 1));
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(claim));
        assertEquals(DrainOutcome.MISSING_ISSUANCE, journal.confirmRecoveredTaskDrains(missingClaim));
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(activeClaim));
        var recoveredTerminal = journal.latestQueryEvent(active.executionId()).orElseThrow();
        assertEquals("FAILED", recoveredTerminal.state());
        assertEquals("successor", recoveredTerminal.context().coordinatorId());
        assertThat(recoveredTerminal.terminalOutcome().resultDisposition()).isEqualTo(ResultDisposition.NONE);
        assertThat(recoveredTerminal.terminalOutcome().error().errorCode()).isEqualTo("QUERY_OWNER_LOST");
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(activeClaim));
        assertEquals(recoveredTerminal, journal.latestQueryEvent(active.executionId()).orElseThrow());
        assertTrue(store.releaseDrainedQuery(activeClaim, false));
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                journal.appendTaskIssuancesSync(recoveryIssuances(claim, 1, 1)))
                .isInstanceOf(org.pangrova.persistence.query.api.exception.JournalWriteException.class);
        assertFalse(store.releaseDrainedQuery(missingClaim, false));
        assertTrue(store.releaseDrainedQuery(claim, false));
        assertEquals(DrainOutcome.LOST_CLAIM, journal.confirmRecoveredTaskDrains(claim));
    }

    @Test
    void coordinatorStatementAndPreDispatchRecoveryShareTheExactGrantBoundary() {
        var store = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var planning = store.tryAcquire("queued-only", "owner", "original-boot", lease).orElseThrow();
        var local = store.tryAcquire("local-statement", "owner", "original-boot", lease).orElseThrow();
        var requests = List.of(planning, local);
        for (var grant : requests) {
            var context = JournalEventContext.create(System.currentTimeMillis(), grant.ownerId());
            journal.appendFencedAdmissionSync(
                    new QueryStateEvent(context, grant.executionId(), "SELECT 1", "{}", "QUEUED"), grant);
        }
        assertTrue(journal.reserveCoordinatorStatement(local));
        assertFalse(journal.reserveCoordinatorStatement(local));
        assertFalse(store.release(local));

        assertTrue(store.yield(planning));
        assertTrue(store.yield(local));
        var planningClaim = store.tryAcquire(planning.executionId(), "successor", "next-boot", lease).orElseThrow();
        assertThat(store.tryAcquire(local.executionId(), "successor", "next-boot", lease)).isEmpty();
        assertThat(store.claimExpired("successor", "next-boot", lease, 1)).isEmpty();
        assertFalse(journal.reserveCoordinatorStatement(planning));
        assertFalse(journal.reserveCoordinatorStatement(planningClaim));
        assertEquals(DrainOutcome.LOST_CLAIM, journal.confirmRecoveredTaskDrains(local));
        assertFalse(store.releaseDrainedQuery(local, false));
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(planningClaim));
        var outcome = journal.latestQueryEvent(planning.executionId()).orElseThrow();
        assertThat(outcome.state()).isEqualTo("FAILED");
        assertThat(outcome.terminalOutcome().error().errorCode()).isEqualTo("QUERY_OWNER_LOST");
        assertTrue(store.releaseDrainedQuery(planningClaim, false));

        var wrongIncarnation = new org.pangrova.execution.api.ownership.ExecutionAnchor(local.executionId(),
                local.ownerId(), "another-boot", local.fencingToken(), local.acquiredAt(), local.expiresAt());
        assertFalse(store.releaseLocallyDrainedQuery(wrongIncarnation));
        assertFalse(store.release(local));
        assertFalse(store.releaseLocallyDrainedQuery(local));
        assertFalse(store.isQueryRetired(local.executionId()));
        var localClaim = store.tryAcquire(local.executionId(), "successor", "next-boot", lease).orElseThrow();
        assertFalse(store.releaseLocallyDrainedQuery(local));
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(localClaim));
        assertTrue(store.releaseDrainedQuery(localClaim, false));
        assertTrue(store.isQueryRetired(local.executionId()));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void expiredLocalRetirementKeepsJournalForTheSuccessor(boolean coordinatorStatement) {
        var store = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var original = store.tryAcquire("expired-local-statement", "original", "original-boot", lease).orElseThrow();
        var context = JournalEventContext.create(System.currentTimeMillis(), original.ownerId());
        journal.appendFencedAdmissionSync(new QueryStateEvent(context, original.executionId(), "SELECT 1",
                "{}", "QUEUED"), original);
        if (coordinatorStatement) assertTrue(journal.reserveCoordinatorStatement(original));

        assertTrue(store.yield(original));
        assertFalse(journal.durableRetirement().orElseThrow().test(original));
        assertThat(journal.latestQueryEvent(original.executionId())).isPresent();
        assertFalse(store.isQueryRetired(original.executionId()));

        var recovery = store.tryAcquire(original.executionId(), "successor", "next-boot", lease).orElseThrow();
        assertThat(journal.observedRecoveryExecutions(Set.of(recovery.executionId())))
                .containsExactly(recovery.executionId());
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(recovery));
        assertThat(journal.latestQueryEvent(original.executionId())).hasValueSatisfying(
                event -> assertThat(event.state()).isEqualTo("FAILED"));
        assertTrue(store.releaseDrainedQuery(recovery, false));
        assertThat(journal.latestQueryEvent(original.executionId())).isEmpty();
    }

    @Test
    void pendingWriteHandoffRetainsTheJournalAndBoundSlot() throws Exception {
        var store = new JdbcExecutionAnchorStore(dataSource);
        String query = "pending-write-retirement";
        var lease = Duration.ofMinutes(1);
        var original = store.tryAcquireLimitedQuery(query, "original", "original-boot", lease,
                "pending-write-tenant", 1).orElseThrow().anchor();
        var context = JournalEventContext.create(System.currentTimeMillis(), original.ownerId());
        journal.appendFencedAdmissionSync(new QueryStateEvent(context, query, "INSERT INTO t VALUES (1)",
                "{}", "QUEUED"), original);
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("""
                     INSERT INTO write_commit_journal (query_id, catalog_name, table_name, state,
                         timestamp_ms, idempotency_key, tenant_id, base_snapshot_id, write_output_binding,
                         commit_fingerprint, owner_id, owner_incarnation, execution_fencing_token)
                     VALUES (?, 'test', 'test.default.t', 'PENDING', ?, ?, 'pending-write-tenant',
                         -1, decode('01', 'hex'), ?, ?, ?, ?)
                     """)) {
            statement.setString(1, query);
            statement.setLong(2, System.currentTimeMillis());
            statement.setString(3, query);
            statement.setString(4, "a7".repeat(32));
            statement.setString(5, original.ownerId());
            statement.setString(6, original.ownerIncarnation());
            statement.setLong(7, original.fencingToken());
            assertEquals(1, statement.executeUpdate());
        }
        try {
            assertFalse(journal.durableRetirement().orElseThrow().test(original));
            assertThat(store.current(query)).isEmpty();
            assertFalse(store.isQueryRetired(query));
            assertThat(journal.latestQueryEvent(query)).isPresent();
            var recovery = store.tryAcquire(query, "successor", "next-boot", lease).orElseThrow();
            assertEquals(DrainOutcome.PENDING_WRITE, journal.confirmRecoveredTaskDrains(recovery));
        } finally {
            try (var connection = dataSource.getConnection();
                 var statement = connection.prepareStatement("DELETE FROM write_commit_journal WHERE query_id=?")) {
                statement.setString(1, query);
                statement.executeUpdate();
            }
        }
        var successor = store.current(query).orElseThrow();
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(successor));
        assertTrue(store.releaseDrainedQuery(successor, false));
        assertTrue(store.isQueryRetired(query));
    }

    @Test
    void journalAndBoundSlotAreVisibleUntilTheSameRetirementCommits() throws Exception {
        var store = new JdbcExecutionAnchorStore(dataSource);
        String query = "atomic-local-retirement";
        var grant = store.tryAcquireLimitedQuery(query, "original", "original-boot", Duration.ofMinutes(1),
                "atomic-retirement-tenant", 1).orElseThrow().anchor();
        var context = JournalEventContext.create(System.currentTimeMillis(), grant.ownerId());
        journal.appendFencedAdmissionSync(new QueryStateEvent(context, query, "SELECT 1", "{}", "QUEUED"), grant);
        journal.appendOwnedTerminalSync(new QueryStateEvent(
                JournalEventContext.create(System.currentTimeMillis(), grant.ownerId()),
                query, null, null, "FINISHED", null, null,
                new QueryTerminalOutcome(ResultDisposition.NONE, null, null)), grant);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION hold_atomic_query_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        PERFORM pg_advisory_xact_lock(173, 542);
                        RETURN OLD;
                    END;
                    $$;
                    CREATE TRIGGER hold_atomic_query_retirement BEFORE DELETE ON query_state_journal
                    FOR EACH ROW WHEN (OLD.query_id = 'atomic-local-retirement')
                    EXECUTE FUNCTION hold_atomic_query_retirement();
                    """);
        }
        var retire = journal.durableRetirement().orElseThrow();
        try (var guard = dataSource.getConnection(); var statement = guard.createStatement()) {
            guard.setAutoCommit(false);
            statement.execute("SELECT pg_advisory_xact_lock(173, 542)");
            var pending = java.util.concurrent.CompletableFuture.supplyAsync(() -> retire.test(grant));
            try {
                int backend = guard.unwrap(org.postgresql.PGConnection.class).getBackendPID();
                await().atMost(10, SECONDS).until(() -> blockedBy(backend));
                assertThat(pending).isNotDone();
                assertTrue(store.current(query).isPresent());
                assertFalse(store.isQueryRetired(query));
                assertThat(journal.latestQueryEvent(query)).isPresent();
                assertEquals(1, retirementTenantSlots());
            } finally {
                guard.rollback();
            }
            assertTrue(pending.get(10, SECONDS));
        } finally {
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.execute("DROP TRIGGER hold_atomic_query_retirement ON query_state_journal");
                statement.execute("DROP FUNCTION hold_atomic_query_retirement()");
            }
        }
        assertTrue(store.isQueryRetired(query));
        assertThat(journal.latestQueryEvent(query)).isEmpty();
        assertEquals(0, retirementTenantSlots());
    }

    private int retirementTenantSlots() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT active_count FROM tenant_concurrency_slots WHERE tenant_id='atomic-retirement-tenant'");
             var rows = statement.executeQuery()) {
            assertTrue(rows.next());
            return rows.getInt(1);
        }
    }

    @Test
    void retiredCoordinatorWaitsForItsAlreadyEnteredReservation(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var original = ProcessIncarnationStore.open(directory);
        var identity = original.identity();
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var owner = anchors.tryAcquire("late-local-reservation", "original", identity.incarnationId(), lease)
                .orElseThrow();
        var event = new QueryStateEvent(JournalEventContext.create(System.currentTimeMillis(), owner.ownerId()),
                owner.executionId(), "SHOW DYNAMIC TABLES", "{}", "QUEUED");
        journal.appendFencedAdmissionSync(event, owner);
        try (var inFlight = dataSource.getConnection()) {
            inFlight.setAutoCommit(false);
            assertTrue(JournalTaskDrainSql.reserveCoordinatorStatement(inFlight, owner, identity.ownershipDomainId()));
            // The old JVM has released its file lock while PostgreSQL still owns the open transaction.
            original.closeAfterDrain(() -> {});
            var replacement = ProcessIncarnationStore.open(directory);
            try {
                int backend = inFlight.unwrap(org.postgresql.PGConnection.class).getBackendPID();
                var publishing = java.util.concurrent.CompletableFuture.runAsync(
                        () -> JdbcCoordinatorWorkRecovery.publishRetired(dataSource, replacement));
                try {
                    await().atMost(10, SECONDS).until(() -> blockedBy(backend));
                    assertThat(publishing).isNotDone();
                } finally {
                    inFlight.commit();
                    publishing.get(10, SECONDS);
                }
                var claim = anchors.tryAcquire(owner.executionId(), "replacement",
                        replacement.identity().incarnationId(), lease).orElseThrow();
                assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(claim));
                assertTrue(anchors.releaseDrainedQuery(claim, false));
            } finally {
                replacement.closeAfterDrain(() -> {});
            }
        } finally {
            original.closeAfterDrain(() -> {});
        }
    }

    private boolean blockedBy(int backend) throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ? = ANY (pg_blocking_pids(pid)))
                """)) {
            statement.setInt(1, backend);
            try (var rows = statement.executeQuery()) {
                rows.next();
                return rows.getBoolean(1);
            }
        }
    }

    @Test
    void workerAndQueryFactsStayFencedThroughTakeoverAndPhysicalRetirement() throws Exception {
        var store = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var prior = store.tryAcquire("late-reports", "coordinator", "original-boot", lease).orElseThrow();
        var pending = acceptanceEvent(prior.executionId(), "alice", "a", "b", QueryAcceptance.DEFAULT_RETENTION_MS);
        journal.acceptFencedQuerySync(pending, prior);
        journal.appendTaskIssuancesSync(recoveryIssuances(prior, 0, 1));
        var authority = new TaskStateEvent.ReportAuthority(prior.ownerIncarnation(), prior.fencingToken());
        var current = new TaskStateEvent(JournalEventContext.create(111, prior.ownerId()), prior.executionId(),
                "task-0", "worker-id", "RUNNING", 1, 1, null, authority);
        journal.appendTaskEventSync(current);
        journal.appendTaskEventSync(current);
        assertEquals(current, storedTaskEvent(current.context().eventId()));

        assertTrue(store.yield(prior));
        var successor = store.tryAcquire(prior.executionId(), prior.ownerId(), "new-boot", lease).orElseThrow();
        assertFalse(journal.durableRetirement().orElseThrow().test(prior));
        assertEquals(current, storedTaskEvent(current.context().eventId()));
        var attempt = journal.pendingTaskDrains(successor, 0).get(0);
        assertTrue(journal.retainTaskDrains(successor, List.of(new DrainReceipt(attempt.rowId(), new byte[] {1}))));
        assertEquals(DrainOutcome.CONFIRMED, journal.confirmRecoveredTaskDrains(successor));
        assertTrue(store.releaseDrainedQuery(successor, false));

        journal.appendTaskEvent(new TaskStateEvent(JournalEventContext.create(112, prior.ownerId()),
                prior.executionId(), "task-0", "worker-id", "RUNNING", 1, 2, null, authority));
        var rejected = assertThrows(JournalWriteException.class, () -> journal.appendTaskEventSync(
                new TaskStateEvent(JournalEventContext.create(113, prior.ownerId()), prior.executionId(),
                        "task-0", "worker-id", "FINISHED", 1, 3, null, authority)));
        assertThat(rejected).hasMessageContaining("Task report execution grant is no longer current");
        journal.appendOwnedTerminalSync(new QueryStateEvent(JournalEventContext.create(Long.MAX_VALUE, prior.ownerId()),
                prior.executionId(), null, null, "FINISHED"), prior);
        assertEquals("FAILED", journal.latestQueryEvent(prior.executionId()).orElseThrow().state());
        assertEquals(prior.executionId(), journal.findAcceptance("alice", "a".repeat(64))
                .orElseThrow().receipt().queryId());
        assertEquals(0, countQueryRows("task_state_journal", prior.executionId()));
        assertEquals(Set.of(prior.executionId()), journal.observedRecoveryExecutions(Set.of(prior.executionId())));
    }

    private List<TaskStateEvent> recoveryIssuances(
            org.pangrova.execution.api.ownership.ExecutionAnchor anchor, int first, int count) {
        var issuance = new TaskIssuance("worker-host", 8081, anchor.ownerIncarnation(),
                anchor.fencingToken(), new byte[] {1, 2, 3});
        return java.util.stream.IntStream.range(first, first + count).mapToObj(partition ->
                new TaskStateEvent(JournalEventContext.create(105, anchor.ownerId()), anchor.executionId(),
                        "task-" + partition, "worker-id", "SCHEDULED", 1, 0, issuance)).toList();
    }

    @Test
    void latestPointLookupReturnsTheTerminalFact() {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(2000L, null), "q-1", null, null, "FINISHED"));

        assertEquals("FINISHED", journal.latestQueryEvent("q-1").orElseThrow().state());
    }

    @Test
    void purgeRemovesAllEventsForQuery() throws Exception {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendTaskEventSync(
                new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "t-1", "w-1", "RUNNING", 0, 1L));

        journal.purgeQuery("q-1");

        assertTrue(journal.latestQueryEvent("q-1").isEmpty());
        assertTrue(storedTasks("q-1").isEmpty());
    }

    @Test
    void purgeDoesNotAffectOtherQueries() {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-1", "SELECT 1", "{}", "QUEUED"));
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1100L, null), "q-2", "SELECT 2", "{}", "QUEUED"));

        journal.purgeQuery("q-1");

        assertTrue(journal.latestQueryEvent("q-1").isEmpty());
        assertEquals("q-2", journal.latestQueryEvent("q-2").orElseThrow().queryId());
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
                var failure = assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
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
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, () -> journal.purgeQuery("q-owned"));
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
    void asyncFlushBatchingBehavior() throws Exception {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-async", "SELECT 1", "{}", "QUEUED"));

        // Append async events
        for (int i = 0; i < 5; i++) {
            journal.appendTaskEvent(
                    new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L + i, null), "q-async", "t-" + i, "w-1", "RUNNING", 0, i));
        }

        // Wait for flush (config interval is 50ms, give it 2s)
        await().atMost(2, SECONDS).until(() -> countQueryRows("task_state_journal", "q-async") == 5);

        List<TaskStateEvent> events = storedTasks("q-async");
        assertEquals(5, events.size(), "All async events should be flushed");
    }

    @Test
    void closeFlushesRemainingEvents() throws Exception {
        journal.appendQueryEventSync(
                new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L, null), "q-close", "SELECT 1", "{}", "QUEUED"));

        // Append async events
        for (int i = 0; i < 3; i++) {
            journal.appendTaskEvent(
                    new TaskStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(1000L + i, null), "q-close", "t-" + i, "w-1", "RUNNING", 0, i));
        }

        // Close immediately (should flush remaining)
        journal.close();

        assertEquals(3, storedTasks("q-close").size(), "Close should flush remaining async events");
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

        assertEquals("RUNNING", journal.latestQueryEvent("q-1").orElseThrow().state());
        assertEquals("FAILED", journal.latestQueryEvent("q-2").orElseThrow().state());
        assertEquals("QUEUED", journal.latestQueryEvent("q-3").orElseThrow().state());
    }

    private List<TaskStateEvent> storedTasks(String queryId) throws SQLException {
        var events = new java.util.ArrayList<TaskStateEvent>();
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT " + JournalEventSql.TASK_COLUMNS + " FROM task_state_journal WHERE query_id = ? ORDER BY id")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) events.add(JournalEventSql.readTask(rows));
            }
        }
        return List.copyOf(events);
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
    void freshQueryAndTaskEventsNeedNoReadbackWhileExactRetriesDo() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            statement.execute("SELECT pg_stat_statements_reset()");
        }
        var query = new QueryStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(1234, "issuer"),
                "q-sql-count", "SELECT 1", "{}", "QUEUED");
        var task = new TaskStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(1235, "issuer"),
                "q-sql-count", "t-sql-count", "worker-1", "RUNNING", 0, 1L);

        journal.appendQueryEventSync(query);
        journal.appendTaskEventSync(task);
        assertEquals(0L, eventReadbacks());

        journal.appendQueryEventSync(query);
        journal.appendTaskEventSync(task);
        assertEquals(2L, eventReadbacks());
    }

    @Test
    void fencedAdmissionTakesOneStatementAndReplaysCommittedReceiptAfterTakeover() throws Exception {
        String queryId = "fenced-committed";
        var event = acceptanceEvent(queryId, "alice", "a", "b", 60_000);
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var old = anchors.tryAcquire(queryId, "old", "old-boot", lease).orElseThrow();
        executeSql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        executeSql("SELECT pg_stat_statements_reset()");

        assertTrue(journal.acceptFencedQuerySync(event, old).newExecution());
        assertEquals(1L, fencedAdmissionStatements());
        assertEquals(0L, acceptanceReadbacks());
        assertEquals(0L, eventReadbacks());

        assertTrue(anchors.yield(old));
        var successor = anchors.tryAcquire(queryId, "new", "new-boot", lease).orElseThrow();
        var replay = journal.acceptFencedQuerySync(event, old);
        assertFalse(replay.newExecution());
        assertEquals(event.acceptance(), replay.acceptance());
        assertThat(anchors.current(queryId)).contains(successor);
        assertEquals(1, countQueryRows("query_state_journal", queryId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void exactDurableReceiptExposesTerminalOutcomeOnlyAfterAnchorRetirement(boolean selectsBranch) {
        String queryId = "receipt-recovery";
        var owner = new QueryOwnerEndpoint("original", "original-boot", "localhost", 8080, "original-ticket");
        var request = new QueryAcceptanceRequest("alice", "a".repeat(64), "b".repeat(64), owner);
        var acceptance = QueryAcceptance.pending(request, queryId, 60_000);
        var context = JournalEventContext.create(System.currentTimeMillis(), owner.coordinatorId());
        var admission = new QueryStateEvent(context, queryId,
                selectsBranch ? "USE BRANCH dev ON lake.db.items" : "SELECT 1", "{}", "QUEUED", null, acceptance);
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var anchor = anchors.tryAcquire(queryId, owner.coordinatorId(), owner.incarnation(), Duration.ofMinutes(1))
                .orElseThrow();
        assertTrue(journal.acceptFencedQuerySync(admission, anchor).newExecution());
        assertThat(journal.recoverReceipt(queryId, "alice", owner.routeTicket()).orElseThrow().pending()).isTrue();
        assertThat(journal.recoverReceipt(queryId, "bob", owner.routeTicket())).isEmpty();
        assertThat(journal.recoverReceipt(queryId, "alice", "other-ticket")).isEmpty();

        var outcome = selectsBranch
                ? new QueryTerminalOutcome(ResultDisposition.NONE, null,
                        new org.pangrova.query.api.session.BranchSelection("lake.db.items", "dev"))
                : new QueryTerminalOutcome(ResultDisposition.UNAVAILABLE, null, null);
        journal.appendOwnedTerminalSync(new QueryStateEvent(
                JournalEventContext.create(System.currentTimeMillis(), owner.coordinatorId()),
                queryId, null, null, "FINISHED", null, null, outcome), anchor);
        assertThat(journal.latestQueryEvent(queryId).orElseThrow().terminalOutcome()).isEqualTo(outcome);
        assertThat(journal.recoverReceipt(queryId, "alice", owner.routeTicket()).orElseThrow().pending()).isTrue();
        assertTrue(journal.durableRetirement().orElseThrow().test(anchor));
        assertTrue(anchors.isQueryRetired(queryId));
        var recovered = journal.recoverReceipt(queryId, "alice", owner.routeTicket()).orElseThrow();
        assertThat(recovered.pending()).isFalse();
        assertThat(recovered.terminalState()).isEqualTo("FINISHED");
        assertThat(recovered.outcome()).isEqualTo(outcome);
    }

    @Test
    void oldOwnerCannotWriteAfterEmptyTakeoverClaimIsReleased() throws Exception {
        String queryId = "late-predecessor";
        var event = acceptanceEvent(queryId, "alice", "a", "b", 60_000);
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var lease = Duration.ofMinutes(1);
        var old = anchors.tryAcquire(queryId, "old", "old-boot", lease).orElseThrow();
        assertTrue(anchors.yield(old));
        var successor = anchors.tryAcquire(queryId, "new", "new-boot", lease).orElseThrow();
        assertTrue(anchors.release(successor));

        var keyed = assertThrows(JournalWriteException.class, () -> journal.acceptFencedQuerySync(event, old));
        assertEquals(JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER, keyed.admissionOutcome());
        var initial = new QueryStateEvent(JournalEventContext.create(100, "old"),
                queryId, "SELECT 1", "{}", "QUEUED");
        var unkeyed = assertThrows(JournalWriteException.class, () -> journal.appendFencedAdmissionSync(initial, old));
        assertEquals(JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER, unkeyed.admissionOutcome());
        assertEquals(0, countQueryRows("query_state_journal", queryId));
        assertTrue(journal.acceptQuerySync(acceptanceEvent("unrelated", "alice", "a", "b", 60_000)).newExecution());
    }

    @Test
    void fencedAdmissionSamplesExpiryAfterWaitingForTheAnchorLock() throws Exception {
        String id = "fenced-while-waiting";
        var event = acceptanceEvent(id, "alice", "a", "b", 60_000);
        var anchors = new JdbcExecutionAnchorStore(dataSource);
        var grant = anchors.tryAcquire(id, "old", "old-boot", Duration.ofSeconds(4)).orElseThrow();
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try (var held = dataSource.getConnection()) {
            held.setAutoCommit(false);
            try (var lock = held.prepareStatement("SELECT 1 FROM execution_anchor WHERE execution_id = ? FOR UPDATE")) {
                lock.setString(1, id);
                try (var rows = lock.executeQuery()) {
                    assertTrue(rows.next());
                }
            }
            var write = executor.submit(() -> journal.acceptFencedQuerySync(event, grant));
            await().atMost(10, SECONDS).until(this::fencedInsertWaitingOnLock);
            await().atMost(10, SECONDS).until(() -> anchors.current(id).isEmpty());
            held.commit();

            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> write.get(5, SECONDS));
            var rejected = assertInstanceOf(JournalWriteException.class, failure.getCause());
            assertEquals(JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER, rejected.admissionOutcome());
            assertEquals(0, countQueryRows("query_state_journal", id));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, SECONDS));
        }
    }

    private boolean fencedInsertWaitingOnLock() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE wait_event_type = 'Lock' AND query LIKE 'WITH locked AS MATERIALIZED (%'
                        )
                        """)) {
            return rows.next() && rows.getBoolean(1);
        }
    }

    private long fencedAdmissionStatements() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements
                        WHERE query LIKE 'WITH locked AS MATERIALIZED (%INSERT INTO query_state_journal%'
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    @Test
    void durableAcceptanceReplaysAcrossJournalInstancesAndIsolatesPrincipals() throws Exception {
        var first = acceptanceEvent("q-accept", "alice", "a", "b", 60_000);
        var admission = journal.acceptQuerySync(first);
        assertThat(admission.newExecution()).isTrue();
        journal.close();

        journal = new JdbcQueryStateJournal(dataSource,
                new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 50, 20, 24, 50_000));
        var replay = acceptanceEvent("q-replay", "alice", "a", "b", 60_000);
        var replayed = journal.acceptQuerySync(replay);
        assertThat(replayed.newExecution()).isFalse();
        assertThat(replayed.acceptance().receipt().queryId()).isEqualTo("q-accept");
        assertThat(replayed.acceptance().receipt()).isEqualTo(admission.acceptance().receipt());
        assertThat(countQueryRows("query_state_journal", "q-accept")).isEqualTo(1);
        assertThat(countQueryRows("query_state_journal", "q-replay")).isEqualTo(0);

        var otherPrincipal = acceptanceEvent("q-bob", "bob", "a", "b", 60_000);
        assertThat(journal.acceptQuerySync(otherPrincipal).newExecution()).isTrue();
        assertThat(countQueryRows("query_state_journal", "q-bob")).isEqualTo(1);
    }

    @Test
    void acceptanceFingerprintConflictIsRejectedWithoutASecondQueryRow() throws Exception {
        journal.acceptQuerySync(acceptanceEvent("q-accept", "alice", "a", "b", 60_000));
        var failure = assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                () -> journal.acceptQuerySync(acceptanceEvent("q-conflict", "alice", "a", "c", 60_000)));
        assertThat(failure.admissionOutcome()).isEqualTo(
                org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER);
        assertThat(countQueryRows("query_state_journal", "q-accept")).isEqualTo(1);
        assertThat(countQueryRows("query_state_journal", "q-conflict")).isEqualTo(0);
        assertThat(journal.findAcceptance("alice", "a".repeat(64))).isPresent();
        assertThat(journal.acceptQuerySync(acceptanceEvent("q-healthy", "alice", "d", "b", 60_000))
                .newExecution()).isTrue();
    }

    @Test
    void activeAcceptanceDoesNotExpireBeforeTerminalCompletion() throws Exception {
        journal.acceptQuerySync(acceptanceEvent("q-active-accept", "alice", "a", "b", 1));
        Thread.sleep(20);
        var replay = journal.acceptQuerySync(acceptanceEvent("q-active-retry", "alice", "a", "b", 1));
        assertThat(replay.newExecution()).isFalse();
        assertThat(replay.acceptance().receipt().queryId()).isEqualTo("q-active-accept");
        journal.purgeExpiredAcceptances();
        assertThat(countQueryRows("query_state_journal", "q-active-accept")).isEqualTo(1);
        assertThat(countQueryRows("query_state_journal", "q-active-retry")).isEqualTo(0);
    }

    @Test
    void expiredTerminalAcceptanceAllowsNewFingerprintBeforeSweep() throws Exception {
        var original = acceptanceEvent("q-old", "alice", "a", "b", 60_000);
        assertThat(journal.acceptQuerySync(original).newExecution()).isTrue();
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                () -> journal.acceptQuerySync(acceptanceEvent("q-active-conflict", "alice", "a", "c", 60_000)));
        journal.appendQueryEventSync(new QueryStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(System.currentTimeMillis(), "issuer"),
                "q-old", null, null, "FINISHED"));
        assertThat(journal.acceptQuerySync(original).acceptance()).isEqualTo(original.acceptance());
        executeSql("UPDATE query_state_journal SET timestamp_ms = timestamp_ms - 120000 WHERE query_id = 'q-old'");
        assertThat(journal.findAcceptance("alice", "a".repeat(64))).isEmpty();
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                () -> journal.acceptQuerySync(acceptanceEvent("q-old", "alice", "a", "c", 60_000)));
        var replacement = acceptanceEvent("q-new", "alice", "a", "c", 60_000);
        var admitted = journal.acceptQuerySync(replacement);
        assertThat(admitted.newExecution()).isTrue();
        assertThat(admitted.acceptance()).isEqualTo(replacement.acceptance());
        assertThat(countQueryRows("query_state_journal", "q-old")).isZero();
        assertThat(journal.findAcceptance("alice", "a".repeat(64))).contains(replacement.acceptance());
        var replay = journal.acceptQuerySync(acceptanceEvent("q-replay", "alice", "a", "c", 60_000));
        assertThat(replay.newExecution()).isFalse();
        assertThat(replay.acceptance()).isEqualTo(replacement.acceptance());
    }

    @Test
    void expiredAcceptanceSweepRemovesTerminalFacts() throws Exception {
        journal.acceptQuerySync(acceptanceEvent("q-sweep", "alice", "a", "b", 1));
        journal.appendQueryEventSync(new QueryStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(System.currentTimeMillis(), "issuer"),
                "q-sweep", null, null, "FINISHED"));
        executeSql("UPDATE query_state_journal SET timestamp_ms = timestamp_ms - 120000 WHERE query_id = 'q-sweep'");
        journal.purgeExpiredAcceptances();
        assertThat(countQueryRows("query_state_journal", "q-sweep")).isEqualTo(0);
    }

    @Test
    void terminalAcceptanceAndTaskFactsSurviveExpiryDuringOwnerTakeover() throws Exception {
        String originalId = "q-undrained";
        var original = acceptanceEvent(originalId, "alice", "a", "b", 60_000);
        assertThat(journal.acceptQuerySync(original).newExecution()).isTrue();
        var task = new TaskStateEvent(JournalEventContext.create(100, "original-owner"), originalId,
                "task-undrained", "worker", "RUNNING", 0, 1);
        journal.appendTaskEventSync(task);
        journal.appendQueryEventSync(new QueryStateEvent(
                JournalEventContext.create(System.currentTimeMillis(), "original-owner"),
                originalId, null, null, "FAILED"));
        executeSql("UPDATE query_state_journal SET timestamp_ms = timestamp_ms - 120000 "
                + "WHERE query_id = 'q-undrained'");
        var anchors = new org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore(dataSource);
        var duration = java.time.Duration.ofMinutes(1);
        var previous = anchors.tryAcquire(originalId, "original-owner", "previous", duration).orElseThrow();
        assertTrue(anchors.yield(previous));

        journal.purgeExpiredAcceptances();
        assertThat(journal.acceptQuerySync(acceptanceEvent("q-retry", "alice", "a", "b", 60_000)).acceptance())
                .isEqualTo(original.acceptance());
        assertThat(countQueryRows("query_state_journal", originalId)).isEqualTo(2);
        assertThat(countQueryRows("task_state_journal", originalId)).isEqualTo(1);

        var recovered = anchors.tryAcquire(originalId, "new-owner", "next", duration).orElseThrow();
        assertTrue(anchors.release(recovered));
        journal.purgeExpiredAcceptances();
        assertThat(countQueryRows("query_state_journal", originalId)).isZero();
        assertThat(countQueryRows("task_state_journal", originalId)).isZero();
    }

    @Test
    void replayDoesNotPublishLineageForTheCandidateQuery() throws Exception {
        var original = acceptanceEvent("q-lineage-original", "alice", "a", "b", 60_000);
        var lineage = new QueryLineageFact("test", original.queryId(), LineageEventType.START,
                original.context().timestampMs(), original.sql(), null, QueryLineageMetadata.empty());
        journal.acceptQuerySync(new QueryStateEvent(original.context(), original.queryId(), original.sql(),
                original.sessionJson(), original.state(), lineage, original.acceptance()));
        var replay = journal.acceptQuerySync(acceptanceEvent("q-lineage-replay", "alice", "a", "b", 60_000));
        assertThat(replay.newExecution()).isFalse();
        assertThat(lineageRows("q-lineage-original")).isEqualTo(1);
        assertThat(lineageRows("q-lineage-replay")).isZero();
    }

    @Test
    void acceptanceShapeRequiresAllFieldsOnTheInitialQueuedRow() throws Exception {
        journal.acceptQuerySync(acceptanceEvent("q-shape", "alice", "a", "b", 60_000));
        for (String assignment : List.of("principal = NULL", "idempotency_key_digest = NULL",
                "request_fingerprint = NULL", "acceptance_receipt_json = NULL", "retention_ms = NULL",
                "retention_ms = 0", "state = 'RUNNING'")) {
            var failure = assertThrows(java.sql.SQLException.class, () -> executeSql(
                    "UPDATE query_state_journal SET " + assignment + " WHERE query_id = 'q-shape'"));
            assertThat(failure.getSQLState()).isEqualTo("23514");
        }
        assertThat(journal.findAcceptance("alice", "a".repeat(64))).isPresent();
    }

    @Test
    void freshAcceptanceUsesOneInsertAndNoReadback() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            statement.execute("SELECT pg_stat_statements_reset()");
        }
        journal.acceptQuerySync(acceptanceEvent("q-one-write", "alice", "a", "b", 60_000));
        assertThat(acceptanceInserts()).isEqualTo(1);
        assertThat(acceptanceReadbacks()).isEqualTo(0);
    }

    @Test
    void acceptanceSurvivesPurgeUntilTerminalRetentionExpires() throws Exception {
        journal.acceptQuerySync(acceptanceEvent("q-retained", "alice", "a", "b", 60_000));
        journal.appendQueryEventSync(new QueryStateEvent(
                org.pangrova.query.api.journal.JournalEventContext.create(System.currentTimeMillis() + 1, "issuer"),
                "q-retained", null, null, "FINISHED"));
        journal.purgeQuery("q-retained");
        assertThat(countQueryRows("query_state_journal", "q-retained")).isEqualTo(2);

        executeSql("UPDATE query_state_journal SET timestamp_ms = timestamp_ms - 120000 "
                + "WHERE query_id = 'q-retained'");
        executeSql("UPDATE query_state_journal SET retention_ms = 1 WHERE query_id = 'q-retained' "
                + "AND idempotency_key_digest IS NOT NULL");
        journal.purgeQuery("q-retained");
        assertThat(countQueryRows("query_state_journal", "q-retained")).isEqualTo(0);
    }

    @Test
    void lostAcceptanceCommitResponseReconcilesTheOriginalReceipt() throws Exception {
        journal.close();
        var wire = new PostgresCommitWireFault(POSTGRES.getHost(), POSTGRES.getMappedPort(5432));
        try (wire) {
            var source = new org.postgresql.ds.PGSimpleDataSource();
            source.setServerNames(new String[] {java.net.InetAddress.getLoopbackAddress().getHostAddress()});
            source.setPortNumbers(new int[] {wire.port()});
            source.setDatabaseName(POSTGRES.getDatabaseName());
            source.setUser(POSTGRES.getUsername());
            source.setPassword(POSTGRES.getPassword());
            source.setSslMode("disable");
            source.setGssEncMode("disable");
            journal = new JdbcQueryStateJournal(source,
                    new StateJournalConfig("JDBC", source.getURL(), 20, 1, 24, 1));
            var event = acceptanceEvent("q-lost-accept", "alice", "a", "b", 60_000);
            wire.armCommitResponseLoss();
            var failure = assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                    () -> journal.acceptQuerySync(event));
            assertEquals(org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED,
                    failure.admissionOutcome());
            assertTrue(wire.awaitActualCommit());
            long originalRow = eventRowId(event.context().eventId());
            wire.allowReconciliation();
            journal.close();
            try (var reader = new JdbcQueryStateJournal(dataSource,
                    new StateJournalConfig("JDBC", POSTGRES.getJdbcUrl(), 20, 1, 24, 1))) {
                assertThat(reader.findAcceptance("alice", "a".repeat(64)).orElseThrow().receipt().queryId())
                        .isEqualTo("q-lost-accept");
            }
            assertThat(countQueryRows("query_state_journal", "q-lost-accept")).isEqualTo(1);
            assertThat(eventRowId(event.context().eventId())).isEqualTo(originalRow);
        } finally {
            wire.allowReconciliation();
            journal.close();
        }
    }

    private static QueryStateEvent acceptanceEvent(String queryId, String principal, String keyDigit,
            String fingerprintDigit, long retentionMs) {
        var request = new QueryAcceptanceRequest(principal, keyDigit.repeat(64), fingerprintDigit.repeat(64));
        var acceptance = QueryAcceptance.pending(request, queryId, retentionMs);
        return new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(
                System.currentTimeMillis(), "issuer"), queryId, "SELECT 1", "{}", "QUEUED", null, acceptance);
    }

    private long lineageRows(String queryId) throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT count(*) FROM query_lineage_outbox WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    private long acceptanceInserts() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements
                        WHERE query LIKE 'INSERT INTO query_state_journal%'
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private long acceptanceReadbacks() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements
                        WHERE query LIKE 'SELECT event_id,% FROM query_state_journal WHERE principal = $%'
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private long eventReadbacks() throws Exception {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("""
                        SELECT COALESCE(SUM(calls), 0) FROM pg_stat_statements
                        WHERE query LIKE 'SELECT event_id,% FROM query_state_journal WHERE event_id = $%'
                           OR query LIKE 'SELECT event_id,% FROM task_state_journal WHERE event_id = $%'
                        """)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    @Test
    void changedPayloadCannotReplaceCommittedEventAndStickyFailureCannotMasqueradeAsCloseSuccess() throws Exception {
        var context = org.pangrova.query.api.journal.JournalEventContext.create(456, "issuer");
        var event = new QueryStateEvent(context, "q-conflict", "SELECT 1", "{}", "QUEUED");
        journal.appendQueryEventSync(event);
        var changed = new QueryStateEvent(context, "q-conflict", "SELECT 2", "{}", "QUEUED");
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                () -> journal.appendQueryEventSync(changed));
        stickyConflict = true;
        assertEquals(event, journal.latestQueryEvent("q-conflict").orElseThrow());
        assertEquals(1, countQueryRows("query_state_journal", "q-conflict"));
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                () -> journal.appendQueryEvent(new QueryStateEvent(
                        org.pangrova.query.api.journal.JournalEventContext.create(999, "issuer"),
                        "q-after-conflict", "SELECT 1", "{}", "QUEUED")));
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, journal::close);
        assertTrue(journal.isDrained(), "Actual writer and SQL owners have exited, not a successful commit claim");
        assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, journal::close);
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
            assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
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
            var failure = assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                    () -> journal.appendQueryEventSync(denied));
            assertEquals(org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED,
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
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
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
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                    () -> journal.appendQueryEventSync(denied)));
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                    () -> journal.appendQueryEventSync(denied)));
            // Exact retries join one resident owner; the second distinct event still fits.
            journal.appendTaskEvent(later);
            var overflow = new QueryStateEvent(org.pangrova.query.api.journal.JournalEventContext.create(812, "issuer"),
                    "q-overflow", "SELECT 44", "{}", "QUEUED");
            var rejected = assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                    () -> journal.appendQueryEvent(overflow));
            assertEquals(org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER,
                    rejected.admissionOutcome());
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                    () -> journal.purgeQuery(denied.queryId())));
            requireQueryIsolationFault(assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, journal::close));
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
            assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, journal::close);
            assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
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
            stmt.execute("DELETE FROM execution_anchor");
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
                var failed = assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class,
                        () -> journal.appendQueryEventSync(event));
                assertEquals(org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED,
                        failed.admissionOutcome());
                assertTrue(wire.awaitActualCommit());
                assertEquals(1, countQueryRows("query_state_journal", event.queryId()));
                long originalRow = eventRowId(event.context().eventId());
                Thread.currentThread().interrupt();
                try {
                    assertThrows(org.pangrova.persistence.query.api.exception.JournalWriteException.class, journal::close);
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
