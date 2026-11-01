package org.pangrova.coordinator.query.state;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.query.store.jdbc.journal.JdbcQueryStateJournal;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.client.core.error.ErrorDetail;
import org.pangrova.spi.query.state.QueryStateEvent;
import org.pangrova.spi.query.state.StateJournalConfig;
import org.pangrova.spi.query.state.TaskStateEvent;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.testing.testkit.TestObservability;
import static org.assertj.core.api.Assertions.assertThat;

/** Real PostgreSQL recovery projection; journal history is not a Native exit authority. */
@Tag("integration")
class JournalRecoveryServiceTest {
    @Test
    void recoverQueriesRebuildsStateMachines() throws Exception {
        try (var fixture = new RecoveryFixture(false)) {
            fixture.query("q-1", "RUNNING");
            fixture.query("q-2", "SCHEDULING");
            assertThat(fixture.recovery.recover()).isEqualTo(2);
            assertThat(fixture.manager.getQueryState("q-1")).contains(QueryStateMachine.State.RUNNING);
            assertThat(fixture.manager.getQueryState("q-2")).contains(QueryStateMachine.State.SCHEDULING);
        }
    }

    @Test
    void recoverTasksRebuildsTracking() throws Exception {
        try (var fixture = new RecoveryFixture(false)) {
            fixture.query("q-1", "RUNNING");
            fixture.task("q-1", "t-1", "w-1", "RUNNING", 5L);
            fixture.task("q-1", "t-2", "w-2", "SCHEDULED", 1L);
            assertThat(fixture.recovery.recover()).isEqualTo(1);
            assertThat(fixture.tracker.getWorkerForTask("t-1")).isEqualTo("w-1");
            assertThat(fixture.tracker.getWorkerForTask("t-2")).isEqualTo("w-2");
            assertThat(fixture.tracker.getTaskState("q-1", "t-1"))
                    .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.RUNNING);
            assertThat(fixture.tracker.getTaskState("q-1", "t-2"))
                    .isEqualTo(org.pangrova.coordinator.task.TaskStateMachine.State.SCHEDULED);
        }
    }

    @Test
    void recoverEmptyJournalReturnsZero() throws Exception {
        try (var fixture = new RecoveryFixture(false)) {
            assertThat(fixture.recovery.recover()).isZero();
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
        }
    }

    @Test
    void recoverDoesNotReplayAlreadyFailedQuery() throws Exception {
        try (var fixture = new RecoveryFixture(false)) {
            fixture.query("q-fail", "RUNNING");
            fixture.query("q-fail", "FAILED");
            assertThat(fixture.journal.replayActiveQueries()).isEmpty();
            assertThat(fixture.recovery.recover()).isZero();
            assertThat(fixture.manager.getQueryState("q-fail")).isEmpty();
            assertThat(fixture.manager.getQueryStateWithJournalFallback("q-fail"))
                    .contains(QueryStateMachine.State.FAILED);
            assertThat(fixture.manager.getStateMachine("q-fail")).isNull();
            assertThat(fixture.queryRows("q-fail")).isEqualTo(2);
        }
    }

    @Test
    void unreachableWorkerIsNotDrainAndCannotPurgeRecoveredRows() throws Exception {
        try (var fixture = new RecoveryFixture(true)) {
            fixture.query("q-unknown", "RUNNING");
            fixture.task("q-unknown", "t-unknown", "missing-worker", "RUNNING", 1L);
            assertThat(fixture.recovery.recover()).isZero(); // One recovered, one logically failed.
            assertThat(fixture.manager.getQueryState("q-unknown")).contains(QueryStateMachine.State.FAILED);
            assertThat(fixture.manager.getStateMachine("q-unknown").getError())
                    .hasValueSatisfying(error -> assertThat(error.errorCode()).isEqualTo("RECOVERY_WORKER_DEAD"));
            assertThat(fixture.manager.awaitAllQueries(1)).isFalse();
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.queryRows("q-unknown")).isGreaterThanOrEqualTo(2);
            assertThat(fixture.taskRows("q-unknown")).isEqualTo(1);
        }
    }

    @Test
    void noRecoveredTaskRowsCannotProveNoNativeOwner() throws Exception {
        try (var fixture = new RecoveryFixture(false)) {
            fixture.query("q-no-task", "RUNNING");
            assertThat(fixture.recovery.recover()).isEqualTo(1);
            fixture.manager.onTaskFailed("q-no-task", new ErrorDetail("RECOVERY_UNRESOLVED",
                    "ownership", "Journal history does not contain exact issued attempts"));
            assertThat(fixture.manager.getQueryState("q-no-task")).contains(QueryStateMachine.State.FAILED);
            assertThat(fixture.taskRows("q-no-task")).isZero();
            assertThat(fixture.manager.awaitAllQueries(1)).isFalse();
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.queryRows("q-no-task")).isGreaterThanOrEqualTo(2);
        }
    }

    private static final class RecoveryFixture implements AutoCloseable {
        private final DataSource source = SharedPostgresContainer.migratedDataSource("recovery_history");
        private final JdbcQueryStateJournal journal = new JdbcQueryStateJournal(source,
                new StateJournalConfig("JDBC", SharedPostgresContainer.instance().getJdbcUrl(), 20, 20, 24, 50_000));
        private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        private final RemoteTaskDispatcher transport =
                new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        private final QueryManager manager;
        private final JournalRecoveryService recovery;

        private RecoveryFixture(boolean checkWorkers) throws SQLException {
            var workers = new WorkerManager(5000, 15000, new InMemoryWorkerRegistry());
            var dispatcher = new QueryDispatcher(QueryDispatcherContext.withDefaults(new SqlCompiler(), null, null,
                    null, transport, new TaskScheduler(new WorkerPerformanceScorer()), workers, tracker,
                    null, null, null, 0L), null, null);
            manager = new QueryManager(new QueryManagerContext(dispatcher, TestObservability.TRACING,
                    null, null, null, null, null, null, null, null, journal));
            recovery = new JournalRecoveryService(journal, manager, tracker, checkWorkers ? workers : null);
        }

        private void query(String query, String state) {
            journal.appendQueryEventSync(new QueryStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), query, "SELECT 1", "{}", state));
        }

        private void task(String query, String task, String worker, String state, long sequence) {
            journal.appendTaskEventSync(new TaskStateEvent(org.pangrova.spi.query.state.JournalEventContext.create(System.currentTimeMillis(), null), query, task, worker, state, 0, sequence));
        }

        private int queryRows(String query) throws SQLException { return count("query_state_journal", query); }
        private int taskRows(String query) throws SQLException { return count("task_state_journal", query); }

        private int count(String table, String query) throws SQLException {
            try (var connection = source.getConnection();
                    var statement = connection.prepareStatement("SELECT count(*) FROM " + table + " WHERE query_id = ?")) {
                statement.setString(1, query);
                try (var rows = statement.executeQuery()) { rows.next(); return rows.getInt(1); }
            }
        }

        @Override
        public void close() {
            // These are projections of unknown historical attempts, not live tasks created by this fixture.
            // Their unresolved physical scope must NOT be acknowledged or force-purged at test teardown.
            journal.close();
            transport.close();
        }
    }
}
