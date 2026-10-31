package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.query.store.jdbc.journal.JdbcQueryStateJournal;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.query.api.state.StateJournalConfig;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/** Real task insert locks and committed observations remain observable after normal query journal purge. */
final class TaskJournalStoreFixture implements AutoCloseable {
    private final DataSource administrator;
    private final String application = "native_task_journal_" + UUID.randomUUID();
    private final JdbcQueryStateJournal journal;

    TaskJournalStoreFixture() throws SQLException {
        administrator = SharedPostgresContainer.migratedDataSource("actual_task_journal");
        String schema;
        try (var connection = administrator.getConnection(); var statement = connection.createStatement()) {
            schema = connection.getSchema();
            statement.execute("CREATE TABLE task_insert_observations (LIKE task_state_journal)");
            statement.execute("CREATE FUNCTION observe_task_insert() RETURNS trigger LANGUAGE plpgsql AS $$"
                    + "BEGIN INSERT INTO " + schema + ".task_insert_observations SELECT NEW.*; RETURN NEW; END $$");
            statement.execute("CREATE TRIGGER observe_actual_task_insert AFTER INSERT ON task_state_journal "
                    + "FOR EACH ROW EXECUTE FUNCTION observe_task_insert()");
        }
        var source = new PGSimpleDataSource();
        var postgres = SharedPostgresContainer.instance();
        source.setURL(postgres.getJdbcUrl());
        source.setUser(postgres.getUsername());
        source.setPassword(postgres.getPassword());
        source.setCurrentSchema(schema);
        source.setApplicationName(application);
        journal = new JdbcQueryStateJournal(source, new StateJournalConfig("JDBC", source.getURL(), 20, 20, 24, 50_000));
    }

    JdbcQueryStateJournal journal() { return journal; }

    Connection blockTaskInserts() throws SQLException {
        var connection = administrator.getConnection();
        try {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) { statement.execute("LOCK TABLE task_state_journal IN ACCESS EXCLUSIVE MODE"); }
            return connection;
        } catch (SQLException | RuntimeException | Error failure) {
            try { connection.close(); } catch (SQLException cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    boolean actualInsertWaiting() throws SQLException {
        try (var connection = administrator.getConnection(); var statement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = ? "
                        + "AND wait_event_type = 'Lock' AND query LIKE 'INSERT INTO task_state_journal%')")) {
            statement.setString(1, application);
            try (var result = statement.executeQuery()) { result.next(); return result.getBoolean(1); }
        }
    }

    int committed(TaskStatusReport report, org.pangrova.protocol.v2.TaskAttemptId issuedAttempt) throws SQLException {
        var context = org.pangrova.coordinator.task.TaskJournalEventContext.create(
                issuedAttempt, report.reportSequence(), report.timestampMs());
        try (var connection = administrator.getConnection(); var statement = connection.prepareStatement(
                "SELECT count(*) FROM task_insert_observations WHERE task_id=? AND state=? AND report_sequence=? "
                        + "AND timestamp_ms=? AND event_id=? AND coordinator_id=? AND query_id=? AND worker_id=? AND stage_id=?")) {
            statement.setString(1, report.taskId());
            statement.setString(2, report.state());
            statement.setLong(3, report.reportSequence());
            statement.setLong(4, report.timestampMs());
            statement.setObject(5, context.eventId());
            statement.setString(6, context.coordinatorId());
            statement.setString(7, report.queryId());
            statement.setString(8, report.workerId());
            statement.setInt(9, report.stageId());
            try (var result = statement.executeQuery()) { result.next(); return result.getInt(1); }
        }
    }

    @Override public void close() { journal.close(); }
}
