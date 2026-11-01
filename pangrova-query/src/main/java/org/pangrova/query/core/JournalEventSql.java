package org.pangrova.query.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.pangrova.spi.coordination.JournalEventContext;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.TaskStateEvent;

/** Single SQL encoding and exact event reconciliation for the two authoritative journal tables. */
final class JournalEventSql {
    static final String QUERY_COLUMNS =
            "event_id, query_id, sql_text, session_json, state, timestamp_ms, coordinator_id, lineage_payload";
    static final String TASK_COLUMNS =
            "event_id, query_id, task_id, worker_id, state, stage_id, report_sequence, timestamp_ms, coordinator_id";
    private static final String QUERY_INSERT = "INSERT INTO query_state_journal (" + QUERY_COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT (event_id) DO NOTHING";
    private static final String TASK_INSERT = "INSERT INTO task_state_journal (" + TASK_COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING";

    private JournalEventSql() { }

    static void commit(Connection connection, List<JournalPendingWrites.Write> batch) throws SQLException {
        connection.setAutoCommit(false);
        try {
            for (JournalPendingWrites.Write event : batch) write(connection, event);
            connection.commit();
        } catch (SQLException failure) {
            try {
                connection.rollback();
                if (failure instanceof PayloadConflict conflict) conflict.rollbackConfirmed = true;
            } catch (SQLException rollback) {
                if (rollback != failure) failure.addSuppressed(rollback);
            }
            throw failure;
        }
    }

    private static void write(Connection connection, JournalPendingWrites.Write event) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                event.query != null ? QUERY_INSERT : TASK_INSERT)) {
            if (event.query != null) bindQuery(statement, event.query);
            else bindTask(statement, event.task);
            statement.executeUpdate();
        }
        if (event.query != null && event.query.lineage() != null) {
            try {
                QueryLineageFactSql.append(connection, event.query.lineage());
            } catch (QueryLineageFactSql.PayloadConflict conflict) {
                var rejected = new PayloadConflict(event.key());
                rejected.addSuppressed(conflict);
                throw rejected;
            }
        }
        String table = event.query != null ? "query_state_journal" : "task_state_journal";
        String columns = event.query != null ? QUERY_COLUMNS : TASK_COLUMNS;
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT " + columns + " FROM " + table + " WHERE event_id = ?")) {
            select.setObject(1, event.context().eventId());
            try (ResultSet rows = select.executeQuery()) {
                boolean exact = rows.next() && (event.query != null
                        ? event.query.equals(readQuery(rows)) : event.task.equals(readTask(rows)));
                if (!exact || rows.next()) throw new PayloadConflict(event.key());
            }
        }
    }

    private static void bindQuery(PreparedStatement statement, QueryStateEvent event) throws SQLException {
        statement.setObject(1, event.context().eventId());
        statement.setString(2, event.queryId());
        statement.setString(3, event.sql());
        statement.setString(4, event.sessionJson());
        statement.setString(5, event.state());
        statement.setLong(6, event.context().timestampMs());
        statement.setString(7, event.context().coordinatorId());
        statement.setString(8, event.lineage() == null ? null : QueryLineageFactSql.encode(event.lineage()));
    }

    private static void bindTask(PreparedStatement statement, TaskStateEvent event) throws SQLException {
        statement.setObject(1, event.context().eventId());
        statement.setString(2, event.queryId());
        statement.setString(3, event.taskId());
        statement.setString(4, event.workerId());
        statement.setString(5, event.state());
        statement.setInt(6, event.stageId());
        statement.setLong(7, event.reportSequence());
        statement.setLong(8, event.context().timestampMs());
        statement.setString(9, event.context().coordinatorId());
    }

    static QueryStateEvent readQuery(ResultSet rows) throws SQLException {
        return new QueryStateEvent(context(rows), rows.getString("query_id"), rows.getString("sql_text"),
                rows.getString("session_json"), rows.getString("state"),
                rows.getString("lineage_payload") == null ? null
                        : QueryLineageFactSql.decode(rows.getString("lineage_payload")));
    }

    static TaskStateEvent readTask(ResultSet rows) throws SQLException {
        return new TaskStateEvent(context(rows), rows.getString("query_id"), rows.getString("task_id"),
                rows.getString("worker_id"), rows.getString("state"), rows.getInt("stage_id"),
                rows.getLong("report_sequence"));
    }

    private static JournalEventContext context(ResultSet rows) throws SQLException {
        return new JournalEventContext(rows.getObject("event_id", UUID.class), rows.getLong("timestamp_ms"),
                rows.getString("coordinator_id"));
    }

    static final class PayloadConflict extends SQLException {
        private static final long serialVersionUID = 1L;
        final JournalPendingWrites.Key eventId;
        boolean rollbackConfirmed;

        PayloadConflict(JournalPendingWrites.Key eventId) {
            super("Journal event ID has a different durable payload: " + eventId, "23505");
            this.eventId = eventId;
        }
    }
}
