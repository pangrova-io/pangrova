package org.pangrova.persistence.query.postgresql.state;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.query.api.journal.TaskStateEvent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/** Persists a bounded same-owner cohort in the existing task journal before HTTP dispatch. */
final class JournalTaskIssuanceSql {
    private static final int FENCE_PARAMETERS = 4;
    private static final String TASK_ROW = "(?::uuid, ?::text, ?::text, ?::text, ?::text, ?::int, "
            + "?::bigint, ?::bigint, ?::text, ?::text, ?::int, ?::text, ?::bigint, ?::bytea)";

    private JournalTaskIssuanceSql() { }

    static void commit(Connection connection, List<TaskStateEvent> cohort) throws SQLException {
        connection.setAutoCommit(false);
        try {
            insert(connection, cohort);
            connection.commit();
        } catch (SQLException | RuntimeException failure) {
            try {
                connection.rollback();
            } catch (SQLException rollback) {
                failure.addSuppressed(rollback);
            }
            throw failure;
        }
    }

    private static void insert(Connection connection, List<TaskStateEvent> cohort) throws SQLException {
        var first = cohort.get(0);
        var inserted = new HashSet<UUID>();
        try (var statement = connection.prepareStatement(insertSql(cohort.size()))) {
            statement.setString(1, first.queryId());
            statement.setString(2, first.context().coordinatorId());
            statement.setString(3, first.issuance().ownerIncarnation());
            statement.setLong(FENCE_PARAMETERS, first.issuance().fencingToken());
            int offset = FENCE_PARAMETERS;
            for (var event : cohort) {
                JournalEventSql.bindTask(statement, event, offset);
                offset += JournalEventSql.TASK_PARAMETERS;
            }
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Task issuance returned no outcome");
                if (!rows.getBoolean("authorized")) {
                    throw new JournalWriteException("Task issuance execution grant is no longer current", null,
                            JournalAdmissionOutcome.REJECTED_BEFORE_TRANSFER);
                }
                do {
                    UUID eventId = rows.getObject("event_id", UUID.class);
                    if (eventId != null) inserted.add(eventId);
                } while (rows.next());
            }
        }
        for (var event : cohort) {
            if (!inserted.contains(event.context().eventId())) JournalEventSql.reconcileTask(connection, event);
        }
    }

    private static String insertSql(int count) {
        return """
                WITH locked AS MATERIALIZED (
                    SELECT expires_at FROM execution_anchor
                    WHERE execution_id = ? AND owner_id = ? AND owner_incarnation = ? AND fencing_token = ?
                    FOR SHARE
                ), authorized AS MATERIALIZED (
                    SELECT 1 FROM locked WHERE expires_at > clock_timestamp()
                ), issued (
                """ + JournalEventSql.TASK_COLUMNS + ") AS (VALUES " + String.join(", ",
                Collections.nCopies(count, TASK_ROW)) + """
                ), inserted AS (
                    INSERT INTO task_state_journal (
                """ + JournalEventSql.TASK_COLUMNS + """
                    )
                    SELECT issued.* FROM issued CROSS JOIN authorized
                    ON CONFLICT (event_id) DO NOTHING RETURNING event_id
                )
                SELECT EXISTS (SELECT 1 FROM authorized) AS authorized, inserted.event_id
                FROM (VALUES (1)) AS result(dummy) LEFT JOIN inserted ON true
                """;
    }
}
