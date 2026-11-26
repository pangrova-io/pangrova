package org.pangrova.persistence.query.postgresql.state;

import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.query.api.journal.TaskStateEvent;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/** Persists a bounded same-owner cohort in the existing task journal before HTTP dispatch. */
final class JournalTaskIssuanceSql {
    private static final int FENCE_PARAMETERS = 4;
    private static final int COHORT_SIZE_11 = 11;
    private static final int COHORT_SIZE_12 = 12;
    private static final int COHORT_SIZE_13 = 13;
    private static final int COHORT_SIZE_14 = 14;
    private static final int COHORT_SIZE_15 = 15;
    private static final String TASK_ROW = "(?::uuid, ?::text, ?::text, ?::text, ?::text, ?::int, "
            + "?::bigint, ?::bigint, ?::text, ?::text, ?::int, ?::text, ?::bigint, ?::bytea)";

    // Each bounded cohort has a compile-time statement shape and binds every field.
    private static final String TASK_ROWS_2 = TASK_ROW + ", " + TASK_ROW;
    private static final String TASK_ROWS_3 = TASK_ROWS_2 + ", " + TASK_ROW;
    private static final String TASK_ROWS_4 = TASK_ROWS_3 + ", " + TASK_ROW;
    private static final String TASK_ROWS_5 = TASK_ROWS_4 + ", " + TASK_ROW;
    private static final String TASK_ROWS_6 = TASK_ROWS_5 + ", " + TASK_ROW;
    private static final String TASK_ROWS_7 = TASK_ROWS_6 + ", " + TASK_ROW;
    private static final String TASK_ROWS_8 = TASK_ROWS_7 + ", " + TASK_ROW;
    private static final String TASK_ROWS_9 = TASK_ROWS_8 + ", " + TASK_ROW;
    private static final String TASK_ROWS_10 = TASK_ROWS_9 + ", " + TASK_ROW;
    private static final String TASK_ROWS_11 = TASK_ROWS_10 + ", " + TASK_ROW;
    private static final String TASK_ROWS_12 = TASK_ROWS_11 + ", " + TASK_ROW;
    private static final String TASK_ROWS_13 = TASK_ROWS_12 + ", " + TASK_ROW;
    private static final String TASK_ROWS_14 = TASK_ROWS_13 + ", " + TASK_ROW;
    private static final String TASK_ROWS_15 = TASK_ROWS_14 + ", " + TASK_ROW;
    private static final String TASK_ROWS_16 = TASK_ROWS_15 + ", " + TASK_ROW;
    private static final String INSERT_PREFIX = """
            WITH locked AS MATERIALIZED (
                SELECT expires_at FROM execution_anchor
                WHERE execution_id = ? AND owner_id = ? AND owner_incarnation = ? AND fencing_token = ?
                FOR SHARE
            ), authorized AS MATERIALIZED (
                SELECT 1 FROM locked WHERE expires_at > clock_timestamp()
            ), issued (
            """ + JournalEventSql.TASK_COLUMNS + ") AS (VALUES ";
    private static final String INSERT_SUFFIX = """
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
        try (var statement = prepareInsert(connection, cohort.size())) {
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

    private static PreparedStatement prepareInsert(Connection connection, int count) throws SQLException {
        return switch (count) {
            case 1 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROW + INSERT_SUFFIX);
            case 2 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_2 + INSERT_SUFFIX);
            case 3 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_3 + INSERT_SUFFIX);
            case 4 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_4 + INSERT_SUFFIX);
            case 5 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_5 + INSERT_SUFFIX);
            case 6 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_6 + INSERT_SUFFIX);
            case 7 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_7 + INSERT_SUFFIX);
            case 8 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_8 + INSERT_SUFFIX);
            case 9 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_9 + INSERT_SUFFIX);
            case 10 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_10 + INSERT_SUFFIX);
            case COHORT_SIZE_11 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_11 + INSERT_SUFFIX);
            case COHORT_SIZE_12 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_12 + INSERT_SUFFIX);
            case COHORT_SIZE_13 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_13 + INSERT_SUFFIX);
            case COHORT_SIZE_14 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_14 + INSERT_SUFFIX);
            case COHORT_SIZE_15 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_15 + INSERT_SUFFIX);
            case 16 -> connection.prepareStatement(INSERT_PREFIX + TASK_ROWS_16 + INSERT_SUFFIX);
            default -> throw new IllegalArgumentException("Unsupported task issuance cohort size: " + count);
        };
    }
}
