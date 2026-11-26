package org.pangrova.persistence.query.postgresql.state;

import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.persistence.query.postgresql.exception.JournalAcceptanceConflictException;
import org.pangrova.persistence.query.postgresql.exception.JournalPayloadConflictException;
import org.pangrova.persistence.query.postgresql.exception.LineagePayloadConflictException;
import org.pangrova.persistence.query.postgresql.journal.JournalEventKey;
import org.pangrova.query.api.journal.JournalEventContext;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.query.api.submission.QuerySubmissionResult;
import org.pangrova.persistence.query.postgresql.lineage.QueryLineageFactSql;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Single SQL encoding and exact event reconciliation for the two authoritative journal tables. */
final class JournalEventSql {
    static final String QUERY_COLUMNS =
            "event_id, query_id, sql_text, session_json, state, timestamp_ms, coordinator_id, lineage_payload, "
                    + "principal, idempotency_key_digest, request_fingerprint, acceptance_receipt_json, retention_ms";
    static final String TASK_COLUMNS =
            "event_id, query_id, task_id, worker_id, state, stage_id, report_sequence, timestamp_ms, coordinator_id";
    private static final String QUERY_INSERT = "INSERT INTO query_state_journal (" + QUERY_COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";
    private static final String TASK_INSERT = "INSERT INTO task_state_journal (" + TASK_COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING";
    private static final String SELECT_QUERY_EVENT = "SELECT " + QUERY_COLUMNS
            + " FROM query_state_journal WHERE event_id = ?";
    private static final String SELECT_ACCEPTANCE = "SELECT " + QUERY_COLUMNS
            + " FROM query_state_journal WHERE principal = ? AND idempotency_key_digest = ? "
            + "AND state = 'QUEUED' ORDER BY id LIMIT 1 FOR UPDATE";
    private static final String SELECT_TASK_EVENT = "SELECT " + TASK_COLUMNS
            + " FROM task_state_journal WHERE event_id = ?";
    private static final String SELECT_RETIRED_TERMINAL_TIMESTAMP = """
            SELECT MAX(q.timestamp_ms) FROM query_state_journal q
            WHERE q.query_id = ? AND q.state IN ('FINISHED', 'FAILED', 'CANCELLED')
              AND NOT EXISTS (SELECT 1 FROM execution_anchor a WHERE a.execution_id = q.query_id)
            """;
    private static final int REQUEST_FINGERPRINT_PARAMETER = 11;
    private static final int ACCEPTANCE_RECEIPT_PARAMETER = 12;
    private static final int RETENTION_PARAMETER = 13;

    private JournalEventSql() { }

    static void commit(Connection connection, List<JournalPendingWrites.Write> batch) throws SQLException {
        connection.setAutoCommit(false);
        try {
            List<QueryAcceptanceAdmission> admissions = new ArrayList<>(batch.size());
            for (JournalPendingWrites.Write event : batch) {
                admissions.add(write(connection, event));
            }
            connection.commit();
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).acceptanceResult(admissions.get(index));
            }
        } catch (SQLException failure) {
            try {
                connection.rollback();
                if (failure instanceof JournalPayloadConflictException conflict) conflict.confirmRollback();
                if (failure instanceof JournalAcceptanceConflictException conflict) conflict.confirmRollback();
            } catch (SQLException rollback) {
                if (rollback != failure) failure.addSuppressed(rollback);
            }
            throw failure;
        }
    }

    private static QueryAcceptanceAdmission write(Connection connection, JournalPendingWrites.Write event)
            throws SQLException {
        if (event.query != null) {
            for (int attempt = 0; attempt < 2; attempt++) {
                int inserted;
                try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT)) {
                    bindQuery(statement, event.query);
                    inserted = statement.executeUpdate();
                }
                if (inserted > 0) {
                    appendLineage(connection, event.query);
                    return event.query.acceptance() == null ? null
                            : new QueryAcceptanceAdmission(true, event.query.acceptance());
                }
                var reconciliation = reconcileQuery(connection, event);
                if (!reconciliation.retryInsert()) {
                    if (reconciliation.publishLineage()) appendLineage(connection, event.query);
                    return reconciliation.admission();
                }
            }
            throw new JournalPayloadConflictException(event.key());
        }
        int inserted;
        try (PreparedStatement statement = connection.prepareStatement(TASK_INSERT)) {
            bindTask(statement, event.task);
            inserted = statement.executeUpdate();
        }
        if (inserted == 0) {
            reconcileTask(connection, event);
        }
        return null;
    }

    private static void appendLineage(Connection connection, QueryStateEvent event) throws SQLException {
        if (event.lineage() == null) return;
        try {
            QueryLineageFactSql.append(connection, event.lineage());
        } catch (LineagePayloadConflictException conflict) {
            var rejected = new JournalPayloadConflictException(
                    new JournalEventKey(true, event.context().eventId()));
            rejected.addSuppressed(conflict);
            throw rejected;
        }
    }

    private record QueryReconciliation(
            QueryAcceptanceAdmission admission, boolean retryInsert, boolean publishLineage) { }

    private static QueryReconciliation reconcileQuery(Connection connection,
            JournalPendingWrites.Write event) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(SELECT_QUERY_EVENT)) {
            select.setObject(1, event.context().eventId());
            try (ResultSet rows = select.executeQuery()) {
                if (rows.next()) {
                    var stored = readQuery(rows);
                    boolean exact = event.query.equals(stored);
                    if (!exact || rows.next()) {
                        throw new JournalPayloadConflictException(event.key(),
                                "Journal event mismatch expected=" + event.query + " actual=" + stored);
                    }
                    return event.query.acceptance() == null
                            ? new QueryReconciliation(null, false, true)
                            : new QueryReconciliation(
                                    new QueryAcceptanceAdmission(true, event.query.acceptance()), false, true);
                }
            }
        }
        if (event.query.acceptance() == null) throw new JournalPayloadConflictException(event.key());
        try (PreparedStatement select = connection.prepareStatement(SELECT_ACCEPTANCE)) {
            select.setString(1, event.query.acceptance().principal());
            select.setString(2, event.query.acceptance().idempotencyKeyDigest());
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) {
                    return new QueryReconciliation(null, true, false);
                }
                var existing = readQuery(rows);
                if (rows.next()) throw new JournalPayloadConflictException(event.key());
                var accepted = existing.acceptance();
                if (!event.query.acceptance().principal().equals(accepted.principal())
                        || !event.query.acceptance().idempotencyKeyDigest().equals(accepted.idempotencyKeyDigest())) {
                    throw new JournalAcceptanceConflictException(event.key(),
                            "Idempotency key was already used for a different request");
                }
                boolean sameQuery = existing.queryId().equals(event.query.queryId());
                if (!sameQuery && isExpired(connection, existing)) {
                    deleteQueryState(connection, existing.queryId());
                    deleteTaskState(connection, existing.queryId());
                    return new QueryReconciliation(null, true, false);
                }
                if (!event.query.acceptance().requestFingerprint().equals(accepted.requestFingerprint())) {
                    throw new JournalAcceptanceConflictException(event.key(),
                            "Idempotency key was already used for a different request");
                }
                return new QueryReconciliation(new QueryAcceptanceAdmission(sameQuery, accepted), false, false);
            }
        }
    }

    private static boolean isExpired(Connection connection, QueryStateEvent acceptance) throws SQLException {
        Long terminal;
        try (PreparedStatement select = connection.prepareStatement(SELECT_RETIRED_TERMINAL_TIMESTAMP)) {
            select.setString(1, acceptance.queryId());
            try (ResultSet rows = select.executeQuery()) {
                terminal = rows.next() ? rows.getLong(1) : null;
                if (terminal != null && rows.wasNull()) terminal = null;
            }
        }
        if (terminal == null) return false;
        long retainedFrom = Math.max(acceptance.context().timestampMs(), terminal);
        long now = System.currentTimeMillis();
        return now >= retainedFrom && acceptance.acceptance().retentionMs() <= now - retainedFrom;
    }

    private static void reconcileTask(Connection connection, JournalPendingWrites.Write event)
            throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(SELECT_TASK_EVENT)) {
            select.setObject(1, event.context().eventId());
            try (ResultSet rows = select.executeQuery()) {
                boolean exact = rows.next() && event.task.equals(readTask(rows));
                if (!exact || rows.next()) throw new JournalPayloadConflictException(event.key());
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
        var acceptance = event.acceptance();
        statement.setString(9, acceptance == null ? null : acceptance.principal());
        statement.setString(10, acceptance == null ? null : acceptance.idempotencyKeyDigest());
        statement.setString(REQUEST_FINGERPRINT_PARAMETER,
                acceptance == null ? null : acceptance.requestFingerprint());
        statement.setString(ACCEPTANCE_RECEIPT_PARAMETER, acceptance == null ? null
                : CanonicalJsonMapper.writeValueAsStringUnchecked(acceptance.receipt()));
        if (acceptance == null) {
            statement.setNull(RETENTION_PARAMETER, java.sql.Types.BIGINT);
        } else {
            statement.setLong(RETENTION_PARAMETER, acceptance.retentionMs());
        }
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
        var acceptance = readAcceptance(rows);
        return new QueryStateEvent(context(rows), rows.getString("query_id"), rows.getString("sql_text"),
                rows.getString("session_json"), rows.getString("state"),
                rows.getString("lineage_payload") == null ? null
                        : QueryLineageFactSql.decode(rows.getString("lineage_payload")), acceptance);
    }

    private static QueryAcceptance readAcceptance(ResultSet rows) throws SQLException {
        String digest = rows.getString("idempotency_key_digest");
        if (digest == null) return null;
        String receipt = rows.getString("acceptance_receipt_json");
        if (receipt == null) throw new SQLException("Acceptance receipt is missing");
        try {
            return new QueryAcceptance(rows.getString("principal"), digest,
                    rows.getString("request_fingerprint"),
                    CanonicalJsonMapper.readValue(receipt.getBytes(StandardCharsets.UTF_8),
                            QuerySubmissionResult.class), rows.getLong("retention_ms"));
        } catch (IOException | RuntimeException malformed) {
            throw new SQLException("Stored query acceptance is malformed", malformed);
        }
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

    static void deleteQueryState(Connection connection, String queryId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM query_state_journal WHERE query_id = ?")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        }
    }

    static void deleteTaskState(Connection connection, String queryId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM task_state_journal WHERE query_id = ?")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        }
    }
}
