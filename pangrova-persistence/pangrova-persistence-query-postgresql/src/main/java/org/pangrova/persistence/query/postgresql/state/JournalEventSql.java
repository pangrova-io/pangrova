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
import org.pangrova.query.api.journal.QueryTerminalOutcome;
import org.pangrova.query.api.journal.TaskIssuance;
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
                    + "principal, idempotency_key_digest, request_fingerprint, acceptance_receipt_json, "
                    + "retention_ms, terminal_outcome";
    static final String TASK_COLUMNS =
            "event_id, query_id, task_id, worker_id, state, stage_id, report_sequence, timestamp_ms, coordinator_id, "
                    + "worker_host, worker_http_port, owner_incarnation, owner_fencing_token, admission_seal";
    static final int TASK_PARAMETERS = 14;
    private static final int WORKER_HTTP_PORT_PARAMETER = 11;
    private static final int TASK_OWNER_INCARNATION_PARAMETER = 12;
    private static final int TASK_OWNER_FENCING_TOKEN_PARAMETER = 13;
    private static final String QUERY_INSERT = "INSERT INTO query_state_journal (" + QUERY_COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT DO NOTHING";
    private static final String LOCK_ACTIVE_GRANT = """
            WITH locked AS MATERIALIZED (
                SELECT expires_at, recovery_drain_state FROM execution_anchor
                WHERE execution_id = ? AND owner_id = ? AND owner_incarnation = ? AND fencing_token = ?
                FOR SHARE
            ), authorized AS MATERIALIZED (
                SELECT 1 FROM locked WHERE expires_at > clock_timestamp() AND recovery_drain_state <> 2
            )
            """;
    private static final String FENCED_INSERT_OUTCOME = """
            SELECT CASE WHEN EXISTS (SELECT 1 FROM inserted) THEN 1
                        WHEN EXISTS (SELECT 1 FROM authorized) THEN 0 ELSE -1 END
            """;
    private static final String FENCED_QUERY_INSERT = LOCK_ACTIVE_GRANT + """
            , inserted AS (
                INSERT INTO query_state_journal (
            """ + QUERY_COLUMNS + """
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?::jsonb FROM authorized
                ON CONFLICT DO NOTHING RETURNING 1
            )
            """ + FENCED_INSERT_OUTCOME;
    private static final int FENCE_PARAMETERS = 4;
    private static final String TASK_INSERT = "INSERT INTO task_state_journal (" + TASK_COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING";
    private static final String FENCED_TASK_REPORT_INSERT = LOCK_ACTIVE_GRANT + """
            , inserted AS (
                INSERT INTO task_state_journal (
            """ + TASK_COLUMNS + """
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM authorized
                ON CONFLICT (event_id) DO NOTHING RETURNING 1
            )
            """ + FENCED_INSERT_OUTCOME;
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
    private static final int TERMINAL_OUTCOME_PARAMETER = 14;

    private JournalEventSql() { }

    static void commit(Connection connection, List<JournalPendingWrites.Write> batch) throws SQLException {
        connection.setAutoCommit(false);
        try {
            List<WriteOutcome> outcomes = new ArrayList<>(batch.size());
            for (JournalPendingWrites.Write event : batch) {
                outcomes.add(write(connection, event));
            }
            connection.commit();
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).outcome(outcomes.get(index));
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

    record WriteOutcome(QueryAcceptanceAdmission admission, boolean rejected) {
        private static final WriteOutcome WRITTEN = new WriteOutcome(null, false);
        private static final WriteOutcome FENCED = new WriteOutcome(null, true);

        static WriteOutcome accepted(QueryAcceptanceAdmission admission) {
            return admission == null ? WRITTEN : new WriteOutcome(admission, false);
        }
    }

    private static WriteOutcome write(Connection connection, JournalPendingWrites.Write event)
            throws SQLException {
        if (event.query != null) {
            for (int attempt = 0; attempt < 2; attempt++) {
                int inserted = insertQuery(connection, event);
                if (inserted < 0) {
                    return "QUEUED".equals(event.query.state()) ? staleAdmission(connection, event)
                            : WriteOutcome.WRITTEN;
                }
                if (inserted > 0) {
                    appendLineage(connection, event.query);
                    return WriteOutcome.accepted(event.query.acceptance() == null ? null
                            : new QueryAcceptanceAdmission(true, event.query.acceptance()));
                }
                var reconciliation = reconcileQuery(connection, event);
                if (!reconciliation.retryInsert()) {
                    if (reconciliation.publishLineage()) appendLineage(connection, event.query);
                    return WriteOutcome.accepted(reconciliation.admission());
                }
            }
            throw new JournalPayloadConflictException(event.key());
        }
        int inserted = insertTask(connection, event.task);
        if (inserted < 0) {
            return WriteOutcome.FENCED;
        }
        if (inserted == 0) {
            reconcileTask(connection, event.task);
        }
        return WriteOutcome.WRITTEN;
    }

    private static int insertTask(Connection connection, TaskStateEvent event) throws SQLException {
        var authority = event.reportAuthority();
        if (authority == null) {
            try (var statement = connection.prepareStatement(TASK_INSERT)) {
                bindTask(statement, event, 0);
                return statement.executeUpdate();
            }
        }
        try (var statement = connection.prepareStatement(FENCED_TASK_REPORT_INSERT)) {
            bindGrant(statement, event.queryId(), event.context().coordinatorId(),
                    authority.ownerIncarnation(), authority.fencingToken());
            bindTask(statement, event, FENCE_PARAMETERS);
            return fencedOutcome(statement);
        }
    }

    private static void bindGrant(PreparedStatement statement, String queryId, String ownerId,
            String incarnation, long token) throws SQLException {
        statement.setString(1, queryId);
        statement.setString(2, ownerId);
        statement.setString(3, incarnation);
        statement.setLong(FENCE_PARAMETERS, token);
    }

    private static int fencedOutcome(PreparedStatement statement) throws SQLException {
        try (var result = statement.executeQuery()) {
            if (!result.next()) throw new SQLException("Fenced journal insert missing outcome");
            return result.getInt(1);
        }
    }

    private static int insertQuery(Connection connection, JournalPendingWrites.Write event) throws SQLException {
        var grant = event.admissionGrant;
        if (grant == null) {
            try (var statement = connection.prepareStatement(QUERY_INSERT)) {
                bindQuery(statement, event.query, 0);
                return statement.executeUpdate();
            }
        }
        try (var statement = connection.prepareStatement(FENCED_QUERY_INSERT)) {
            bindGrant(statement, grant.executionId(), grant.ownerId(), grant.ownerIncarnation(), grant.fencingToken());
            bindQuery(statement, event.query, FENCE_PARAMETERS);
            return fencedOutcome(statement);
        }
    }

    private static WriteOutcome staleAdmission(Connection connection, JournalPendingWrites.Write event)
            throws SQLException {
        // A commit may have succeeded before ownership was lost. Replaying that
        // exact receipt must not let the predecessor start dispatch.
        if (event.query.acceptance() != null && exactQueryEventExists(connection, event)) {
            return WriteOutcome.accepted(new QueryAcceptanceAdmission(false, event.query.acceptance()));
        }
        return WriteOutcome.FENCED;
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
        if (exactQueryEventExists(connection, event)) {
            return event.query.acceptance() == null
                    ? new QueryReconciliation(null, false, true)
                    : new QueryReconciliation(new QueryAcceptanceAdmission(true, event.query.acceptance()),
                            false, true);
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

    private static boolean exactQueryEventExists(Connection connection, JournalPendingWrites.Write event)
            throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(SELECT_QUERY_EVENT)) {
            select.setObject(1, event.context().eventId());
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) return false;
                var stored = readQuery(rows);
                if (!event.query.equals(stored) || rows.next()) {
                    throw new JournalPayloadConflictException(event.key(),
                            "Journal event mismatch expected=" + event.query + " actual=" + stored);
                }
                return true;
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

    static void reconcileTask(Connection connection, TaskStateEvent event)
            throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(SELECT_TASK_EVENT)) {
            select.setObject(1, event.context().eventId());
            try (ResultSet rows = select.executeQuery()) {
                boolean exact = rows.next() && event.equals(readTask(rows));
                if (!exact || rows.next()) {
                    throw new JournalPayloadConflictException(new JournalEventKey(false, event.context().eventId()));
                }
            }
        }
    }

    private static void bindQuery(PreparedStatement statement, QueryStateEvent event, int offset) throws SQLException {
        statement.setObject(offset + 1, event.context().eventId());
        statement.setString(offset + 2, event.queryId());
        statement.setString(offset + 3, event.sql());
        statement.setString(offset + 4, event.sessionJson());
        statement.setString(offset + 5, event.state());
        statement.setLong(offset + 6, event.context().timestampMs());
        statement.setString(offset + 7, event.context().coordinatorId());
        statement.setString(offset + 8, event.lineage() == null ? null : QueryLineageFactSql.encode(event.lineage()));
        var acceptance = event.acceptance();
        statement.setString(offset + 9, acceptance == null ? null : acceptance.principal());
        statement.setString(offset + 10, acceptance == null ? null : acceptance.idempotencyKeyDigest());
        statement.setString(offset + REQUEST_FINGERPRINT_PARAMETER,
                acceptance == null ? null : acceptance.requestFingerprint());
        statement.setString(offset + ACCEPTANCE_RECEIPT_PARAMETER, acceptance == null ? null
                : CanonicalJsonMapper.writeValueAsStringUnchecked(acceptance.receipt()));
        if (acceptance == null) {
            statement.setNull(offset + RETENTION_PARAMETER, java.sql.Types.BIGINT);
        } else {
            statement.setLong(offset + RETENTION_PARAMETER, acceptance.retentionMs());
        }
        statement.setString(offset + TERMINAL_OUTCOME_PARAMETER, event.terminalOutcome() == null ? null
                : CanonicalJsonMapper.writeValueAsStringUnchecked(event.terminalOutcome()));
    }

    static void bindTask(PreparedStatement statement, TaskStateEvent event, int offset) throws SQLException {
        statement.setObject(offset + 1, event.context().eventId());
        statement.setString(offset + 2, event.queryId());
        statement.setString(offset + 3, event.taskId());
        statement.setString(offset + 4, event.workerId());
        statement.setString(offset + 5, event.state());
        statement.setInt(offset + 6, event.stageId());
        statement.setLong(offset + 7, event.reportSequence());
        statement.setLong(offset + 8, event.context().timestampMs());
        statement.setString(offset + 9, event.context().coordinatorId());
        var issuance = event.issuance();
        statement.setString(offset + 10, issuance == null ? null : issuance.workerHost());
        statement.setObject(offset + WORKER_HTTP_PORT_PARAMETER,
                issuance == null ? null : issuance.workerHttpPort(), java.sql.Types.INTEGER);
        var report = event.reportAuthority();
        String incarnation = null;
        Long token = null;
        if (issuance != null) {
            incarnation = issuance.ownerIncarnation();
            token = issuance.fencingToken();
        } else if (report != null) {
            incarnation = report.ownerIncarnation();
            token = report.fencingToken();
        }
        statement.setString(offset + TASK_OWNER_INCARNATION_PARAMETER, incarnation);
        statement.setObject(offset + TASK_OWNER_FENCING_TOKEN_PARAMETER, token, java.sql.Types.BIGINT);
        statement.setBytes(offset + TASK_PARAMETERS, issuance == null ? null : issuance.admissionSeal());
    }

    static QueryStateEvent readQuery(ResultSet rows) throws SQLException {
        var acceptance = readAcceptance(rows);
        return new QueryStateEvent(context(rows), rows.getString("query_id"), rows.getString("sql_text"),
                rows.getString("session_json"), rows.getString("state"),
                rows.getString("lineage_payload") == null ? null
                        : QueryLineageFactSql.decode(rows.getString("lineage_payload")), acceptance,
                readTerminalOutcome(rows.getString("terminal_outcome")));
    }

    static QueryTerminalOutcome readTerminalOutcome(String body) throws SQLException {
        if (body == null) return null;
        try {
            return CanonicalJsonMapper.readValue(body.getBytes(StandardCharsets.UTF_8),
                    QueryTerminalOutcome.class);
        } catch (IOException | RuntimeException failure) {
            throw new SQLException("Stored terminal outcome is malformed", failure);
        }
    }

    static QueryAcceptance readAcceptance(ResultSet rows) throws SQLException {
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
        byte[] seal = rows.getBytes("admission_seal");
        TaskIssuance issuance = seal == null ? null
                : new TaskIssuance(rows.getString("worker_host"), rows.getInt("worker_http_port"),
                        rows.getString("owner_incarnation"), rows.getLong("owner_fencing_token"), seal);
        String incarnation = rows.getString("owner_incarnation");
        var report = seal == null && incarnation != null
                ? new TaskStateEvent.ReportAuthority(incarnation, rows.getLong("owner_fencing_token")) : null;
        return new TaskStateEvent(context(rows), rows.getString("query_id"), rows.getString("task_id"),
                rows.getString("worker_id"), rows.getString("state"), rows.getInt("stage_id"),
                rows.getLong("report_sequence"), issuance, report);
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
