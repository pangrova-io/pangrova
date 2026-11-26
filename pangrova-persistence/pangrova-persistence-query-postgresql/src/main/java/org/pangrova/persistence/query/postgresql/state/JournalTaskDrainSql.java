package org.pangrova.persistence.query.postgresql.state;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal;

import static org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.PAGE_SIZE;

final class JournalTaskDrainSql {
    private static final int FENCE_PARAMETERS = 4;
    private static final String CLAIM = """
            SELECT execution_id, fencing_token, expires_at FROM execution_anchor
            WHERE execution_id = ? AND owner_id = ? AND owner_incarnation = ? AND fencing_token = ?
            """;
    private static final String PENDING = """
            WITH claim AS MATERIALIZED (
            """ + CLAIM + """
                AND expires_at > clock_timestamp()
            )
            SELECT t.* FROM claim JOIN task_state_journal t ON t.query_id = claim.execution_id
            WHERE t.owner_fencing_token < claim.fencing_token AND t.id > ?
                AND t.admission_seal IS NOT NULL AND t.recovery_drain_receipt IS NULL
            ORDER BY t.id LIMIT ?
            """;

    // Anchor locking and this proof use separate READ COMMITTED snapshots. A same-token
    // issuance that held FOR SHARE before our lock must be visible to the proof.
    private static final String LOCK_CLAIM = CLAIM + " FOR UPDATE";
    private static final String CONFIRM = """
            WITH claim AS MATERIALIZED (
            """ + CLAIM + """
                AND expires_at > clock_timestamp() AND recovery_drain_state IN (1, 2)
            ), outcome AS MATERIALIZED (
                SELECT CASE
                    WHEN NOT EXISTS (SELECT 1 FROM claim) THEN 'LOST_CLAIM'
                    WHEN EXISTS (SELECT 1 FROM execution_anchor a JOIN claim c USING (execution_id)
                                 WHERE a.recovery_drain_state = 2) THEN 'CONFIRMED'
                    WHEN NOT EXISTS (SELECT 1 FROM query_state_journal q JOIN claim c ON q.query_id = c.execution_id)
                        THEN 'NO_SQL_EVIDENCE'
                    WHEN NOT EXISTS (SELECT 1 FROM query_state_journal q JOIN claim c ON q.query_id = c.execution_id
                                     WHERE q.state IN ('FINISHED', 'FAILED', 'CANCELLED'))
                        THEN 'AWAITING_TERMINAL'
                    WHEN NOT EXISTS (SELECT 1 FROM task_state_journal t JOIN claim c ON t.query_id = c.execution_id
                                     WHERE t.admission_seal IS NOT NULL)
                         OR EXISTS (
                             SELECT 1 FROM task_state_journal t JOIN claim c ON t.query_id = c.execution_id
                             WHERE t.owner_fencing_token >= c.fencing_token
                                OR (t.admission_seal IS NULL AND NOT EXISTS (
                                    SELECT 1 FROM task_state_journal issued
                                    WHERE issued.query_id = t.query_id AND issued.task_id = t.task_id
                                      AND issued.worker_id = t.worker_id AND issued.stage_id = t.stage_id
                                      AND issued.coordinator_id = t.coordinator_id
                                      AND issued.admission_seal IS NOT NULL
                                ))
                         ) THEN 'MISSING_ISSUANCE'
                    WHEN EXISTS (
                        SELECT 1 FROM task_state_journal t JOIN claim c ON t.query_id = c.execution_id
                        WHERE t.admission_seal IS NOT NULL AND t.recovery_drain_receipt IS NULL
                    ) THEN 'PENDING_RECEIPTS'
                    ELSE 'CONFIRMED'
                END AS state
            ), sealed AS (
                UPDATE execution_anchor a SET recovery_drain_state = 2
                FROM claim c, outcome o
                WHERE a.execution_id = c.execution_id AND o.state = 'CONFIRMED' AND a.recovery_drain_state = 1
            )
            SELECT state FROM outcome
            """;

    private static final String RETAIN = """
            WITH locked AS MATERIALIZED (
            """ + CLAIM + """
                FOR SHARE
            ), claim AS MATERIALIZED (
                SELECT execution_id, fencing_token FROM locked WHERE expires_at > clock_timestamp()
            ), evidence AS (
                SELECT * FROM unnest(?::bigint[], ?::bytea[]) AS receipt(id, body)
            ), recorded AS (
                UPDATE task_state_journal t SET recovery_drain_receipt = evidence.body
                FROM claim, evidence
                WHERE t.query_id = claim.execution_id AND t.id = evidence.id
                    AND t.owner_fencing_token < claim.fencing_token
                    AND t.admission_seal IS NOT NULL AND t.recovery_drain_receipt IS NULL
            )
            SELECT EXISTS (SELECT 1 FROM claim)
            """;

    private JournalTaskDrainSql() { }

    static List<RecoveredTaskDrainJournal.IssuedAttempt> pending(
            Connection connection, ExecutionAnchor claim, long afterRowId) throws SQLException {
        var page = new ArrayList<RecoveredTaskDrainJournal.IssuedAttempt>(PAGE_SIZE);
        try (var statement = connection.prepareStatement(PENDING)) {
            bindClaim(statement, claim);
            statement.setLong(FENCE_PARAMETERS + 1, afterRowId);
            statement.setInt(FENCE_PARAMETERS + 2, PAGE_SIZE);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    page.add(new RecoveredTaskDrainJournal.IssuedAttempt(
                            rows.getLong("id"), JournalEventSql.readTask(rows)));
                }
            }
        }
        return List.copyOf(page);
    }

    static boolean retain(Connection connection, ExecutionAnchor claim,
            List<RecoveredTaskDrainJournal.DrainReceipt> receipts) throws SQLException {
        try (var statement = connection.prepareStatement(RETAIN);
                var arrays = new JournalSqlArrays(connection)) {
            bindClaim(statement, claim);
            arrays.bind(statement, FENCE_PARAMETERS + 1, "bigint",
                    receipts.stream().map(RecoveredTaskDrainJournal.DrainReceipt::rowId).toArray(Long[]::new));
            arrays.bind(statement, FENCE_PARAMETERS + 2, "bytea",
                    receipts.stream().map(RecoveredTaskDrainJournal.DrainReceipt::encoded).toArray(byte[][]::new));
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Task drain update returned no outcome");
                return rows.getBoolean(1);
            }
        }
    }

    static RecoveredTaskDrainJournal.DrainOutcome confirm(Connection connection, ExecutionAnchor claim)
            throws SQLException {
        connection.setAutoCommit(false);
        try (var lock = connection.prepareStatement(LOCK_CLAIM)) {
            bindClaim(lock, claim);
            try (var rows = lock.executeQuery()) {
                if (!rows.next()) {
                    connection.rollback();
                    return RecoveredTaskDrainJournal.DrainOutcome.LOST_CLAIM;
                }
            }
            try (var statement = connection.prepareStatement(CONFIRM)) {
                bindClaim(statement, claim);
                try (var rows = statement.executeQuery()) {
                    if (!rows.next()) throw new SQLException("Task drain proof returned no outcome");
                    var outcome = RecoveredTaskDrainJournal.DrainOutcome.valueOf(rows.getString(1));
                    connection.commit();
                    return outcome;
                }
            }
        } catch (SQLException | RuntimeException failure) {
            try {
                connection.rollback();
            } catch (SQLException rollback) {
                if (rollback != failure) failure.addSuppressed(rollback);
            }
            throw failure;
        }
    }

    private static void bindClaim(PreparedStatement statement, ExecutionAnchor claim) throws SQLException {
        statement.setString(1, claim.executionId());
        statement.setString(2, claim.ownerId());
        statement.setString(3, claim.ownerIncarnation());
        statement.setLong(FENCE_PARAMETERS, claim.fencingToken());
    }
}
