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

    private static void bindClaim(PreparedStatement statement, ExecutionAnchor claim) throws SQLException {
        statement.setString(1, claim.executionId());
        statement.setString(2, claim.ownerId());
        statement.setString(3, claim.ownerIncarnation());
        statement.setLong(FENCE_PARAMETERS, claim.fencingToken());
    }
}
