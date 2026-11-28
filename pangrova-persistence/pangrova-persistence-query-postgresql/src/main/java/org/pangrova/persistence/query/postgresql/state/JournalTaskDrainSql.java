package org.pangrova.persistence.query.postgresql.state;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal;

import static org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal.PAGE_SIZE;

final class JournalTaskDrainSql {
    private static final int FENCE_PARAMETERS = 4;
    private static final String OBSERVED = """
            SELECT claims.id FROM unnest(?::varchar[]) AS claims(id)
            LEFT JOIN LATERAL (
                SELECT 1 AS hit FROM query_state_journal q WHERE q.query_id = claims.id LIMIT 1
            ) query_fact ON true
            LEFT JOIN LATERAL (
                SELECT 1 AS hit FROM task_state_journal t
                WHERE query_fact.hit IS NULL AND t.query_id = claims.id LIMIT 1
            ) task_fact ON true
            LEFT JOIN LATERAL (
                SELECT 1 AS hit FROM execution_anchor a
                WHERE query_fact.hit IS NULL AND task_fact.hit IS NULL
                  AND a.execution_id = claims.id AND a.coordinator_work_exited LIMIT 1
            ) work_exit ON true
            WHERE query_fact.hit IS NOT NULL OR task_fact.hit IS NOT NULL OR work_exit.hit IS NOT NULL
            """;
    private static final String CLAIM = """
            SELECT execution_id, owner_id, owner_incarnation, fencing_token, expires_at,
                   coordinator_work_token, coordinator_work_exited FROM execution_anchor
            WHERE execution_id = ? AND owner_id = ? AND owner_incarnation = ? AND fencing_token = ?
            """;
    private static final String RESERVE_COORDINATOR_STATEMENT = """
            WITH locked AS MATERIALIZED (
            """ + CLAIM + """
                AND recovery_drain_state = 0 FOR UPDATE
            )
            UPDATE execution_anchor a SET coordinator_work_token = l.fencing_token,
                coordinator_work_domain = origin.id,
                coordinator_work_incarnation = CASE WHEN origin.id IS NOT NULL THEN l.owner_incarnation END
            FROM locked l CROSS JOIN (VALUES (CAST(? AS text))) AS origin(id)
            WHERE a.execution_id = l.execution_id AND l.coordinator_work_token IS NULL
                   AND l.expires_at > clock_timestamp()
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
    // The same proof authorizes handoff of staged files for the fenced abort fingerprint.
    private static final String LOCK_CLAIM = CLAIM + " FOR UPDATE";
    private static final String CONFIRM = """
            WITH claim AS MATERIALIZED (
            """ + CLAIM + """
                AND expires_at > clock_timestamp() AND recovery_drain_state IN (1, 2)
            ), evidence AS MATERIALIZED (
                SELECT EXISTS (SELECT 1 FROM claim WHERE coordinator_work_exited) AS local_work_exited,
                       EXISTS (SELECT 1 FROM query_state_journal q JOIN claim c ON q.query_id = c.execution_id)
                           AS query_observed
            ), write_state AS MATERIALIZED (
                SELECT w.query_id, w.commit_fingerprint, w.state, w.staging_drain_token, w.execution_fencing_token
                FROM write_commit_journal w JOIN claim c ON w.query_id = c.execution_id
            ), outcome AS MATERIALIZED (
                SELECT CASE
                    WHEN NOT EXISTS (SELECT 1 FROM claim) THEN 'LOST_CLAIM'
                    WHEN EXISTS (SELECT 1 FROM execution_anchor a JOIN claim c USING (execution_id)
                                 WHERE a.recovery_drain_state = 2) THEN 'CONFIRMED'
                    WHEN NOT query_observed AND NOT local_work_exited THEN 'NO_SQL_EVIDENCE'
                    WHEN EXISTS (SELECT 1 FROM claim
                                 WHERE coordinator_work_token IS NOT NULL AND NOT coordinator_work_exited)
                        THEN 'PENDING_COORDINATOR_STATEMENT'
                    WHEN (NOT local_work_exited AND NOT EXISTS (
                              SELECT 1 FROM task_state_journal t JOIN claim c ON t.query_id = c.execution_id
                              WHERE t.admission_seal IS NOT NULL
                          ) AND NOT EXISTS (
                              SELECT 1 FROM query_state_journal q JOIN claim c ON q.query_id = c.execution_id
                              WHERE q.state = 'QUEUED'
                          )) OR EXISTS (
                             SELECT 1 FROM task_state_journal t JOIN claim c ON t.query_id = c.execution_id
                             WHERE t.owner_fencing_token >= c.fencing_token
                                OR (t.admission_seal IS NULL AND NOT EXISTS (
                                    SELECT 1 FROM task_state_journal issued
                                    WHERE issued.query_id = t.query_id AND issued.task_id = t.task_id
                                      AND issued.worker_id = t.worker_id AND issued.stage_id = t.stage_id
                                      AND issued.coordinator_id = t.coordinator_id
                                      AND issued.admission_seal IS NOT NULL
                                      AND (t.owner_incarnation IS NULL OR (
                                          issued.owner_incarnation = t.owner_incarnation
                                          AND issued.owner_fencing_token = t.owner_fencing_token))
                                ))
                         ) THEN 'MISSING_ISSUANCE'
                    WHEN EXISTS (
                        SELECT 1 FROM task_state_journal t JOIN claim c ON t.query_id = c.execution_id
                        WHERE t.admission_seal IS NOT NULL AND t.recovery_drain_receipt IS NULL
                    ) THEN 'PENDING_RECEIPTS'
                    WHEN EXISTS (
                        SELECT 1 FROM write_state w
                        WHERE w.state = 'PENDING' AND NOT EXISTS (
                            SELECT 1 FROM write_commit_quarantine q
                            WHERE q.query_id = w.query_id AND q.commit_fingerprint = w.commit_fingerprint
                        )
                    ) THEN 'PENDING_WRITE'
                    ELSE 'CONFIRMED'
                END AS state FROM evidence
            ), staging_handoff AS (
                UPDATE write_commit_journal w SET staging_drain_token = c.fencing_token,
                    staging_observed_due_at = clock_timestamp()
                FROM claim c, outcome o, write_state original
                WHERE o.state = 'CONFIRMED' AND original.query_id = c.execution_id
                  AND original.execution_fencing_token < c.fencing_token
                  AND original.staging_drain_token = 0 AND w.staging_drain_token = 0
                  AND w.query_id = original.query_id AND w.commit_fingerprint = original.commit_fingerprint
            ), sealed AS (
                UPDATE execution_anchor a SET recovery_drain_state = 2
                FROM claim c, outcome o
                WHERE a.execution_id = c.execution_id AND o.state = 'CONFIRMED' AND a.recovery_drain_state = 1
                RETURNING a.execution_id, a.owner_id
            ), terminal AS (
                INSERT INTO query_state_journal (event_id, query_id, state, timestamp_ms, coordinator_id,
                                                 terminal_outcome)
                SELECT gen_random_uuid(), s.execution_id,
                       CASE WHEN completed.committed THEN 'FINISHED' ELSE 'FAILED' END,
                       (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint, s.owner_id,
                       jsonb_build_object('resultDisposition', 'NONE', 'error',
                            CASE WHEN NOT completed.committed THEN jsonb_build_object(
                                'errorCode', 'QUERY_OWNER_LOST', 'errorType', 'EXECUTION',
                                'message', 'Original query execution stopped before completion') END)
                FROM sealed s CROSS JOIN evidence e CROSS JOIN LATERAL (
                    SELECT EXISTS (SELECT 1 FROM write_state w
                                   WHERE w.query_id = s.execution_id AND w.state = 'COMMITTED') AS committed
                ) completed
                WHERE e.query_observed AND NOT EXISTS (
                    SELECT 1 FROM query_state_journal q WHERE q.query_id = s.execution_id
                    AND q.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                )
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

    static boolean reserveCoordinatorStatement(Connection connection, ExecutionAnchor grant, String domain)
            throws SQLException {
        try (var statement = connection.prepareStatement(RESERVE_COORDINATOR_STATEMENT)) {
            bindClaim(statement, grant);
            statement.setString(FENCE_PARAMETERS + 1, domain);
            return statement.executeUpdate() == 1;
        }
    }

    static Set<String> observed(Connection connection, Set<String> claims) throws SQLException {
        var observed = new HashSet<String>();
        try (var statement = connection.prepareStatement(OBSERVED);
                var arrays = new JournalSqlArrays(connection)) {
            arrays.bind(statement, 1, "varchar", claims.toArray(String[]::new));
            try (var rows = statement.executeQuery()) {
                while (rows.next()) observed.add(rows.getString(1));
            }
        }
        return Set.copyOf(observed);
    }

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
