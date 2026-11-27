package org.pangrova.persistence.query.postgresql.coordination;

import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Exact execution retirement in the transaction owned by its caller. */
public final class JdbcExecutionRetirement {
    private static final int ACQUISITION_LOCK_NAMESPACE = 0x5047414E;

    private static final String ADMISSION_LOCK =
            "SELECT pg_advisory_xact_lock(?, hashtext('execution_anchor'::regclass::oid::text || ':' || ?))";
    private static final String RELEASE_LOCK = "WITH admission AS MATERIALIZED (" + ADMISSION_LOCK + ") "
            + "SELECT 1 FROM admission CROSS JOIN execution_anchor a WHERE a.execution_id = ? "
            + "AND a.owner_id = ? AND a.owner_incarnation = ? AND a.fencing_token = ? FOR UPDATE OF a";

    private static final String RELEASE_MUTATIONS = """
            WITH owner(execution_id, owner_id, owner_incarnation, fencing_token, local_drain, retired_at)
                    AS MATERIALIZED (
                VALUES (CAST(? AS text), CAST(? AS text), CAST(? AS text), CAST(? AS bigint),
                        CAST(? AS boolean), clock_timestamp())
            ),
            retirement AS MATERIALIZED (
                SELECT owner.*, EXISTS (
                    SELECT 1 FROM write_commit_journal j
                    WHERE j.query_id=owner.execution_id AND j.state='PENDING'
                      AND NOT EXISTS (
                        SELECT 1 FROM write_commit_quarantine q
                        WHERE q.query_id=j.query_id AND q.commit_fingerprint=j.commit_fingerprint
                      )
                ) AS recover_write FROM owner
            ),
            retained AS (
                UPDATE execution_anchor a SET expires_at=LEAST(a.expires_at, r.retired_at), version=a.version+1,
                    coordinator_work_token = CASE WHEN r.local_drain AND a.coordinator_work_token IS NULL
                        THEN a.fencing_token ELSE a.coordinator_work_token END,
                    coordinator_work_exited = a.coordinator_work_exited OR (r.local_drain
                        AND (a.coordinator_work_token IS NULL OR a.coordinator_work_token=a.fencing_token))
                FROM retirement r
                WHERE a.execution_id=r.execution_id AND a.owner_id=r.owner_id
                  AND a.owner_incarnation=r.owner_incarnation AND a.fencing_token=r.fencing_token
                  AND ((a.expires_at>r.retired_at AND r.recover_write)
                      OR (a.expires_at<=r.retired_at AND r.local_drain AND NOT a.coordinator_work_exited
                          AND (a.coordinator_work_token IS NULL OR a.coordinator_work_token=a.fencing_token)))
                RETURNING 1
            ),
            removed AS (
                DELETE FROM execution_anchor a USING retirement r
                WHERE a.execution_id=r.execution_id AND a.owner_id=r.owner_id
                  AND a.owner_incarnation=r.owner_incarnation AND a.fencing_token=r.fencing_token
                  AND a.expires_at>r.retired_at AND NOT r.recover_write
                  AND (a.coordinator_work_token IS NULL OR a.coordinator_work_exited
                      OR (r.local_drain AND a.coordinator_work_token=a.fencing_token))
                RETURNING a.execution_id
            ),
            slots AS MATERIALIZED (
                SELECT release_tenant_query_slot(execution_id, NULL, TRUE) AS released FROM removed
            )
            """;
    private static final String RELEASE_RESULT = """
            SELECT EXISTS (SELECT 1 FROM retained) OR EXISTS (SELECT 1 FROM removed),
                   COALESCE((SELECT bool_and(released) FROM slots), TRUE),
                   EXISTS (SELECT 1 FROM removed)
            """;

    private static final String RELEASE = RELEASE_MUTATIONS + RELEASE_RESULT;
    private static final String RETIRE_QUERY = RELEASE_MUTATIONS + """
            , tasks AS (
                DELETE FROM task_state_journal j USING removed r WHERE j.query_id = r.execution_id
            ), writes AS (
                DELETE FROM write_commit_journal w USING removed r
                WHERE w.query_id = r.execution_id AND
            """ + JdbcWriteCommitJournal.RETIRABLE_WRITE + """
            ), queries AS (
                DELETE FROM query_state_journal q USING removed r, retirement retired
                WHERE q.query_id = r.execution_id
                  AND NOT EXISTS (
                    SELECT 1 FROM query_state_journal a
                    WHERE a.query_id = q.query_id AND a.idempotency_key_digest IS NOT NULL
                      AND (
                        NOT EXISTS (
                          SELECT 1 FROM query_state_journal t
                          WHERE t.query_id = a.query_id
                            AND t.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                        )
                        OR a.retention_ms > (EXTRACT(EPOCH FROM retired.retired_at) * 1000)::bigint
                            - GREATEST(a.timestamp_ms, (
                              SELECT MAX(t.timestamp_ms) FROM query_state_journal t
                              WHERE t.query_id = a.query_id
                                AND t.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                            ))
                      )
                  )
            )
            """ + RELEASE_RESULT;

    private JdbcExecutionRetirement() {}

    /** Retires locally drained SQL and its journal through one transaction. */
    public static boolean retireQuery(Connection connection, ExecutionAnchor anchor) throws SQLException {
        return lockRetirement(connection, anchor) && execute(connection, anchor, true, true).removed();
    }

    record Result(boolean completed, boolean removed) {
        static final Result NOT_OWNED = new Result(false, false);
    }

    static void lockAcquisition(Connection connection, String executionId) throws SQLException {
        // Row locks cannot serialize an absent identifier. Hold this transaction-scoped lock
        // before allocating a token so another owner cannot acquire and release the same ID
        // while this transaction is paused between sequence allocation and INSERT.
        try (var statement = connection.prepareStatement(ADMISSION_LOCK)) {
            statement.setInt(1, ACQUISITION_LOCK_NAMESPACE);
            statement.setString(2, executionId);
            statement.execute();
        }
    }

    static boolean lockRetirement(Connection connection, ExecutionAnchor anchor) throws SQLException {
        try (var statement = connection.prepareStatement(RELEASE_LOCK)) {
            statement.setInt(1, ACQUISITION_LOCK_NAMESPACE);
            statement.setString(2, anchor.executionId());
            bindOwner(statement, 3, anchor);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    static Result releaseLockedAnchor(Connection connection, ExecutionAnchor anchor, boolean localDrain)
            throws SQLException {
        return execute(connection, anchor, localDrain, false);
    }

    private static Result execute(Connection connection, ExecutionAnchor anchor, boolean localDrain,
            boolean retireJournal) throws SQLException {
        try (var statement = retireJournal ? connection.prepareStatement(RETIRE_QUERY)
                : connection.prepareStatement(RELEASE)) {
            statement.setBoolean(bindOwner(statement, 1, anchor), localDrain);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || !result.getBoolean(2)) {
                    connection.rollback();
                    throw new IllegalStateException("cluster admission ownership/count mismatch");
                }
                return new Result(result.getBoolean(1), result.getBoolean(3));
            }
        }
    }

    static int bindOwner(PreparedStatement statement, int parameter, ExecutionAnchor anchor)
            throws SQLException {
        statement.setString(parameter++, anchor.executionId());
        statement.setString(parameter++, anchor.ownerId());
        statement.setString(parameter++, anchor.ownerIncarnation());
        statement.setLong(parameter++, anchor.fencingToken());
        return parameter;
    }
}
