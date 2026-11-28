package org.pangrova.persistence.query.postgresql.transaction;

import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.enums.JournalAdmissionOutcome;
import org.pangrova.persistence.query.api.exception.JournalWriteException;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/** Claims and acknowledges cleanup of observed, unpublished objects under an exact recovery grant. */
public final class JdbcObservedWriteStaging {
    private static final String LOCK_OWNER = """
            WITH owner AS MATERIALIZED (
                SELECT execution_id, fencing_token, expires_at FROM execution_anchor
                WHERE execution_id = ? AND owner_id = ? AND owner_incarnation = ?
                  AND fencing_token = ? AND recovery_drain_state = 2
                FOR SHARE
            ), target AS MATERIALIZED (
                SELECT w.query_id, w.commit_fingerprint
                FROM owner a JOIN write_commit_journal w ON w.query_id = a.execution_id
                WHERE w.staging_drain_token > 0 AND w.staging_drain_token <= a.fencing_token
                  AND w.staging_observed_reclaimed_at IS NULL
                FOR UPDATE OF w
            ), authority AS MATERIALIZED (
                SELECT a.execution_id, a.fencing_token
                FROM owner a
                WHERE CASE WHEN EXISTS (SELECT 1 FROM target)
                    THEN a.expires_at > clock_timestamp() ELSE FALSE END
            )
            """;
    private static final String CLAIM = LOCK_OWNER + """
            , claimed AS (
                UPDATE write_commit_journal w SET staging_observed_claim_token = a.fencing_token
                FROM authority a
                WHERE w.query_id = a.execution_id AND (w.staging_observed_claim_token IS NULL
                    OR w.staging_observed_claim_token < a.fencing_token)
                RETURNING w.*
            )
            SELECT * FROM claimed
            UNION ALL
            SELECT w.* FROM authority a JOIN write_commit_journal w ON w.query_id = a.execution_id
            WHERE w.staging_observed_claim_token = a.fencing_token
            """;
    private static final String COMPLETE = LOCK_OWNER + """
            UPDATE write_commit_journal w SET staging_observed_reclaimed_at = clock_timestamp()
            FROM authority a
            WHERE w.query_id = a.execution_id AND w.commit_fingerprint = ?
              AND w.staging_observed_claim_token = a.fencing_token
            """;

    private final DataSource dataSource;

    public JdbcObservedWriteStaging(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public Optional<WriteCommitEntry> claim(ExecutionAnchor owner) {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(CLAIM)) {
            bindOwner(statement, owner);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(JdbcWriteCommitJournal.readEntry(rows)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw unavailable("Cannot claim observed staging cleanup", failure);
        }
    }

    public boolean complete(ExecutionAnchor owner, String fingerprint) {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(COMPLETE)) {
            bindOwner(statement, owner);
            statement.setString(5, fingerprint);
            return statement.executeUpdate() == 1;
        } catch (SQLException failure) {
            throw unavailable("Cannot complete observed staging cleanup", failure);
        }
    }

    private static void bindOwner(PreparedStatement statement, ExecutionAnchor owner) throws SQLException {
        statement.setString(1, owner.executionId());
        statement.setString(2, owner.ownerId());
        statement.setString(3, owner.ownerIncarnation());
        statement.setLong(4, owner.fencingToken());
    }

    private static JournalWriteException unavailable(String message, SQLException failure) {
        return new JournalWriteException(message, failure, JournalAdmissionOutcome.ACCEPTED_UNESTABLISHED);
    }
}
