package org.pangrova.persistence.query.postgresql.transaction;

import org.pangrova.persistence.query.api.commit.WriteCommitEntry;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;

/** Durable, independently leased cleanup of observed objects after verified writer exit. */
public final class JdbcObservedWriteStaging {
    private static final int CLAIM_SECONDS = 120;
    private static final int DECLINE_SECONDS = 60;
    private static final String CLAIM = """
            WITH sampled AS MATERIALIZED (SELECT clock_timestamp() AS now), target AS MATERIALIZED (
                SELECT w.query_id FROM write_commit_journal w, sampled t
                WHERE w.staging_observed_due_at <= t.now
                  AND COALESCE((w.staging_observed_declined_by ->> ?)::timestamptz, '-infinity') <= t.now
                ORDER BY w.staging_observed_due_at, w.query_id
                FOR UPDATE OF w SKIP LOCKED LIMIT 1
            ), claimed AS (
                UPDATE write_commit_journal w
                SET staging_observed_claim_id = ?,
                    staging_observed_due_at = clock_timestamp() + make_interval(secs => ?)
                FROM target WHERE w.query_id = target.query_id
                RETURNING w.*
            )
            SELECT c.*, CASE WHEN c.query_id IS NULL THEN (
                SELECT (EXTRACT(EPOCH FROM (MIN(GREATEST(w.staging_observed_due_at,
                    COALESCE((w.staging_observed_declined_by ->> ?)::timestamptz, '-infinity')))
                    - t.now)) * 1000)::bigint
                FROM write_commit_journal w WHERE w.staging_observed_due_at IS NOT NULL
            ) END AS retry_delay_ms
            FROM sampled t LEFT JOIN claimed c ON TRUE
            """;
    private static final String CLAIM_FENCE = """
            WHERE query_id = ? AND commit_fingerprint = ? AND staging_observed_claim_id = ?
                AND staging_observed_due_at > clock_timestamp()
            """;
    private static final String COMPLETE = """
            UPDATE write_commit_journal
            SET staging_observed_due_at = NULL, staging_observed_claim_id = NULL,
                staging_observed_declined_by = NULL, staging_observed_reclaimed_at = clock_timestamp()
            """ + CLAIM_FENCE;
    private static final String DECLINE = """
            UPDATE write_commit_journal w
            SET staging_observed_due_at = clock_timestamp(), staging_observed_claim_id = NULL,
                staging_observed_declined_by = COALESCE((
                    SELECT jsonb_object_agg(node, deadline)
                    FROM jsonb_each_text(w.staging_observed_declined_by) AS prior(node, deadline)
                    WHERE deadline::timestamptz > clock_timestamp()), '{}'::jsonb)
                    || jsonb_build_object(?::text, clock_timestamp() + make_interval(secs => ?))
            """ + CLAIM_FENCE;

    private final DataSource dataSource;
    public JdbcObservedWriteStaging(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Claims one item or returns the next eligible deadline, in the same statement. */
    public Poll claim(String coordinatorId) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(CLAIM)) {
            UUID id = UUID.randomUUID();
            statement.setString(1, coordinatorId);
            statement.setObject(2, id);
            statement.setInt(3, CLAIM_SECONDS);
            statement.setString(4, coordinatorId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Missing write staging schedule");
                if (rows.getString("query_id") != null) {
                    return new Poll(new Claim(id, JdbcWriteCommitJournal.readEntry(rows)), OptionalLong.empty());
                }
                long millis = rows.getLong("retry_delay_ms");
                return new Poll(null, rows.wasNull() ? OptionalLong.empty() : OptionalLong.of(Math.max(0, millis)));
            }
        }
    }

    public boolean complete(Claim claim) throws SQLException {
        return resolve(COMPLETE, claim, null);
    }

    /** Releases an unavailable claim immediately for a peer, with a bounded pause for this Coordinator. */
    public boolean decline(Claim claim, String coordinatorId) throws SQLException {
        return resolve(DECLINE, claim, Objects.requireNonNull(coordinatorId, "coordinatorId"));
    }

    private boolean resolve(String sql, Claim claim, String declinedBy) throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(sql)) {
            int bind = 1;
            if (declinedBy != null) {
                statement.setString(bind++, declinedBy);
                statement.setInt(bind++, DECLINE_SECONDS);
            }
            statement.setString(bind++, claim.intent().queryId());
            statement.setString(bind++, claim.intent().commitFingerprint());
            statement.setObject(bind, claim.id());
            return statement.executeUpdate() == 1;
        }
    }

    public record Claim(UUID id, WriteCommitEntry intent) {}

    public record Poll(Claim claim, OptionalLong delayMillis) {}

}
