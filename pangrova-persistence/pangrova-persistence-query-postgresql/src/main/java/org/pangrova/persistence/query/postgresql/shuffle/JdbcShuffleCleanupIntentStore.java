package org.pangrova.persistence.query.postgresql.shuffle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.pangrova.persistence.query.api.lifecycle.ShuffleCleanupIntentStore;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

/** Durable terminal cleanup intent in the same PostgreSQL authority as execution anchors. */
public final class JdbcShuffleCleanupIntentStore implements ShuffleCleanupIntentStore {
    private final DataSource dataSource;

    /** Creates an intent store sharing the Coordinator's execution-anchor database. */
    public JdbcShuffleCleanupIntentStore(DataSource dataSource) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Persists cleanup responsibility before publishing a terminal query state. */
    @Override
    public void markPending(ExecutionAnchor grant) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (lockLiveAnchor(connection, grant) == 0) {
                    recordIntent(connection, grant);
                }
                connection.commit();
            } catch (Exception failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("shuffle cleanup intent persistence failed", failure);
        }
    }

    /** Ensures a proven predecessor has a cleanup intent if its remote scope still exists. */
    public boolean prepareProvenRecovery(ExecutionAnchor grant) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (lockLiveAnchor(connection, grant) != 2) {
                    throw new SecurityException("Recovered Shuffle cleanup lacks predecessor exit proof");
                }
                boolean pending = recordRecoveredIntent(connection, grant.executionId());
                connection.commit();
                return pending;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    if (rollback != failure) failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("shuffle recovery cleanup intent persistence failed", failure);
        }
    }

    private static boolean recordRecoveredIntent(Connection connection, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement("""
                WITH target(query_id) AS (VALUES (CAST(? AS text))),
                allocated AS MATERIALIZED (
                    SELECT s.query_id FROM shuffle.shuffle_query_scope s JOIN target t USING (query_id)
                ), recorded AS (
                    INSERT INTO shuffle.shuffle_cleanup_intent (query_id)
                    SELECT query_id FROM allocated ON CONFLICT (query_id) DO NOTHING
                ), empty AS (
                    DELETE FROM shuffle.shuffle_cleanup_intent i USING target t
                    WHERE i.query_id = t.query_id AND NOT EXISTS (SELECT 1 FROM allocated)
                )
                SELECT EXISTS (SELECT 1 FROM allocated)
                """)) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Recovered Shuffle intent returned no outcome");
                return rows.getBoolean(1);
            }
        }
    }

    @Override
    public boolean completeWithoutScope(ExecutionAnchor grant) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                boolean complete = lockLiveAnchor(connection, grant) != 0
                        || deleteEmptyIntent(connection, grant.executionId());
                connection.commit();
                return complete;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    if (rollback != failure) failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("empty Shuffle cleanup intent retirement failed", failure);
        }
    }

    private static boolean deleteEmptyIntent(Connection connection, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement("""
                WITH target(query_id) AS (VALUES (CAST(? AS text))),
                allocated AS MATERIALIZED (
                    SELECT 1 FROM shuffle.shuffle_query_scope s JOIN target t USING (query_id)
                ), retired AS (
                    DELETE FROM shuffle.shuffle_cleanup_intent i USING target t
                    WHERE i.query_id = t.query_id AND NOT EXISTS (SELECT 1 FROM allocated)
                )
                SELECT NOT EXISTS (SELECT 1 FROM allocated)
                """)) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Empty Shuffle intent retirement returned no outcome");
                return rows.getBoolean(1);
            }
        }
    }

    private static int lockLiveAnchor(
            Connection connection, ExecutionAnchor grant)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_id, owner_incarnation, fencing_token, "
                        + "expires_at > clock_timestamp() AS live, recovery_drain_state "
                        + "FROM public.execution_anchor WHERE execution_id = ? FOR UPDATE")) {
            statement.setString(1, grant.executionId());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !rows.getBoolean("live")
                        || !rows.getString("owner_id").equals(grant.ownerId())
                        || !rows.getString("owner_incarnation").equals(grant.ownerIncarnation())
                        || rows.getLong("fencing_token") != grant.fencingToken()) {
                    return -1;
                }
                return rows.getInt("recovery_drain_state");
            }
        }
    }

    private static void recordIntent(
            Connection connection, ExecutionAnchor grant)
            throws SQLException {
        // Take this statement's snapshot after acquiring the anchor lock. Any
        // registration that previously held the anchor must now be visible.
        String sql = """
                WITH locked_scope AS MATERIALIZED (
                    SELECT owner_id = ? AND owner_incarnation = ? AND owner_fencing_token = ? AS owner_matches
                    FROM shuffle.shuffle_query_scope WHERE query_id = ? FOR UPDATE
                ), admission AS MATERIALIZED (
                    SELECT COALESCE((SELECT owner_matches FROM locked_scope), true) AS allowed
                ), recorded AS (
                    INSERT INTO shuffle.shuffle_cleanup_intent (query_id)
                    SELECT ? FROM admission WHERE allowed
                    ON CONFLICT (query_id) DO NOTHING
                )
                SELECT allowed FROM admission
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, grant.ownerId());
            statement.setString(2, grant.ownerIncarnation());
            statement.setLong(3, grant.fencingToken());
            statement.setString(4, grant.executionId());
            statement.setString(5, grant.executionId());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !rows.getBoolean(1)) {
                    throw new SecurityException("shuffle cleanup intent scope owner mismatch");
                }
            }
        }
    }
}
