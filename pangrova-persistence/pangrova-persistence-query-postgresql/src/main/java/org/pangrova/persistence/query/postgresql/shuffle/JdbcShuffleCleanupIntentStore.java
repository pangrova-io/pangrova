package org.pangrova.persistence.query.postgresql.shuffle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.pangrova.persistence.query.api.lifecycle.ShuffleCleanupIntentStore;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver;

/** Durable terminal cleanup intent in the same PostgreSQL authority as execution anchors. */
public final class JdbcShuffleCleanupIntentStore implements ShuffleCleanupIntentStore {
    private final DataSource dataSource;
    private final ShuffleExecutionOwnerResolver ownerResolver;

    /** Creates an intent store sharing the Coordinator's execution-anchor database. */
    public JdbcShuffleCleanupIntentStore(
            DataSource dataSource, ShuffleExecutionOwnerResolver ownerResolver) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
        this.ownerResolver = java.util.Objects.requireNonNull(ownerResolver, "ownerResolver");
    }

    /** Persists cleanup responsibility before publishing a terminal query state. */
    @Override
    public void markPending(String queryId) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                ShuffleExecutionOwner owner = ownerResolver.resolve(queryId);
                requireLiveAnchor(connection, queryId, owner);
                recordIntent(connection, queryId, owner);
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
    public boolean prepareProvenRecovery(String queryId, ShuffleExecutionOwner owner) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (requireLiveAnchor(connection, queryId, owner) != 2) {
                    throw new SecurityException("Recovered Shuffle cleanup lacks predecessor exit proof");
                }
                boolean pending = recordRecoveredIntent(connection, queryId);
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
                )
                SELECT EXISTS (SELECT 1 FROM allocated)
                       OR EXISTS (SELECT 1 FROM shuffle.shuffle_cleanup_intent i JOIN target t USING (query_id))
                """)) {
            statement.setString(1, queryId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) throw new SQLException("Recovered Shuffle intent returned no outcome");
                return rows.getBoolean(1);
            }
        }
    }

    /** Returns whether cleanup remains due, including after Coordinator restart. */
    @Override
    public boolean isPending(String queryId) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT 1 FROM shuffle.shuffle_cleanup_intent WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("shuffle cleanup intent lookup failed", failure);
        }
    }

    @Override
    public Set<String> pendingForQueries(Set<String> queryIds) {
        Set<String> requested = Set.copyOf(queryIds);
        if (requested.isEmpty()) return Set.of();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT query_id FROM shuffle.shuffle_cleanup_intent WHERE query_id = ANY (?)")) {
            var ids = connection.createArrayOf("varchar", requested.toArray(String[]::new));
            try {
                statement.setArray(1, ids);
                try (ResultSet rows = statement.executeQuery()) {
                    Set<String> pending = new HashSet<>();
                    while (rows.next()) pending.add(rows.getString(1));
                    return Set.copyOf(pending);
                }
            } finally {
                ids.free();
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("shuffle cleanup intent batch lookup failed", failure);
        }
    }

    /** Lists a bounded batch for locally-owned retry or claimed-owner recovery. */
    public List<String> pendingForOwner(String ownerId, int limit) {
        return pendingForOwnerAfter(ownerId, null, limit);
    }

    /** Keyset page for fair cleanup retries when more than one batch is pending. */
    @Override
    public List<String> pendingForOwnerAfter(
            String ownerId, String afterQueryId, int limit) {
        if (limit <= 0 || limit > 1_000) {
            throw new IllegalArgumentException("shuffle cleanup scan limit is invalid");
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT i.query_id FROM shuffle.shuffle_cleanup_intent i "
                                + "JOIN public.execution_anchor a ON a.execution_id = i.query_id "
                                + "WHERE a.owner_id = ? AND a.expires_at > clock_timestamp() "
                                + "AND a.recovery_drain_state = 0 "
                                + "AND (? IS NULL OR i.query_id > ?) "
                                + "ORDER BY i.query_id LIMIT ?")) {
            statement.setString(1, ownerId);
            if (afterQueryId == null) {
                statement.setNull(2, java.sql.Types.VARCHAR);
                statement.setNull(3, java.sql.Types.VARCHAR);
            } else {
                statement.setString(2, afterQueryId);
                statement.setString(3, afterQueryId);
            }
            statement.setInt(4, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<String> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(rows.getString(1));
                }
                return List.copyOf(result);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("shuffle cleanup intent scan failed", failure);
        }
    }

    private static int requireLiveAnchor(
            Connection connection, String queryId, ShuffleExecutionOwner owner)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_id, owner_incarnation, fencing_token, "
                        + "expires_at > clock_timestamp() AS live, recovery_drain_state "
                        + "FROM public.execution_anchor WHERE execution_id = ? FOR UPDATE")) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !rows.getBoolean("live")
                        || !rows.getString("owner_id").equals(owner.ownerId())
                        || !rows.getString("owner_incarnation").equals(owner.ownerIncarnation())
                        || rows.getLong("fencing_token") != owner.fencingToken()) {
                    throw new SecurityException("shuffle cleanup intent owner is not live");
                }
                return rows.getInt("recovery_drain_state");
            }
        }
    }

    private static void recordIntent(
            Connection connection, String queryId, ShuffleExecutionOwner owner)
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
            statement.setString(1, owner.ownerId());
            statement.setString(2, owner.ownerIncarnation());
            statement.setLong(3, owner.fencingToken());
            statement.setString(4, queryId);
            statement.setString(5, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !rows.getBoolean(1)) {
                    throw new SecurityException("shuffle cleanup intent scope owner mismatch");
                }
            }
        }
    }
}
