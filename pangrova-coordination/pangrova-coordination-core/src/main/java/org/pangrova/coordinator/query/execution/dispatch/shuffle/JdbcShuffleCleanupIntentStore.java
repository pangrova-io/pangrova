package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver;

/** Durable terminal cleanup intent in the same PostgreSQL authority as execution anchors. */
public final class JdbcShuffleCleanupIntentStore {
    private final DataSource dataSource;
    private final ShuffleExecutionOwnerResolver ownerResolver;

    /** Creates an intent store sharing the Coordinator's execution-anchor database. */
    public JdbcShuffleCleanupIntentStore(
            DataSource dataSource, ShuffleExecutionOwnerResolver ownerResolver) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
        this.ownerResolver = java.util.Objects.requireNonNull(ownerResolver, "ownerResolver");
    }

    /** Persists cleanup responsibility before publishing a terminal query state. */
    public void markPending(String queryId) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                ShuffleExecutionOwner owner = ownerResolver.resolve(queryId);
                requireLiveAnchor(connection, queryId, owner);
                if (scopeExists(connection, queryId)) {
                    scopeExistsForOwner(connection, queryId, owner);
                }
                insertIntent(connection, queryId);
                connection.commit();
            } catch (Exception failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("shuffle cleanup intent persistence failed", failure);
        }
    }

    /** Returns whether cleanup remains due, including after Coordinator restart. */
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

    /** Lists a bounded batch for locally-owned retry or claimed-owner recovery. */
    public List<String> pendingForOwner(String ownerId, int limit) {
        return pendingForOwnerAfter(ownerId, null, limit);
    }

    /** Keyset page for fair cleanup retries when more than one batch is pending. */
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

    private static boolean scopeExists(Connection connection, String queryId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM shuffle.shuffle_query_scope WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static void requireLiveAnchor(
            Connection connection, String queryId, ShuffleExecutionOwner owner)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_id, owner_incarnation, fencing_token, "
                        + "expires_at > clock_timestamp() AS live "
                        + "FROM public.execution_anchor WHERE execution_id = ? FOR UPDATE")) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !rows.getBoolean("live")
                        || !rows.getString("owner_id").equals(owner.ownerId())
                        || !rows.getString("owner_incarnation").equals(owner.ownerIncarnation())
                        || rows.getLong("fencing_token") != owner.fencingToken()) {
                    throw new SecurityException("shuffle cleanup intent owner is not live");
                }
            }
        }
    }

    private static boolean scopeExistsForOwner(
            Connection connection, String queryId, ShuffleExecutionOwner owner)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT owner_id, owner_incarnation, owner_fencing_token "
                        + "FROM shuffle.shuffle_query_scope WHERE query_id = ? FOR UPDATE")) {
            statement.setString(1, queryId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return false;
                }
                if (!rows.getString(1).equals(owner.ownerId())
                        || !rows.getString(2).equals(owner.ownerIncarnation())
                        || rows.getLong(3) != owner.fencingToken()) {
                    throw new SecurityException("shuffle cleanup intent scope owner mismatch");
                }
                return true;
            }
        }
    }

    private static void insertIntent(Connection connection, String queryId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO shuffle.shuffle_cleanup_intent (query_id) VALUES (?) "
                        + "ON CONFLICT (query_id) DO NOTHING")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        }
    }
}
