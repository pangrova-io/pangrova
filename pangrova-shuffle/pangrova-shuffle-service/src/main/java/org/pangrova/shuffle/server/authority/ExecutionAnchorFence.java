package org.pangrova.shuffle.server.authority;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;

/** Validates shuffle ownership against the one durable execution authority. */
public final class ExecutionAnchorFence {
    private static final String OWNER_FIELDS = "owner_id, owner_incarnation, fencing_token, "
            + "expires_at > clock_timestamp() AS live";
    private static final String ANCHOR_BY_ID = " FROM public.execution_anchor WHERE execution_id = ?";
    private static final String LIVE_ANCHOR = "SELECT " + OWNER_FIELDS + ANCHOR_BY_ID;
    private static final String LOCKED_LIVE_ANCHOR_EXPIRY =
            "SELECT " + OWNER_FIELDS + ", expires_at" + ANCHOR_BY_ID + " FOR SHARE";
    private static final String LOCKED_LIVE_ANCHOR = LIVE_ANCHOR + " FOR SHARE";
    private static final String CLEANUP_ANCHOR = LIVE_ANCHOR + " AND recovery_drain_state <> 1";

    private ExecutionAnchorFence() {}

    /** Requires the requested owner to match the live durable execution anchor. */
    public static void requireLiveOwner(
            Connection connection,
            String queryId,
            ExecutionOwnerIdentity requested,
            boolean lock) throws SQLException {
        try (PreparedStatement statement = lock ? connection.prepareStatement(LOCKED_LIVE_ANCHOR)
                : connection.prepareStatement(LIVE_ANCHOR)) {
            requireOwner(statement, queryId, requested);
        }
    }

    /** Holds the exact live owner across a read-window grant and supplies its existing expiry. */
    public static OffsetDateTime lockLiveOwnerExpiry(
            Connection connection, String queryId, ExecutionOwnerIdentity requested) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LOCKED_LIVE_ANCHOR_EXPIRY)) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                requireOwner(row, requested);
                return row.getObject("expires_at", OffsetDateTime.class);
            }
        }
    }

    /** Requires either the original execution or proven predecessor exit before adopting cleanup. */
    public static void requireCleanupOwner(Connection connection, String queryId, ExecutionOwnerIdentity requested)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(CLEANUP_ANCHOR + " FOR SHARE")) {
            requireOwner(statement, queryId, requested);
        }
    }

    /** Orders terminal sealing with new registrations, including when no scope row exists yet. */
    public static void lockCleanupOwner(Connection connection, String queryId, ExecutionOwnerIdentity requested)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(CLEANUP_ANCHOR + " FOR UPDATE")) {
            requireOwner(statement, queryId, requested);
        }
    }

    private static void requireOwner(PreparedStatement statement, String queryId, ExecutionOwnerIdentity requested)
            throws SQLException {
        statement.setString(1, queryId);
        try (ResultSet row = statement.executeQuery()) {
            requireOwner(row, requested);
        }
    }

    private static void requireOwner(ResultSet row, ExecutionOwnerIdentity requested) throws SQLException {
        if (!row.next() || !row.getBoolean("live")
                || !row.getString("owner_id").equals(requested.getOwnerId())
                || !row.getString("owner_incarnation").equals(requested.getOwnerIncarnation())
                || row.getLong("fencing_token") != requested.getFencingToken()) {
            throw new SecurityException("shuffle execution owner is not the live durable anchor");
        }
    }

    /** Finds the query whose durable allocation owns the shuffle. */
    public static String queryIdForShuffle(Connection connection, String shuffleId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT query_id FROM shuffle_allocation WHERE shuffle_id = ?")) {
            statement.setString(1, shuffleId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalArgumentException("shuffle does not exist");
                }
                return row.getString(1);
            }
        }
    }
}
