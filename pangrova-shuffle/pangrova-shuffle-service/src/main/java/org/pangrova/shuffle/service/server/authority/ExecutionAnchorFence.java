package org.pangrova.shuffle.service.server.authority;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.pangrova.protocol.v2.ExecutionOwnerIdentity;

/** Validates shuffle ownership against the one durable execution authority. */
public final class ExecutionAnchorFence {
    private static final String LIVE_ANCHOR = "SELECT owner_id, owner_incarnation, "
            + "fencing_token, expires_at > clock_timestamp() AS live "
            + "FROM public.execution_anchor WHERE execution_id = ?";
    private static final String LOCKED_LIVE_ANCHOR = LIVE_ANCHOR + " FOR SHARE";

    private ExecutionAnchorFence() {}

    /** Requires the requested owner to match the live durable execution anchor. */
    public static void requireLiveOwner(
            Connection connection,
            String queryId,
            ExecutionOwnerIdentity requested,
            boolean lock) throws SQLException {
        String sql = lock ? LOCKED_LIVE_ANCHOR : LIVE_ANCHOR;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, queryId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next() || !row.getBoolean("live")
                        || !row.getString("owner_id").equals(requested.getOwnerId())
                        || !row.getString("owner_incarnation")
                                .equals(requested.getOwnerIncarnation())
                        || row.getLong("fencing_token") != requested.getFencingToken()) {
                    throw new SecurityException(
                            "shuffle execution owner is not the live durable anchor");
                }
            }
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
