package org.pangrova.query.transaction;

import org.pangrova.spi.coordination.ExecutionAnchor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.pangrova.query.store.jdbc.JdbcQuerySupport.canonicalInstant;
import static org.pangrova.query.store.jdbc.JdbcQuerySupport.getInstant;
import static org.pangrova.query.store.jdbc.JdbcQuerySupport.setInstant;

/** Shared SQL mappings and durability guards behind {@link JdbcExecutionAnchorStore}; all state is passed
 * explicitly. */
final class JdbcExecutionAnchorStoreMappings {

    static final Logger LOG = LoggerFactory.getLogger(JdbcExecutionAnchorStore.class);
    static final int ACQUISITION_LOCK_NAMESPACE = 0x5047414E;

    static final String SELECT_FOR_UPDATE =
            "SELECT owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version "
                    + "FROM execution_anchor WHERE execution_id = ? FOR UPDATE";
    static final String SELECT_CURRENT =
            "SELECT owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version "
                    + "FROM execution_anchor WHERE execution_id = ?";
    static final String INSERT_ANCHOR =
            "INSERT INTO execution_anchor "
                    + "(execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version) VALUES (?, ?, ?, ?, ?, ?, 1) "
                    + "ON CONFLICT DO NOTHING";
    static final String REPLACE_EXPIRED =
            "UPDATE execution_anchor SET owner_id=?, owner_incarnation=?, fencing_token=?, "
                    + "acquired_at=?, expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND fencing_token=? AND version=? AND expires_at<=?";
    static final String RENEW_OWNED =
            "UPDATE execution_anchor SET expires_at=?, version=version+1 "
                    + "WHERE owner_id=? AND owner_incarnation=? AND expires_at>? AND execution_id=ANY(?) "
                    + "RETURNING execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version";
    static final String RELEASE =
            "DELETE FROM execution_anchor WHERE execution_id=? AND owner_id=? "
                    + "AND owner_incarnation=? AND fencing_token=? AND expires_at>?";
    static final String YIELD =
            "UPDATE execution_anchor SET expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND owner_id=? AND owner_incarnation=? "
                    + "AND fencing_token=? AND expires_at>?";
    static final String LOCK_OWNED =
            "SELECT execution_id FROM execution_anchor WHERE owner_id=? AND owner_incarnation=? "
                    + "ORDER BY execution_id FOR UPDATE";
    static final String REFRESH_INSERTED =
            "UPDATE execution_anchor SET acquired_at=?, expires_at=? "
                    + "WHERE execution_id=? AND fencing_token=? AND version=1";
    static final String SELECT_EXPIRED =
            "SELECT execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version FROM execution_anchor "
                    + "WHERE expires_at<=? ORDER BY expires_at, execution_id "
                    + "LIMIT ? FOR UPDATE SKIP LOCKED";

    private JdbcExecutionAnchorStoreMappings() {}

    static ExecutionAnchor requireCurrent(Connection connection, ExecutionAnchor expected) throws SQLException {
        Objects.requireNonNull(expected, "expected");
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("Execution publication fence requires a transaction");
        }
        AnchorRow row;
        try (var statement = connection.prepareStatement(SELECT_CURRENT + " FOR SHARE")) {
            statement.setString(1, expected.executionId());
            try (var result = statement.executeQuery()) {
                row = result.next() ? mapRow(result) : null;
            }
        }
        if (row == null || !row.ownerId.equals(expected.ownerId())
                || !row.ownerIncarnation.equals(expected.ownerIncarnation())
                || row.fencingToken != expected.fencingToken() || !row.expiresAt.isAfter(dbNow(connection))) {
            throw new IllegalStateException("Execution publication owner is absent, replaced or expired");
        }
        return anchor(expected.executionId(), row);
    }

    static void lockAcquisition(Connection connection, String executionId) throws SQLException {
        // Row locks cannot serialize an absent identifier. Hold this transaction-scoped lock
        // before allocating a token so another owner cannot acquire and release the same ID
        // while this transaction is paused between sequence allocation and INSERT.
        try (var statement = connection.prepareStatement(
                "SELECT pg_advisory_xact_lock(?, hashtext('execution_anchor'::regclass::oid::text || ':' || ?))")) {
            statement.setInt(1, ACQUISITION_LOCK_NAMESPACE);
            statement.setString(2, executionId);
            statement.execute();
        }
    }

    static AnchorRow selectForUpdate(
            Connection connection, String executionId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_FOR_UPDATE)) {
            statement.setString(1, executionId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? mapRow(result) : null;
            }
        }
    }

    static List<String> lockOwned(Connection connection, String ownerId, String ownerIncarnation)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LOCK_OWNED)) {
            statement.setString(1, ownerId);
            statement.setString(2, ownerIncarnation);
            try (ResultSet result = statement.executeQuery()) {
                List<String> identities = new ArrayList<>();
                while (result.next()) identities.add(result.getString(1));
                return identities;
            }
        }
    }

    static void refreshInserted(Connection connection, String executionId, long token,
                                Instant now, Instant deadline) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(REFRESH_INSERTED)) {
            setInstant(statement, 1, now);
            setInstant(statement, 2, deadline);
            statement.setString(3, executionId);
            statement.setLong(4, token);
            if (statement.executeUpdate() != 1) {
                throw new SQLException("Inserted execution anchor lost its transaction ownership");
            }
        }
    }

    static boolean insert(
            Connection connection,
            String executionId,
            String ownerId,
            String ownerIncarnation,
            long token,
            Instant now,
            Instant deadline) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_ANCHOR)) {
            statement.setString(1, executionId);
            statement.setString(2, ownerId);
            statement.setString(3, ownerIncarnation);
            statement.setLong(4, token);
            setInstant(statement, 5, now);
            setInstant(statement, 6, deadline);
            return statement.executeUpdate() == 1;
        }
    }

    static boolean replaceExpired(Connection connection, ExpiredReplacement replacement)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(REPLACE_EXPIRED)) {
            statement.setString(1, replacement.ownerId());
            statement.setString(2, replacement.ownerIncarnation());
            statement.setLong(3, replacement.token());
            setInstant(statement, 4, replacement.now());
            setInstant(statement, 5, replacement.deadline());
            statement.setString(6, replacement.executionId());
            statement.setLong(7, replacement.row().fencingToken);
            statement.setLong(8, replacement.row().version);
            setInstant(statement, 9, replacement.now());
            return statement.executeUpdate() == 1;
        }
    }

    static List<ExpiredRow> selectExpired(
            Connection connection, Instant now, int maxBatchSize) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_EXPIRED)) {
            setInstant(statement, 1, now);
            statement.setInt(2, maxBatchSize);
            try (ResultSet result = statement.executeQuery()) {
                List<ExpiredRow> rows = new ArrayList<>();
                while (result.next()) {
                    rows.add(new ExpiredRow(
                            result.getString("execution_id"), mapRow(result)));
                }
                return rows;
            }
        }
    }

    static AnchorRow mapRow(ResultSet result) throws SQLException {
        return new AnchorRow(
                result.getString("owner_id"),
                result.getString("owner_incarnation"),
                result.getLong("fencing_token"),
                getInstant(result, "acquired_at"),
                getInstant(result, "expires_at"),
                result.getLong("version"));
    }

    static long nextToken(Connection connection) throws SQLException {
        // V78 restarts above every legacy cached range and disables sequence caching.
        // Uniqueness alone is insufficient: Workers reject a replacement token below its predecessor.
        String product = connection.getMetaData().getDatabaseProductName();
        String sql = product != null && product.toLowerCase().contains("h2")
                ? "SELECT NEXT VALUE FOR execution_anchor_token_seq"
                : "SELECT nextval('execution_anchor_token_seq')";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("Execution-anchor sequence returned no row");
            }
            long token = result.getLong(1);
            if (token <= 0) {
                throw new SQLException("Execution-anchor sequence returned invalid token");
            }
            return token;
        }
    }

    static Instant dbNow(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT clock_timestamp()");
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("Database clock returned no timestamp");
            }
            Instant now = getInstant(result, 1);
            if (now == null) {
                throw new SQLException("Database clock returned no timestamp");
            }
            return now;
        }
    }

    static ExecutionAnchor anchor(String executionId, AnchorRow row) {
        return anchor(executionId, row.ownerId, row.ownerIncarnation,
                row.fencingToken, row.acquiredAt, row.expiresAt);
    }

    static ExecutionAnchor anchor(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            long token,
            Instant acquiredAt,
            Instant expiresAt) {
        return new ExecutionAnchor(
                executionId, ownerId, ownerIncarnation, token,
                canonicalInstant(acquiredAt),
                canonicalInstant(expiresAt));
    }

    static IllegalStateException storeFailure(
            String operation, String executionId, SQLException failure) {
        LOG.error("event=execution_anchor_store_failed operation={} executionId={} error={}",
                operation, executionId, failure.getMessage(), failure);
        return new IllegalStateException(
                "Execution-anchor store operation failed: " + operation, failure);
    }

    record AnchorRow(
            String ownerId,
            String ownerIncarnation,
            long fencingToken,
            Instant acquiredAt,
            Instant expiresAt,
            long version) {}

    record ExpiredRow(String executionId, AnchorRow anchorRow) {}

    record ExpiredReplacement(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            long token,
            Instant now,
            Instant deadline,
            AnchorRow row) {}
}
