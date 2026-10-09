package org.pangrova.metadata.coordination;

import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.ExecutionAnchorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.metadata.store.JdbcSupport.canonicalInstant;
import static org.pangrova.metadata.store.JdbcSupport.getInstant;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/** JDBC execution-anchor store using database-clock leases and fenced row locks. */
public final class JdbcExecutionAnchorStore implements ExecutionAnchorStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExecutionAnchorStore.class);

    private static final String SELECT_FOR_UPDATE =
            "SELECT owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version "
                    + "FROM execution_anchor WHERE execution_id = ? FOR UPDATE";
    private static final String SELECT_CURRENT =
            "SELECT owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version "
                    + "FROM execution_anchor WHERE execution_id = ?";
    private static final String INSERT_ANCHOR =
            "INSERT INTO execution_anchor "
                    + "(execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version) VALUES (?, ?, ?, ?, ?, ?, 1) "
                    + "ON CONFLICT DO NOTHING";
    private static final String REFRESH_INSERTED_ANCHOR_TIMES =
            "UPDATE execution_anchor SET acquired_at=?, expires_at=? "
                    + "WHERE execution_id=? AND owner_id=? AND owner_incarnation=? AND fencing_token=?";
    private static final String REPLACE_EXPIRED =
            "UPDATE execution_anchor SET owner_id=?, owner_incarnation=?, fencing_token=?, "
                    + "acquired_at=?, expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND fencing_token=? AND version=? AND expires_at<=?";
    private static final String RENEW_OWNED =
            "WITH owned AS MATERIALIZED ("
                    + "SELECT execution_id, fencing_token, expires_at FROM execution_anchor "
                    + "WHERE owner_id=? AND owner_incarnation=? ORDER BY execution_id FOR UPDATE) "
                    + "UPDATE execution_anchor AS live SET "
                    + "expires_at=clock_timestamp() + (? * INTERVAL '1 millisecond'), version=live.version+1 "
                    + "FROM owned WHERE live.execution_id=owned.execution_id "
                    + "AND live.fencing_token=owned.fencing_token AND owned.expires_at>clock_timestamp() "
                    + "RETURNING live.execution_id, live.owner_id, live.owner_incarnation, live.fencing_token, "
                    + "live.acquired_at, live.expires_at, live.version";
    private static final String RELEASE =
            "DELETE FROM execution_anchor WHERE execution_id=? AND owner_id=? "
                    + "AND owner_incarnation=? AND fencing_token=? AND expires_at>?";
    private static final String YIELD =
            "UPDATE execution_anchor SET expires_at=CURRENT_TIMESTAMP, version=version+1 "
                    + "WHERE execution_id=? AND owner_id=? AND owner_incarnation=? "
                    + "AND fencing_token=? AND expires_at>CURRENT_TIMESTAMP";
    private static final String YIELD_OWNED =
            "UPDATE execution_anchor SET expires_at=CURRENT_TIMESTAMP, version=version+1 "
                    + "WHERE owner_id=? AND owner_incarnation=? AND expires_at>CURRENT_TIMESTAMP";
    private static final String SELECT_EXPIRED =
            "SELECT execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version FROM execution_anchor "
                    + "WHERE expires_at<=? ORDER BY expires_at, execution_id "
                    + "LIMIT ? FOR UPDATE SKIP LOCKED";

    private final DataSource dataSource;

    public JdbcExecutionAnchorStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public Optional<ExecutionAnchor> tryAcquire(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration) {
        requireRequest(executionId, ownerId, ownerIncarnation, leaseDuration);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            Optional<ExecutionAnchor> result = acquireLocked(
                    connection, executionId, ownerId, ownerIncarnation, leaseDuration);
            connection.commit();
            return result;
        } catch (SQLException failure) {
            throw storeFailure("acquire", executionId, failure);
        }
    }

    private Optional<ExecutionAnchor> acquireLocked(
            Connection connection,
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration) throws SQLException {
        AnchorRow row = selectForUpdate(connection, executionId);
        if (row == null) {
            long token = nextToken(connection);
            Instant insertTime = dbNow(connection);
            Instant insertDeadline = insertTime.plusMillis(leaseDuration.toMillis());
            if (insert(connection, executionId, ownerId, ownerIncarnation, token, insertTime, insertDeadline)) {
                // ON CONFLICT may have waited for an uncommitted owner. Refresh
                // inside this transaction after the insert has actually won.
                Instant acquiredAt = dbNow(connection);
                Instant expiresAt = acquiredAt.plusMillis(leaseDuration.toMillis());
                refreshInsertedAnchorTimes(
                        connection, executionId, ownerId, ownerIncarnation, token, acquiredAt, expiresAt);
                return Optional.of(anchor(
                        executionId, ownerId, ownerIncarnation, token, acquiredAt, expiresAt));
            }
            row = selectForUpdate(connection, executionId);
        }
        if (row == null) {
            return Optional.empty();
        }
        // SELECT FOR UPDATE can block. PostgreSQL CURRENT_TIMESTAMP is fixed at
        // transaction start, so use the wall clock only after acquiring the row.
        Instant now = dbNow(connection);
        Instant deadline = now.plusMillis(leaseDuration.toMillis());
        if (row.expiresAt.isAfter(now)) {
            return Optional.empty();
        }
        long token = nextToken(connection);
        if (!replaceExpired(connection, new ExpiredReplacement(
                executionId, ownerId, ownerIncarnation, token, now, deadline, row))) {
            return Optional.empty();
        }
        return Optional.of(anchor(
                executionId, ownerId, ownerIncarnation, token, now, deadline));
    }

    private static void refreshInsertedAnchorTimes(
            Connection connection,
            String executionId,
            String ownerId,
            String ownerIncarnation,
            long token,
            Instant acquiredAt,
            Instant expiresAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(REFRESH_INSERTED_ANCHOR_TIMES)) {
            setInstant(statement, 1, acquiredAt);
            setInstant(statement, 2, expiresAt);
            statement.setString(3, executionId);
            statement.setString(4, ownerId);
            statement.setString(5, ownerIncarnation);
            statement.setLong(6, token);
            if (statement.executeUpdate() != 1) {
                throw new SQLException("New execution anchor changed before its acquisition could be committed");
            }
        }
    }

    @Override
    public List<ExecutionAnchor> renewOwned(
            String ownerId, String ownerIncarnation, Duration leaseDuration) {
        requireOwner(ownerId, ownerIncarnation);
        requireDuration(leaseDuration);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(RENEW_OWNED)) {
                // Expiry is checked after the materialized row-lock boundary,
                // not against a transaction timestamp captured before waiting.
                statement.setString(1, ownerId);
                statement.setString(2, ownerIncarnation);
                statement.setLong(3, leaseDuration.toMillis());
                List<ExecutionAnchor> renewed = new ArrayList<>();
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        renewed.add(anchor(result.getString("execution_id"), mapRow(result)));
                    }
                }
                connection.commit();
                return List.copyOf(renewed);
            }
        } catch (SQLException failure) {
            throw storeFailure("renew", ownerId, failure);
        }
    }

    @Override
    public boolean release(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            Instant now = dbNow(connection);
            try (PreparedStatement statement = connection.prepareStatement(RELEASE)) {
                statement.setString(1, anchor.executionId());
                statement.setString(2, anchor.ownerId());
                statement.setString(3, anchor.ownerIncarnation());
                statement.setLong(4, anchor.fencingToken());
                setInstant(statement, 5, now);
                boolean released = statement.executeUpdate() == 1;
                connection.commit();
                return released;
            }
        } catch (SQLException failure) {
            throw storeFailure("release", anchor.executionId(), failure);
        }
    }

    @Override
    public boolean yield(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor must not be null");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(YIELD)) {
            statement.setString(1, anchor.executionId());
            statement.setString(2, anchor.ownerId());
            statement.setString(3, anchor.ownerIncarnation());
            statement.setLong(4, anchor.fencingToken());
            return statement.executeUpdate() == 1;
        } catch (SQLException failure) {
            throw storeFailure("yield", anchor.executionId(), failure);
        }
    }

    @Override
    public int yieldOwned(String ownerId, String ownerIncarnation) {
        requireOwner(ownerId, ownerIncarnation);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(YIELD_OWNED)) {
            statement.setString(1, ownerId);
            statement.setString(2, ownerIncarnation);
            return statement.executeUpdate();
        } catch (SQLException failure) {
            throw storeFailure("yield", ownerId, failure);
        }
    }

    @Override
    public List<ExecutionAnchor> claimExpired(
            String newOwnerId,
            String newOwnerIncarnation,
            Duration leaseDuration,
            int maxBatchSize) {
        requireOwner(newOwnerId, newOwnerIncarnation);
        requireDuration(leaseDuration);
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            Instant now = dbNow(connection);
            Instant deadline = now.plusMillis(leaseDuration.toMillis());
            List<ExpiredRow> expired = selectExpired(
                    connection, now, maxBatchSize);
            List<ExecutionAnchor> claimed = new ArrayList<>(expired.size());
            for (ExpiredRow row : expired) {
                long token = nextToken(connection);
                if (replaceExpired(connection, new ExpiredReplacement(
                        row.executionId, newOwnerId, newOwnerIncarnation,
                        token, now, deadline, row.anchorRow))) {
                    claimed.add(anchor(
                            row.executionId, newOwnerId, newOwnerIncarnation,
                            token, now, deadline));
                }
            }
            connection.commit();
            return List.copyOf(claimed);
        } catch (SQLException failure) {
            throw storeFailure("claim", newOwnerId, failure);
        }
    }

    @Override
    public Optional<ExecutionAnchor> current(String executionId) {
        requireNonBlank(executionId, "executionId");
        try (Connection connection = dataSource.getConnection()) {
            Instant now = dbNow(connection);
            try (PreparedStatement statement = connection.prepareStatement(SELECT_CURRENT)) {
                statement.setString(1, executionId);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return Optional.empty();
                    }
                    AnchorRow row = mapRow(result);
                    return row.expiresAt.isAfter(now)
                            ? Optional.of(anchor(executionId, row)) : Optional.empty();
                }
            }
        } catch (SQLException failure) {
            throw storeFailure("read", executionId, failure);
        }
    }

    private static AnchorRow selectForUpdate(
            Connection connection, String executionId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_FOR_UPDATE)) {
            statement.setString(1, executionId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? mapRow(result) : null;
            }
        }
    }

    private static boolean insert(
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

    private static boolean replaceExpired(Connection connection, ExpiredReplacement replacement)
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

    private static List<ExpiredRow> selectExpired(
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

    private static AnchorRow mapRow(ResultSet result) throws SQLException {
        return new AnchorRow(
                result.getString("owner_id"),
                result.getString("owner_incarnation"),
                result.getLong("fencing_token"),
                getInstant(result, "acquired_at"),
                getInstant(result, "expires_at"),
                result.getLong("version"));
    }

    private static long nextToken(Connection connection) throws SQLException {
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

    private static Instant dbNow(Connection connection) throws SQLException {
        String databaseProduct = connection.getMetaData().getDatabaseProductName();
        String sql = databaseProduct != null && databaseProduct.toLowerCase().contains("postgres")
                ? "SELECT clock_timestamp()"
                : "SELECT CURRENT_TIMESTAMP";
        try (PreparedStatement statement = connection.prepareStatement(
                sql);
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

    private static ExecutionAnchor anchor(String executionId, AnchorRow row) {
        return anchor(executionId, row.ownerId, row.ownerIncarnation,
                row.fencingToken, row.acquiredAt, row.expiresAt);
    }

    private static ExecutionAnchor anchor(
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

    private static void requireRequest(
            String executionId, String ownerId, String ownerIncarnation,
            Duration leaseDuration) {
        requireNonBlank(executionId, "executionId");
        requireOwner(ownerId, ownerIncarnation);
        requireDuration(leaseDuration);
    }

    private static void requireOwner(String ownerId, String ownerIncarnation) {
        requireNonBlank(ownerId, "ownerId");
        requireNonBlank(ownerIncarnation, "ownerIncarnation");
    }

    private static void requireDuration(Duration leaseDuration) {
        Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        if (leaseDuration.toMillis() <= 0) {
            throw new IllegalArgumentException("leaseDuration must be at least one millisecond");
        }
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static IllegalStateException storeFailure(
            String operation, String executionId, SQLException failure) {
        LOG.error("event=execution_anchor_store_failed operation={} executionId={} error={}",
                operation, executionId, failure.getMessage(), failure);
        return new IllegalStateException(
                "Execution-anchor store operation failed: " + operation, failure);
    }

    private record AnchorRow(
            String ownerId,
            String ownerIncarnation,
            long fencingToken,
            Instant acquiredAt,
            Instant expiresAt,
            long version) {}

    private record ExpiredRow(String executionId, AnchorRow anchorRow) {}

    private record ExpiredReplacement(
            String executionId,
            String ownerId,
            String ownerIncarnation,
            long token,
            Instant now,
            Instant deadline,
            AnchorRow row) {}
}
