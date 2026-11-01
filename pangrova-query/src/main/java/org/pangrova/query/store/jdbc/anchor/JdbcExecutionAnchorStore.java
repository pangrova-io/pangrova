package org.pangrova.query.store.jdbc.anchor;

import org.pangrova.spi.query.coordination.ExecutionAnchor;
import org.pangrova.persistence.api.query.coordination.ExecutionAnchorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** PostgreSQL execution ownership with database-clock leases and monotonic per-execution fencing. */
public final class JdbcExecutionAnchorStore implements ExecutionAnchorStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExecutionAnchorStore.class);
    private static final int ACQUISITION_LOCK_NAMESPACE = 0x5047414E;

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
    private static final String REPLACE_EXPIRED =
            "UPDATE execution_anchor SET owner_id=?, owner_incarnation=?, fencing_token=?, "
                    + "acquired_at=?, expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND fencing_token=? AND version=? AND expires_at<=?";
    private static final String RENEW_OWNED =
            "UPDATE execution_anchor SET expires_at=?, version=version+1 "
                    + "WHERE owner_id=? AND owner_incarnation=? AND expires_at>? AND execution_id=ANY(?) "
                    + "RETURNING execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version";
    private static final String RELEASE =
            "DELETE FROM execution_anchor WHERE execution_id=? AND owner_id=? "
                    + "AND owner_incarnation=? AND fencing_token=? AND expires_at>?";
    private static final String YIELD =
            "UPDATE execution_anchor SET expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND owner_id=? AND owner_incarnation=? "
                    + "AND fencing_token=? AND expires_at>?";
    private static final String LOCK_OWNED =
            "SELECT execution_id FROM execution_anchor WHERE owner_id=? AND owner_incarnation=? "
                    + "ORDER BY execution_id FOR UPDATE";
    private static final String REFRESH_INSERTED =
            "UPDATE execution_anchor SET acquired_at=?, expires_at=? "
                    + "WHERE execution_id=? AND fencing_token=? AND version=1";
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
            lockAcquisition(connection, executionId);
            Optional<ExecutionAnchor> result = acquireLocked(
                    connection, executionId, ownerId, ownerIncarnation, leaseDuration);
            connection.commit();
            return result;
        } catch (SQLException failure) {
            throw storeFailure("acquire", executionId, failure);
        }
    }

    private static void lockAcquisition(Connection connection, String executionId) throws SQLException {
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

    private Optional<ExecutionAnchor> acquireLocked(
            Connection connection,
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration) throws SQLException {
        AnchorRow row = selectForUpdate(connection, executionId);
        if (row == null) {
            long token = nextToken(connection);
            Instant now = dbNow(connection);
            if (insert(connection, executionId, ownerId, ownerIncarnation, token,
                    now, now.plusMillis(leaseDuration.toMillis()))) {
                // INSERT may wait for an invisible competing INSERT that rolls back.
                // Our row is now exclusively owned; grant from time after that wait.
                now = dbNow(connection);
                Instant deadline = now.plusMillis(leaseDuration.toMillis());
                refreshInserted(connection, executionId, token, now, deadline);
                return Optional.of(anchor(
                        executionId, ownerId, ownerIncarnation, token, now, deadline));
            }
            row = selectForUpdate(connection, executionId);
        }
        Instant now = dbNow(connection);
        if (row == null || row.expiresAt.isAfter(now)) {
            return Optional.empty();
        }
        long token = nextToken(connection);
        now = dbNow(connection);
        Instant deadline = now.plusMillis(leaseDuration.toMillis());
        if (!replaceExpired(connection, new ExpiredReplacement(
                executionId, ownerId, ownerIncarnation, token, now, deadline, row))) {
            return Optional.empty();
        }
        return Optional.of(anchor(
                executionId, ownerId, ownerIncarnation, token, now, deadline));
    }

    @Override
    public List<ExecutionAnchor> renewOwned(
            String ownerId, String ownerIncarnation, Duration leaseDuration) {
        requireOwner(ownerId, ownerIncarnation);
        requireDuration(leaseDuration);
        return updateOwnedLeases(ownerId, ownerIncarnation, Optional.of(leaseDuration)).receipts();
    }

    private record LeaseUpdate(int count, List<ExecutionAnchor> receipts) {}

    private LeaseUpdate updateOwnedLeases(
            String ownerId, String ownerIncarnation, Optional<Duration> extension) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            List<String> locked = lockOwned(connection, ownerId, ownerIncarnation);
            Instant now = dbNow(connection);
            Instant deadline = extension.map(duration -> now.plusMillis(duration.toMillis())).orElse(now);
            java.sql.Array identities = connection.createArrayOf("text", locked.toArray(String[]::new));
            try (PreparedStatement statement = connection.prepareStatement(RENEW_OWNED)) {
                setInstant(statement, 1, deadline);
                statement.setString(2, ownerId);
                statement.setString(3, ownerIncarnation);
                setInstant(statement, 4, now);
                statement.setArray(5, identities);
                List<ExecutionAnchor> renewed = new ArrayList<>();
                int updated = 0;
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        updated++;
                        if (extension.isPresent()) {
                            renewed.add(anchor(rows.getString("execution_id"), mapRow(rows)));
                        }
                    }
                }
                connection.commit();
                return new LeaseUpdate(updated, List.copyOf(renewed));
            } finally {
                identities.free();
            }
        } catch (SQLException failure) {
            throw storeFailure(extension.isPresent() ? "renew" : "yield", ownerId, failure);
        }
    }

    @Override
    public boolean release(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            selectForUpdate(connection, anchor.executionId());
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
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            selectForUpdate(connection, anchor.executionId());
            Instant now = dbNow(connection);
            try (PreparedStatement statement = connection.prepareStatement(YIELD)) {
                setInstant(statement, 1, now);
                statement.setString(2, anchor.executionId());
                statement.setString(3, anchor.ownerId());
                statement.setString(4, anchor.ownerIncarnation());
                statement.setLong(5, anchor.fencingToken());
                setInstant(statement, 6, now);
                boolean yielded = statement.executeUpdate() == 1;
                connection.commit();
                return yielded;
            }
        } catch (SQLException failure) {
            throw storeFailure("yield", anchor.executionId(), failure);
        }
    }

    @Override
    public int yieldOwned(String ownerId, String ownerIncarnation) {
        requireOwner(ownerId, ownerIncarnation);
        return updateOwnedLeases(ownerId, ownerIncarnation, Optional.empty()).count();
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
            List<ExpiredRow> expired = selectExpired(
                    connection, dbNow(connection), maxBatchSize);
            List<ExecutionAnchor> claimed = new ArrayList<>(expired.size());
            for (ExpiredRow row : expired) {
                long token = nextToken(connection);
                Instant now = dbNow(connection);
                Instant deadline = now.plusMillis(leaseDuration.toMillis());
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
            try (PreparedStatement statement = connection.prepareStatement(SELECT_CURRENT)) {
                statement.setString(1, executionId);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return Optional.empty();
                    }
                    AnchorRow row = mapRow(result);
                    return row.expiresAt.isAfter(dbNow(connection))
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

    private static List<String> lockOwned(Connection connection, String ownerId, String ownerIncarnation)
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

    private static void refreshInserted(Connection connection, String executionId, long token,
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

    private static Instant dbNow(Connection connection) throws SQLException {
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

    private static Instant canonicalInstant(Instant value) {
        return Objects.requireNonNull(value, "value is null").truncatedTo(ChronoUnit.MICROS);
    }

    private static void setInstant(PreparedStatement statement, int index, Instant value)
            throws SQLException {
        statement.setObject(index, canonicalInstant(value).atOffset(ZoneOffset.UTC));
    }

    private static Instant getInstant(ResultSet result, String column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : canonicalInstant(value.toInstant());
    }

    private static Instant getInstant(ResultSet result, int column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : canonicalInstant(value.toInstant());
    }
}
