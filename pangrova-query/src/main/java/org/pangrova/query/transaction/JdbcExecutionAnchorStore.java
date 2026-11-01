package org.pangrova.query.transaction;

import org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.AnchorRow;
import org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.ExpiredReplacement;
import org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.ExpiredRow;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.ExecutionAnchorStore;

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

import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.RELEASE;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.RENEW_OWNED;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.SELECT_CURRENT;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.YIELD;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.anchor;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.dbNow;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.insert;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.lockAcquisition;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.lockOwned;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.mapRow;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.nextToken;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.refreshInserted;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.replaceExpired;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.selectExpired;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.selectForUpdate;
import static org.pangrova.query.transaction.JdbcExecutionAnchorStoreMappings.storeFailure;
import static org.pangrova.query.store.jdbc.JdbcQuerySupport.setInstant;

/** PostgreSQL execution ownership with database-clock leases and monotonic per-execution fencing. */
public final class JdbcExecutionAnchorStore implements ExecutionAnchorStore {

    private final DataSource dataSource;

    public JdbcExecutionAnchorStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /** Holds the exact live owner on a caller-owned publication transaction. Sample time only after its lock. */
    public static ExecutionAnchor requireCurrent(Connection connection, ExecutionAnchor expected) throws SQLException {
        return JdbcExecutionAnchorStoreMappings.requireCurrent(connection, expected);
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
        if (row == null || row.expiresAt().isAfter(now)) {
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
                        row.executionId(), newOwnerId, newOwnerIncarnation,
                        token, now, deadline, row.anchorRow()))) {
                    claimed.add(anchor(
                            row.executionId(), newOwnerId, newOwnerIncarnation,
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
                    return row.expiresAt().isAfter(dbNow(connection))
                            ? Optional.of(anchor(executionId, row)) : Optional.empty();
                }
            }
        } catch (SQLException failure) {
            throw storeFailure("read", executionId, failure);
        }
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
}
