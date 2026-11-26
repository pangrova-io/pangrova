package org.pangrova.persistence.query.postgresql.coordination;

import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.coordination.ExecutionAnchorStore;
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
import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.pangrova.common.util.Preconditions.requireText;

/** PostgreSQL execution ownership with database-clock leases and monotonic per-execution fencing. */
public final class JdbcExecutionAnchorStore implements ExecutionAnchorStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExecutionAnchorStore.class);
    private static final int ACQUISITION_LOCK_NAMESPACE = 0x5047414E;

    private static final String SELECT_FOR_UPDATE =
            "SELECT owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version "
                    + "FROM execution_anchor WHERE execution_id = ? FOR UPDATE";
    private static final String SELECT_CURRENT =
            "SELECT owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version "
                    + "FROM execution_anchor WHERE execution_id = ? AND expires_at > clock_timestamp()";
    private static final String INSERT_ANCHOR = """
            WITH lease_clock AS MATERIALIZED (SELECT clock_timestamp() AS sampled_at)
            INSERT INTO execution_anchor
                (execution_id, owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version)
            SELECT ?, ?, ?, nextval('execution_anchor_token_seq'), sampled_at, sampled_at + CAST(? AS interval), 1
            FROM lease_clock
            WHERE NOT EXISTS (SELECT 1 FROM execution_read_window w
                WHERE w.execution_id=? AND w.protected_until>clock_timestamp())
            ON CONFLICT DO NOTHING
            RETURNING fencing_token
            """;
    private static final String REPLACE_EXPIRED =
            "UPDATE execution_anchor SET owner_id=?, owner_incarnation=?, fencing_token=?, "
                    + "acquired_at=?, expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND fencing_token=? AND version=? AND expires_at<=? "
                    + "AND NOT EXISTS (SELECT 1 FROM execution_read_window w "
                    + "WHERE w.execution_id=execution_anchor.execution_id "
                    + "AND w.protected_until>clock_timestamp())";
    private static final String RENEW_OWNED =
            "UPDATE execution_anchor SET expires_at=?, version=version+1 "
                    + "WHERE owner_id=? AND owner_incarnation=? AND expires_at>? AND execution_id=ANY(?) "
                    + "RETURNING execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version";
    private static final String RELEASE = """
            WITH owner(execution_id, owner_id, owner_incarnation, fencing_token, retired_at) AS MATERIALIZED (
                VALUES (CAST(? AS text), CAST(? AS text), CAST(? AS text), CAST(? AS bigint), clock_timestamp())
            ),
            retirement AS MATERIALIZED (
                SELECT owner.*, EXISTS (
                    SELECT 1 FROM write_commit_journal j
                    WHERE j.query_id=owner.execution_id AND j.state='PENDING'
                      AND NOT EXISTS (
                        SELECT 1 FROM write_commit_quarantine q
                        WHERE q.query_id=j.query_id AND q.commit_fingerprint=j.commit_fingerprint
                      )
                ) AS recover_write FROM owner
            ),
            retained AS (
                UPDATE execution_anchor a SET expires_at=r.retired_at, version=a.version+1
                FROM retirement r
                WHERE a.execution_id=r.execution_id AND a.owner_id=r.owner_id
                  AND a.owner_incarnation=r.owner_incarnation AND a.fencing_token=r.fencing_token
                  AND a.expires_at>r.retired_at AND r.recover_write
                RETURNING 1
            ),
            removed AS (
                DELETE FROM execution_anchor a USING retirement r
                WHERE a.execution_id=r.execution_id AND a.owner_id=r.owner_id
                  AND a.owner_incarnation=r.owner_incarnation AND a.fencing_token=r.fencing_token
                  AND a.expires_at>r.retired_at AND NOT r.recover_write
                RETURNING 1
            )
            SELECT EXISTS (SELECT 1 FROM retained) OR EXISTS (SELECT 1 FROM removed)
            """;
    private static final String YIELD =
            "UPDATE execution_anchor SET expires_at=?, version=version+1 "
                    + "WHERE execution_id=? AND owner_id=? AND owner_incarnation=? "
                    + "AND fencing_token=? AND expires_at>?";
    private static final String LOCK_OWNED =
            "SELECT execution_id FROM execution_anchor WHERE owner_id=? AND owner_incarnation=? "
                    + "ORDER BY execution_id FOR UPDATE";
    private static final String REFRESH_INSERTED = """
            WITH lease_clock AS MATERIALIZED (SELECT clock_timestamp() AS sampled_at)
            UPDATE execution_anchor
            SET acquired_at=sampled_at, expires_at=sampled_at + CAST(? AS interval)
            FROM lease_clock
            WHERE execution_id=? AND fencing_token=? AND version=1
            RETURNING owner_id, owner_incarnation, fencing_token, acquired_at, expires_at, version
            """;
    private static final String PUBLICATION_REVISION =
            "COALESCE((SELECT epoch FROM materialization.registry_epochs WHERE tenant_id=?), 0)";
    private static final String REFRESH_INSERTED_QUERY =
            REFRESH_INSERTED.stripTrailing() + ", " + PUBLICATION_REVISION + " AS publication_revision";
    private static final String SELECT_PUBLICATION_REVISION = "SELECT " + PUBLICATION_REVISION;

    private static final String SELECT_EXPIRED =
            "SELECT execution_id, owner_id, owner_incarnation, fencing_token, "
                    + "acquired_at, expires_at, version FROM execution_anchor "
                    + "WHERE expires_at<=? AND NOT EXISTS (SELECT 1 FROM execution_read_window w "
                    + "WHERE w.execution_id=execution_anchor.execution_id "
                    + "AND w.protected_until>clock_timestamp()) "
                    + "ORDER BY expires_at, execution_id LIMIT ? FOR UPDATE SKIP LOCKED";
    private static final String REAP_READ_WINDOWS = """
            DELETE FROM execution_read_window WHERE read_id IN (
                SELECT read_id FROM execution_read_window
                WHERE protected_until<=clock_timestamp()
                ORDER BY protected_until, read_id LIMIT ? FOR UPDATE SKIP LOCKED
            )
            """;

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
        return acquire(executionId, ownerId, ownerIncarnation, leaseDuration, null)
                .map(QueryAcquisition::anchor);
    }

    @Override
    public Optional<QueryAcquisition> tryAcquireQuery(String executionId, String ownerId,
            String ownerIncarnation, Duration leaseDuration, String tenantId) {
        requireText(tenantId, "tenantId");
        return acquire(executionId, ownerId, ownerIncarnation, leaseDuration, tenantId);
    }

    private Optional<QueryAcquisition> acquire(String executionId, String ownerId, String ownerIncarnation,
            Duration leaseDuration, String tenantId) {
        requireRequest(executionId, ownerId, ownerIncarnation, leaseDuration);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            lockAcquisition(connection, executionId);
            var result = acquireLocked(connection, executionId, ownerId, ownerIncarnation, leaseDuration, tenantId);
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

    private Optional<QueryAcquisition> acquireLocked(
            Connection connection,
            String executionId,
            String ownerId,
            String ownerIncarnation,
            Duration leaseDuration, String tenantId) throws SQLException {
        AnchorRow row = selectForUpdate(connection, executionId);
        if (row == null) {
            String interval = leaseDuration.toMillis() + " milliseconds";
            OptionalLong token = insert(connection, executionId, ownerId, ownerIncarnation, interval);
            if (token.isPresent()) {
                // INSERT may wait for an invisible competing INSERT that rolls back.
                // Our row is now exclusively owned; grant from time after that wait.
                return Optional.of(refreshInserted(connection, executionId, token.getAsLong(), interval, tenantId));
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
        return Optional.of(new QueryAcquisition(anchor(executionId, ownerId, ownerIncarnation, token, now, deadline),
                tenantId == null ? OptionalLong.empty() : currentPublicationRevision(connection, tenantId)));
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
            AnchorRow current = selectForUpdate(connection, anchor.executionId());
            // Start a fresh statement after acquiring the row lock. A writer may
            // have committed its recovery intent while this transaction waited.
            boolean released = current != null && releaseLockedAnchor(connection, anchor);
            connection.commit();
            return released;
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
            boolean yielded = yieldLockedAnchor(connection, anchor, now);
            connection.commit();
            return yielded;
        } catch (SQLException failure) {
            throw storeFailure("yield", anchor.executionId(), failure);
        }
    }

    private static boolean releaseLockedAnchor(Connection connection, ExecutionAnchor anchor)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(RELEASE)) {
            bindOwner(statement, 1, anchor);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }

    private static boolean yieldLockedAnchor(Connection connection, ExecutionAnchor anchor, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(YIELD)) {
            setInstant(statement, 1, now);
            int next = bindOwner(statement, 2, anchor);
            setInstant(statement, next, now);
            return statement.executeUpdate() == 1;
        }
    }

    private static int bindOwner(PreparedStatement statement, int parameter, ExecutionAnchor anchor)
            throws SQLException {
        statement.setString(parameter++, anchor.executionId());
        statement.setString(parameter++, anchor.ownerId());
        statement.setString(parameter++, anchor.ownerIncarnation());
        statement.setLong(parameter++, anchor.fencingToken());
        return parameter;
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
            reapReadWindows(connection, maxBatchSize);
            List<ExpiredRow> expired = selectExpired(connection, dbNow(connection), maxBatchSize);
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
        requireText(executionId, "executionId");
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(SELECT_CURRENT)) {
                statement.setString(1, executionId);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? Optional.of(anchor(executionId, mapRow(result))) : Optional.empty();
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

    private static QueryAcquisition refreshInserted(Connection connection, String executionId, long token,
            String interval, String tenantId) throws SQLException {
        String sql = tenantId == null ? REFRESH_INSERTED : REFRESH_INSERTED_QUERY;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, interval);
            statement.setString(2, executionId);
            statement.setLong(3, token);
            if (tenantId != null) {
                statement.setString(4, tenantId);
            }
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("Inserted execution anchor lost its transaction ownership");
                }
                OptionalLong revision = tenantId == null ? OptionalLong.empty()
                        : OptionalLong.of(result.getLong("publication_revision"));
                return new QueryAcquisition(anchor(executionId, mapRow(result)), revision);
            }
        }
    }

    private static OptionalLong currentPublicationRevision(Connection connection, String tenantId)
            throws SQLException {
        try (var statement = connection.prepareStatement(SELECT_PUBLICATION_REVISION)) {
            statement.setString(1, tenantId);
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("Missing materialization publication revision");
                }
                return OptionalLong.of(result.getLong(1));
            }
        }
    }

    private static OptionalLong insert(
            Connection connection,
            String executionId,
            String ownerId,
            String ownerIncarnation,
            String interval) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_ANCHOR)) {
            statement.setString(1, executionId);
            statement.setString(2, ownerId);
            statement.setString(3, ownerIncarnation);
            statement.setString(4, interval);
            statement.setString(5, executionId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? OptionalLong.of(result.getLong("fencing_token")) : OptionalLong.empty();
            }
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

    private static void reapReadWindows(Connection connection, int limit) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(REAP_READ_WINDOWS)) {
            statement.setInt(1, limit);
            statement.executeUpdate();
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
        String sql = product != null && product.toLowerCase(Locale.ROOT).contains("h2")
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
        requireText(executionId, "executionId");
        requireOwner(ownerId, ownerIncarnation);
        requireDuration(leaseDuration);
    }

    private static void requireOwner(String ownerId, String ownerIncarnation) {
        requireText(ownerId, "ownerId");
        requireText(ownerIncarnation, "ownerIncarnation");
    }

    private static void requireDuration(Duration leaseDuration) {
        Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        if (leaseDuration.toMillis() <= 0) {
            throw new IllegalArgumentException("leaseDuration must be at least one millisecond");
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
