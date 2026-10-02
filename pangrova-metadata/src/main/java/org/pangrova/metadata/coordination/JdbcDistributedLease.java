package org.pangrova.metadata.coordination;

import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.metadata.store.JdbcSupport.getInstant;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;

/**
 * JDBC-backed distributed lease using a single {@code distributed_lease} table.
 *
 * <p>Schema requirement:
 * <pre>
 *   lease_key   VARCHAR(255) PRIMARY KEY,
 *   holder_id   VARCHAR(255) NOT NULL,
 *   epoch       BIGINT       NOT NULL DEFAULT 0,
 *   acquired_at TIMESTAMPTZ(6) NOT NULL,
 *   expires_at  TIMESTAMPTZ(6) NOT NULL,
 *   version     BIGINT       NOT NULL DEFAULT 0
 * </pre>
 *
 * <p>Concurrency strategy: database-level row locking via {@code version}
 * column provides distributed mutual exclusion across JVM processes.
 * The database allocates fencing epochs while holding the lease row lock;
 * callers cannot supply or advance them.</p>
 */
public final class JdbcDistributedLease implements DistributedLease {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDistributedLease.class);

    private static final String SQL_SELECT =
            "SELECT holder_id, epoch, acquired_at, expires_at, version "
                    + "FROM distributed_lease WHERE lease_key = ? FOR UPDATE";

    private static final String SQL_SELECT_READ_ONLY =
            "SELECT holder_id, epoch, acquired_at, expires_at, version "
                    + "FROM distributed_lease WHERE lease_key = ?";

    private static final String SQL_INSERT =
            "INSERT INTO distributed_lease "
                    + "(lease_key, holder_id, epoch, acquired_at, expires_at, version) "
                    + "VALUES (?, ?, 1, ?, ?, 1) ON CONFLICT DO NOTHING";

    private static final String SQL_ACQUIRE_REPLACE =
            "UPDATE distributed_lease "
                    + "SET holder_id = ?, epoch = epoch + 1, acquired_at = ?, expires_at = ?, "
                    + "version = version + 1 "
                    + "WHERE lease_key = ? AND version = ? "
                    + "AND expires_at <= ?";

    private static final String SQL_RENEW =
            "UPDATE distributed_lease "
                    + "SET expires_at = ?, version = version + 1 "
                    + "WHERE lease_key = ? AND holder_id = ? AND epoch = ? AND version = ? "
                    + "AND expires_at >= ?";

    private static final String SQL_RELEASE =
            "UPDATE distributed_lease SET expires_at = CURRENT_TIMESTAMP, version = version + 1 "
                    + "WHERE lease_key = ? AND holder_id = ? AND epoch = ? "
                    + "AND expires_at > CURRENT_TIMESTAMP";

    private final DataSource dataSource;

    public JdbcDistributedLease(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public Optional<LeaseEntry> tryAcquire(
            String leaseKey, String holderId, Duration duration) {
        requireRequest(leaseKey, holderId, duration);
        return executeAcquire(leaseKey, holderId, duration);
    }

    private Optional<LeaseEntry> executeAcquire(
            String leaseKey, String holderId, Duration duration) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // A-LEASE-01: anchor timing on the DB server clock (single authoritative clock) so that
            // coordinator clock skew / GC pauses cannot disagree on expiry and cause split-brain.
            Instant now = dbNow(conn);
            Instant expiresAt = now.plusMillis(duration.toMillis());
            Optional<LeaseEntry> acquired = doAcquire(
                    conn, leaseKey, holderId, now, expiresAt);
            conn.commit();
            if (acquired.isPresent()) {
                LOG.info("event=lease_acquired leaseKey={} holderId={} epoch={} expiresAt={}",
                        leaseKey, holderId, acquired.get().epoch(),
                        acquired.get().expiresAt());
            }
            return acquired;
        } catch (SQLException e) {
            LOG.warn("event=lease_acquire_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<LeaseEntry> doAcquire(
            Connection conn, String leaseKey, String holderId,
            Instant now, Instant expiresAt) throws SQLException {
        LeaseRow existing = selectLease(conn, leaseKey);
        if (existing == null) {
            if (insertLease(conn, leaseKey, holderId, now, expiresAt)) {
                return Optional.of(new LeaseEntry(
                        leaseKey, holderId, 1, now, expiresAt));
            }
            existing = selectLease(conn, leaseKey);
            if (existing == null) {
                return Optional.empty();
            }
        }
        if (existing.expiresAt.isAfter(now)) {
            return existing.holderId.equals(holderId)
                    ? Optional.of(toEntry(leaseKey, existing)) : Optional.empty();
        }
        long nextEpoch = Math.incrementExact(existing.epoch);
        return replaceExpired(conn, leaseKey, holderId, nextEpoch,
                now, expiresAt, existing)
                ? Optional.of(new LeaseEntry(
                        leaseKey, holderId, nextEpoch, now, expiresAt))
                : Optional.empty();
    }

    private boolean replaceExpired(
            Connection conn, String leaseKey, String holderId, long nextEpoch,
            Instant now, Instant expiresAt, LeaseRow existing) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_ACQUIRE_REPLACE)) {
            ps.setString(1, holderId);
            setInstant(ps, 2, now);
            setInstant(ps, 3, expiresAt);
            ps.setString(4, leaseKey);
            ps.setLong(5, existing.version);
            setInstant(ps, 6, now);
            boolean replaced = ps.executeUpdate() > 0;
            if (replaced && nextEpoch <= existing.epoch) {
                throw new SQLException("Lease epoch did not advance");
            }
            return replaced;
        }
    }

    @Override
    public boolean tryRenew(
            String leaseKey, String holderId, long epoch, Duration duration) {
        requireRequest(leaseKey, holderId, duration);
        requireEpoch(epoch);
        return executeRenew(leaseKey, holderId, epoch, duration);
    }

    private boolean executeRenew(
            String leaseKey, String holderId, long epoch, Duration duration) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // A-LEASE-01: the DB server clock governs renewal validity and the new deadline.
            Instant now = dbNow(conn);
            Instant expiresAt = now.plusMillis(duration.toMillis());
            LeaseRow existing = selectLease(conn, leaseKey);
            if (existing == null || !existing.holderId.equals(holderId)
                    || existing.epoch != epoch) {
                conn.rollback();
                return false;
            }
            boolean renewed = renewLease(
                    conn, leaseKey, holderId, epoch, now, expiresAt, existing.version);
            conn.commit();
            if (renewed) {
                LOG.debug("event=lease_renewed leaseKey={} holderId={} expiresAt={}",
                        leaseKey, holderId, expiresAt);
            }
            return renewed;
        } catch (SQLException e) {
            LOG.warn("event=lease_renew_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean release(String leaseKey, String holderId, long epoch) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        requireEpoch(epoch);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_RELEASE)) {
            ps.setString(1, leaseKey);
            ps.setString(2, holderId);
            ps.setLong(3, epoch);
            int updated = ps.executeUpdate();
            if (updated > 0) {
                LOG.info("event=lease_released leaseKey={} holderId={}", leaseKey, holderId);
            }
            return updated > 0;
        } catch (SQLException e) {
            LOG.error("event=lease_release_error leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public Optional<LeaseEntry> currentHolder(String leaseKey) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        try (Connection conn = dataSource.getConnection()) {
            LeaseRow row = selectLeaseReadOnly(conn, leaseKey);
            if (row != null && row.expiresAt.isAfter(dbNow(conn))) {
                return Optional.of(new LeaseEntry(leaseKey, row.holderId, row.epoch,
                        row.acquiredAt, row.expiresAt));
            }
            return Optional.empty();
        } catch (SQLException e) {
            LOG.error("event=lease_query_error leaseKey={} error={}", leaseKey, e.getMessage(), e);
            return Optional.empty();
        }
    }

    private LeaseRow selectLease(Connection conn, String leaseKey) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_SELECT)) {
            ps.setString(1, leaseKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new LeaseRow(
                            rs.getString("holder_id"),
                            rs.getLong("epoch"),
                            getInstant(rs, "acquired_at"),
                            getInstant(rs, "expires_at"),
                            rs.getLong("version"));
                }
                return null;
            }
        }
    }

    private LeaseRow selectLeaseReadOnly(Connection conn, String leaseKey) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_SELECT_READ_ONLY)) {
            ps.setString(1, leaseKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new LeaseRow(
                            rs.getString("holder_id"),
                            rs.getLong("epoch"),
                            getInstant(rs, "acquired_at"),
                            getInstant(rs, "expires_at"),
                            rs.getLong("version"));
                }
                return null;
            }
        }
    }

    private boolean insertLease(Connection conn, String leaseKey, String holderId,
                                Instant now, Instant expiresAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setString(1, leaseKey);
            ps.setString(2, holderId);
            setInstant(ps, 3, now);
            setInstant(ps, 4, expiresAt);
            return ps.executeUpdate() > 0;
        }
    }

    private boolean renewLease(
            Connection conn, String leaseKey, String holderId, long epoch,
            Instant now, Instant expiresAt, long version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_RENEW)) {
            setInstant(ps, 1, expiresAt);
            ps.setString(2, leaseKey);
            ps.setString(3, holderId);
            ps.setLong(4, epoch);
            ps.setLong(5, version);
            setInstant(ps, 6, now);
            return ps.executeUpdate() > 0;
        }
    }

    private static LeaseEntry toEntry(String leaseKey, LeaseRow row) {
        return new LeaseEntry(leaseKey, row.holderId, row.epoch,
                row.acquiredAt, row.expiresAt);
    }

    private static void requireRequest(
            String leaseKey, String holderId, Duration duration) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        if (leaseKey.isBlank() || holderId.isBlank() || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "leaseKey and holderId must be non-blank and duration must be positive");
        }
    }

    private static void requireEpoch(long epoch) {
        if (epoch <= 0) {
            throw new IllegalArgumentException("epoch must be positive");
        }
    }

    /**
     * Reads the database server's current time. A-LEASE-01: every lease expiry decision must use a
     * single authoritative clock (the DB) rather than each coordinator's local JVM clock. Otherwise
     * clock skew or a GC pause on one coordinator can make it disagree with peers about whether a
     * lease has expired, opening a double-active-leader (split-brain) window.
     */
    private static Instant dbNow(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT CURRENT_TIMESTAMP");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                Instant now = getInstant(rs, 1);
                if (now != null) {
                    return now;
                }
            }
            throw new SQLException("CURRENT_TIMESTAMP query returned no row");
        }
    }

    private record LeaseRow(String holderId, long epoch, Instant acquiredAt,
                            Instant expiresAt, long version) {}
}
