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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * JDBC-backed distributed lease using a single {@code distributed_lease} table.
 *
 * <p>Schema requirement:
 * <pre>
 *   lease_key   VARCHAR(255) PRIMARY KEY,
 *   holder_id   VARCHAR(255) NOT NULL,
 *   epoch       BIGINT       NOT NULL DEFAULT 0,
 *   acquired_at TIMESTAMP    NOT NULL,
 *   expires_at  TIMESTAMP    NOT NULL,
 *   version     BIGINT       NOT NULL DEFAULT 0
 * </pre>
 *
 * <p>Concurrency strategy: database-level row locking via {@code version}
 * column provides distributed mutual exclusion across JVM processes.
 * Epoch fencing allows a new leader to force-replace a stale holder.</p>
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
                    + "VALUES (?, ?, ?, ?, ?, 1)";

    private static final String SQL_ACQUIRE_REPLACE =
            "UPDATE distributed_lease "
                    + "SET holder_id = ?, epoch = ?, acquired_at = ?, expires_at = ?, "
                    + "version = version + 1 "
                    + "WHERE lease_key = ? AND version = ? "
                    + "AND (expires_at < ? OR holder_id = ? OR epoch < ?)";

    private static final String SQL_RENEW =
            "UPDATE distributed_lease "
                    + "SET acquired_at = ?, expires_at = ?, version = version + 1 "
                    + "WHERE lease_key = ? AND holder_id = ? AND version = ? "
                    + "AND expires_at >= ?";

    private static final String SQL_RELEASE =
            "DELETE FROM distributed_lease WHERE lease_key = ? AND holder_id = ?";

    private static final String SQL_EXPIRE_STALE =
            "DELETE FROM distributed_lease WHERE expires_at < CURRENT_TIMESTAMP";

    private final DataSource dataSource;

    public JdbcDistributedLease(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public boolean tryAcquire(String leaseKey, String holderId, long epoch, Duration duration) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        return executeAcquire(leaseKey, holderId, epoch, duration);
    }

    private boolean executeAcquire(String leaseKey, String holderId, long epoch, Duration duration) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // A-LEASE-01: anchor timing on the DB server clock (single authoritative clock) so that
            // coordinator clock skew / GC pauses cannot disagree on expiry and cause split-brain.
            Instant now = dbNow(conn);
            Instant expiresAt = now.plus(duration);
            boolean acquired = doAcquire(conn, leaseKey, holderId, epoch, now, expiresAt);
            conn.commit();
            if (acquired) {
                LOG.info("event=lease_acquired leaseKey={} holderId={} epoch={} expiresAt={}",
                        leaseKey, holderId, epoch, expiresAt);
            }
            return acquired;
        } catch (SQLException e) {
            LOG.warn("event=lease_acquire_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.getMessage());
            return false;
        }
    }

    private boolean doAcquire(Connection conn, String leaseKey, String holderId,
                              long epoch, Instant now, Instant expiresAt) throws SQLException {
        LeaseRow existing = selectLease(conn, leaseKey);
        if (existing == null) {
            if (insertLease(conn, leaseKey, holderId, epoch, now, expiresAt)) {
                return true;
            }
            // INSERT failed due to concurrent insert (PK conflict) — re-read with FOR UPDATE
            existing = selectLease(conn, leaseKey);
            if (existing == null) {
                return false; // unexpected: row vanished between insert and select
            }
        }
        return replaceExpiredOrFenced(conn, leaseKey, holderId, epoch, now, expiresAt, existing);
    }

    private boolean replaceExpiredOrFenced(Connection conn, String leaseKey, String holderId,
                                           long epoch, Instant now, Instant expiresAt,
                                           LeaseRow existing) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_ACQUIRE_REPLACE)) {
            ps.setString(1, holderId);
            ps.setLong(2, epoch);
            ps.setTimestamp(3, Timestamp.from(now));
            ps.setTimestamp(4, Timestamp.from(expiresAt));
            ps.setString(5, leaseKey);
            ps.setLong(6, existing.version);
            ps.setTimestamp(7, Timestamp.from(now));  // expires_at < now
            ps.setString(8, holderId);                // holder_id = self (re-entrant)
            ps.setLong(9, epoch);                     // epoch < new epoch (fencing)
            return ps.executeUpdate() > 0;
        }
    }

    @Override
    public boolean tryRenew(String leaseKey, String holderId, Duration duration) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        return executeRenew(leaseKey, holderId, duration);
    }

    private boolean executeRenew(String leaseKey, String holderId, Duration duration) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // A-LEASE-01: the DB server clock governs renewal validity and the new deadline.
            Instant now = dbNow(conn);
            Instant expiresAt = now.plus(duration);
            LeaseRow existing = selectLease(conn, leaseKey);
            if (existing == null || !existing.holderId.equals(holderId)) {
                conn.rollback();
                return false;
            }
            boolean renewed = renewLease(conn, leaseKey, holderId, now, expiresAt, existing.version);
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

    /**
     * Starts a background task that periodically renews the lease before it expires.
     * The renewal runs at half the lease duration interval to ensure the lease
     * is refreshed well before expiration. If a renewal fails, the failure is
     * logged but the scheduler continues attempting renewals.
     *
     * @param scheduler the executor to schedule renewal tasks on
     * @param leaseKey the lease key to renew
     * @param holderId the holder ID that owns the lease
     * @param leaseDuration the full lease duration (renewal runs at half this interval)
     * @return a ScheduledFuture that can be cancelled to stop auto-renewal
     */
    public ScheduledFuture<?> startAutoRenewal(ScheduledExecutorService scheduler,
                                               String leaseKey, String holderId,
                                               Duration leaseDuration) {
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        long renewalIntervalMs = leaseDuration.toMillis() / 2;
        return scheduler.scheduleAtFixedRate(() -> {
            boolean renewed = tryRenew(leaseKey, holderId, leaseDuration);
            if (!renewed) {
                LOG.warn("event=auto_renewal_failed leaseKey={} holderId={}", leaseKey, holderId);
            }
        }, renewalIntervalMs, renewalIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean release(String leaseKey, String holderId) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_RELEASE)) {
            ps.setString(1, leaseKey);
            ps.setString(2, holderId);
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

    @Override
    public int expireStale() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_EXPIRE_STALE)) {
            // A-LEASE-01: compare against the DB server clock (CURRENT_TIMESTAMP), not the caller's.
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=lease_stale_expired count={}", deleted);
            }
            return deleted;
        } catch (SQLException e) {
            LOG.error("event=lease_expire_stale_error error={}", e.getMessage(), e);
            return 0;
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
                            rs.getTimestamp("acquired_at").toInstant(),
                            rs.getTimestamp("expires_at").toInstant(),
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
                            rs.getTimestamp("acquired_at").toInstant(),
                            rs.getTimestamp("expires_at").toInstant(),
                            rs.getLong("version"));
                }
                return null;
            }
        }
    }

    private boolean insertLease(Connection conn, String leaseKey, String holderId,
                                long epoch, Instant now, Instant expiresAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setString(1, leaseKey);
            ps.setString(2, holderId);
            ps.setLong(3, epoch);
            ps.setTimestamp(4, Timestamp.from(now));
            ps.setTimestamp(5, Timestamp.from(expiresAt));
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            // SQLState 23xxx = integrity constraint violation (duplicate key)
            if (e.getSQLState() != null && e.getSQLState().startsWith("23")) {
                LOG.debug("event=lease_insert_conflict leaseKey={} holderId={}", leaseKey, holderId);
                return false;
            }
            throw e;
        }
    }

    private boolean renewLease(Connection conn, String leaseKey, String holderId,
                               Instant now, Instant expiresAt, long version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_RENEW)) {
            ps.setTimestamp(1, Timestamp.from(now));
            ps.setTimestamp(2, Timestamp.from(expiresAt));
            ps.setString(3, leaseKey);
            ps.setString(4, holderId);
            ps.setLong(5, version);
            ps.setTimestamp(6, Timestamp.from(now));  // expires_at >= now
            return ps.executeUpdate() > 0;
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
                Timestamp ts = rs.getTimestamp(1);
                if (ts != null) {
                    return ts.toInstant();
                }
            }
            throw new SQLException("CURRENT_TIMESTAMP query returned no row");
        }
    }

    private record LeaseRow(String holderId, long epoch, Instant acquiredAt,
                            Instant expiresAt, long version) {}
}
