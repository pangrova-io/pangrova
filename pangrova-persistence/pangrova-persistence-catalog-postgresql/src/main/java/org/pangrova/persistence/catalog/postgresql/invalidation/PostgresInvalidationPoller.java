package org.pangrova.persistence.catalog.postgresql.invalidation;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.api.cache.InvalidationEntry.InvalidationType;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.pangrova.metadata.api.cache.InvalidationEntry;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.cache.InvalidationProgressRegistry;

/**
 * Polls the {@code invalidation_log} JDBC table at a fixed interval and applies
 * cache invalidations to a local {@link MetadataCache}. This enables cross-node
 * L1 cache coherence in a multi-coordinator deployment without requiring a
 * message broker.
 *
 * <p>The canonical migration installs a BEFORE INSERT statement trigger that
 * serializes writers until commit, before allocating identity values. The ID
 * watermark is therefore commit ordered even for long transactions and direct
 * SQL writers. Timestamps are diagnostic only, never an event-selection fence.
 * Polling reads bounded pages through the primary-key index.</p>
 *
 * <p><strong>Lifecycle:</strong> Call {@link #validateSchema()} once at bootstrap to
 * validate the migrated table, then {@link #start()} to begin polling. Call {@link #close()}
 * during shutdown to stop the poller and release the thread pool.</p>
 *
 * <p><strong>Thread safety:</strong> All mutable state ({@code lastProcessedTimestamp})
 * is accessed only from the single polling thread. The {@link #writeInvalidation}
 * method is safe to call from any thread.</p>
 */
@ThreadSafe
public final class PostgresInvalidationPoller implements AutoCloseable,
        org.pangrova.persistence.catalog.api.invalidation.InvalidationLogPoller,
        org.pangrova.persistence.catalog.api.invalidation.InvalidationLogMaintenance {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresInvalidationPoller.class);

    static final long DEFAULT_POLL_INTERVAL_SECONDS = 5;

    static final int POLL_PAGE_SIZE = 1000;
    // Upper bound on pages drained per tick, so a continuously-writing producer
    // cannot hold the scheduler thread; the next tick resumes from the watermark.
    static final int MAX_PAGES_PER_POLL = 32;

    // ── DML ──────────────────────────────────────────────────────────

    private static final String SQL_POLL =
            "SELECT id, cache_key, invalidation_type, created_at, updated_at "
                    + "FROM invalidation_log "
                    + "WHERE id > ? "
                    + "ORDER BY id ASC LIMIT ?";

    private static final String SQL_INSERT =
            "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                    + "VALUES (?, ?, NOW(), NOW())";

    // ── Fields ───────────────────────────────────────────────────────

    private final DataSource dataSource;
    private final MetadataCache cache;
    private final long pollIntervalSeconds;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<Instant> lastProcessedTimestamp =
            new AtomicReference<>(Instant.EPOCH);
    private final AtomicLong lastProcessedId = new AtomicLong(0);
    private boolean started;
    private final InvalidationProgressRegistry progressRegistry;
    private final Optional<String> reportingCoordinatorId;

    /**
     * Creates a poller with the default 5-second poll interval.
     *
     * @param dataSource JDBC data source for the invalidation_log table
     * @param cache      local metadata cache to invalidate
     */
    public PostgresInvalidationPoller(DataSource dataSource, MetadataCache cache) {
        this(dataSource, cache, DEFAULT_POLL_INTERVAL_SECONDS);
    }

    /**
     * Creates a poller with a custom poll interval.
     *
     * @param dataSource          JDBC data source for the invalidation_log table
     * @param cache               local metadata cache to invalidate
     * @param pollIntervalSeconds interval between polls in seconds
     */
    public PostgresInvalidationPoller(DataSource dataSource, MetadataCache cache,
                                  long pollIntervalSeconds) {
        this(dataSource, cache, pollIntervalSeconds, null, Optional.empty());
    }

    /**
     * Binds cluster cleanup to the live membership floor before starting. A registered
     * query coordinator supplies its ID to resume and report. A maintenance-only reader
     * uses an empty ID and still observes every live query coordinator's watermark.
     */
    public PostgresInvalidationPoller(DataSource dataSource, MetadataCache cache,
            InvalidationProgressRegistry progressRegistry, Optional<String> reportingCoordinatorId) {
        this(dataSource, cache, DEFAULT_POLL_INTERVAL_SECONDS,
                Objects.requireNonNull(progressRegistry, "progressRegistry"), reportingCoordinatorId);
    }

    PostgresInvalidationPoller(DataSource dataSource, MetadataCache cache, long pollIntervalSeconds,
            InvalidationProgressRegistry progressRegistry, Optional<String> reportingCoordinatorId) {
        this.reportingCoordinatorId = Objects.requireNonNull(reportingCoordinatorId, "reportingCoordinatorId");
        if (reportingCoordinatorId.isPresent() && (reportingCoordinatorId.get().isBlank() || progressRegistry == null)) {
            throw new IllegalArgumentException("Reporting requires a coordinator ID and progress registry");
        }
        this.progressRegistry = progressRegistry;
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
        if (pollIntervalSeconds <= 0) {
            throw new IllegalArgumentException(
                    "pollIntervalSeconds must be positive, got " + pollIntervalSeconds);
        }
        this.pollIntervalSeconds = pollIntervalSeconds;

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                r -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("invalidation-poller");
                    t.setDaemon(true);
                    return t;
                });
        executor.setRemoveOnCancelPolicy(true);
        this.scheduler = executor;
    }

    // ── Schema Initialization ────────────────────────────────────────

    /**
     * Validates the canonical migrated schema and mandatory commit-order gate.
     * Does not create a second, competing schema during bootstrap.
     */
    public void validateSchema() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT id, cache_key, invalidation_type, created_at, updated_at "
                    + "FROM invalidation_log WHERE FALSE").close();
            try (ResultSet result = stmt.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_trigger "
                    + "WHERE tgrelid = 'invalidation_log'::regclass "
                    + "AND tgname = 'invalidation_append_commit_gate' AND tgenabled = 'O')")) {
                if (!result.next() || !result.getBoolean(1)) {
                    throw new SQLException("Canonical invalidation commit-order trigger is missing or disabled");
                }
            }
            LOG.info("event=invalidation_schema_validated");
        } catch (SQLException e) {
            throw new MetastoreException("Failed to validate invalidation_log schema", e);
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────

    /**
     * Starts the periodic polling task. Safe to call multiple times — subsequent
     * calls are no-ops if the scheduler is already running.
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        seedWatermarkFromRegistry();
        scheduler.scheduleAtFixedRate(this::poll, 0, pollIntervalSeconds, TimeUnit.SECONDS);
        started = true;
        LOG.info("event=invalidation_poller_started intervalSeconds={}", pollIntervalSeconds);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("event=invalidation_poller_shutdown_timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("event=invalidation_poller_shutdown_interrupted");
        }
        LOG.info("event=invalidation_poller_closed");
    }

    // ── Write Path (called by DDL execution) ─────────────────────────

    /**
     * Writes an invalidation entry to the log table. Called by the DDL execution
     * path on the coordinator that performed the DDL operation. Uses database
     * server time (NOW()) for both created_at and updated_at to avoid clock skew.
     *
     * @param cacheKey         the cache key to invalidate
     * @param invalidationType TABLE or CATALOG level invalidation
     */
    public void writeInvalidation(String cacheKey, InvalidationType invalidationType) {
        Objects.requireNonNull(cacheKey, "cacheKey must not be null");
        Objects.requireNonNull(invalidationType, "invalidationType must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            ps.setString(1, cacheKey);
            ps.setString(2, invalidationType.name());
            ps.executeUpdate();
            LOG.debug("event=invalidation_written cacheKey={} type={}",
                    cacheKey, invalidationType);
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to write invalidation entry for key=" + cacheKey, e);
        }
    }

    // ── Poll Logic ───────────────────────────────────────────────────

    /**
     * Polls a bounded page since the last processed committed ID and
     * applies them to the local cache. Each coordinator polls independently;
     * ID-based deduplication ensures each entry is applied at most once.
     * Invoked by the scheduler.
     */
    @org.pangrova.common.VisibleForTesting
    synchronized void poll() {
        try {
            int applied = 0;
            for (int page = 0; page < MAX_PAGES_PER_POLL; page++) {
                List<InvalidationEntry> entries = fetchNewEntries();
                if (entries.isEmpty()) {
                    break;
                }
                applied += applyPage(entries);
                reportLastProcessedToRegistry();
                if (entries.size() < POLL_PAGE_SIZE) {
                    break;
                }
            }
            if (applied > 0) {
                LOG.info("event=invalidation_poll_applied count={} lastTimestamp={} lastId={}",
                        applied, lastProcessedTimestamp.get(), lastProcessedId.get());
            }
        } catch (Exception e) {
            LOG.warn("event=invalidation_poll_failed lastTimestamp={} error={}",
                    lastProcessedTimestamp.get(), e.getMessage());
        }
    }

    private int applyPage(List<InvalidationEntry> entries) {
        int applied = 0;
        for (InvalidationEntry entry : entries) {
            if (entry.id() <= lastProcessedId.get()) {
                continue;
            }
            applyInvalidation(entry);
            lastProcessedId.set(entry.id());
            if (entry.updatedAt().isAfter(lastProcessedTimestamp.get())) {
                lastProcessedTimestamp.set(entry.updatedAt());
            }
            applied++;
        }
        return applied;
    }

    private List<InvalidationEntry> fetchNewEntries() {
        try (Connection conn = dataSource.getConnection()) {
            return queryEntriesWithWatermark(conn);
        } catch (SQLException e) {
            throw new MetastoreException("Failed to poll invalidation_log", e);
        }
    }

    /**
     * Executes the polling query against the supplied connection and returns
     * the resulting invalidation entries.
     */
    private List<InvalidationEntry> queryEntriesWithWatermark(Connection conn) throws SQLException {
        List<InvalidationEntry> entries = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SQL_POLL)) {
            ps.setLong(1, lastProcessedId.get());
            ps.setInt(2, POLL_PAGE_SIZE);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    entries.add(mapEntry(rs));
                }
            }
        }
        return entries;
    }

    private void applyInvalidation(InvalidationEntry entry) {
        switch (entry.invalidationType()) {
            case TABLE -> applyTableInvalidation(entry);
            case CATALOG -> applyCatalogInvalidation(entry);
            // Result-cache rows from an older deployment are ignored: the cache no longer exists.
            case RESULT_CACHE -> LOG.debug(
                    "event=invalidation_skip type=RESULT_CACHE reason=cache_removed key={}",
                    entry.cacheKey());
            default -> throw new IllegalStateException(
                    "Unhandled invalidation type: " + entry.invalidationType());
        }
    }

    private void applyTableInvalidation(InvalidationEntry entry) {
        String[] parts = entry.cacheKey().split("\u001F");
        if (parts.length == 3) {
            cache.invalidateTable(parts[0], parts[1], parts[2]);
            LOG.debug("event=invalidation_applied type=TABLE key={}", entry.cacheKey());
        } else {
            LOG.warn("event=invalidation_skip reason=malformed_table_key key={}", entry.cacheKey());
        }
    }

    private void applyCatalogInvalidation(InvalidationEntry entry) {
        cache.invalidateCatalog(entry.cacheKey());
        LOG.debug("event=invalidation_applied type=CATALOG key={}", entry.cacheKey());
    }

    private static InvalidationEntry mapEntry(ResultSet rs) throws SQLException {
        return new InvalidationEntry(
                rs.getLong("id"),
                rs.getString("cache_key"),
                InvalidationType.valueOf(rs.getString("invalidation_type")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    /**
     * Returns the last processed invalidation log ID. Useful for monitoring
     * and coordinator registry propagation reporting.
     */
    public long getLastProcessedId() {
        return lastProcessedId.get();
    }

    /**
     * Returns the last processed timestamp watermark. Useful for monitoring.
     */
    public Instant getLastProcessedTimestamp() {
        return lastProcessedTimestamp.get();
    }

    private void seedWatermarkFromRegistry() {
        if (reportingCoordinatorId.isEmpty()) {
            return;
        }
        String id = reportingCoordinatorId.orElseThrow();
        long reported = progressRegistry.lastProcessedInvalidationId(id);
        if (reported > 0) {
            lastProcessedId.set(reported);
            LOG.info("event=invalidation_watermark_seeded coordinatorId={} lastProcessedId={}", id, reported);
        }
    }

    private void reportLastProcessedToRegistry() {
        if (reportingCoordinatorId.isEmpty()) {
            return;
        }
        String id = reportingCoordinatorId.orElseThrow();
        try {
            progressRegistry.updateLastProcessedInvalidationId(id, lastProcessedId.get());
        } catch (Exception e) {
            LOG.debug("event=invalidation_registry_update_failed coordinatorId={} error={}",
                    id, e.getMessage());
        }
    }

    // ── TTL Cleanup ──────────────────────────────────────────────────

    private static final String SQL_CLEANUP =
            "DELETE FROM invalidation_log WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '24 hours' AND id <= ?";

    /**
     * Deletes invalidation entries older than 24 hours that have already been
     * processed (id &le; lastProcessedId). Called periodically by the maintenance
     * leader to prevent unbounded table growth.
     *
     * @return number of rows deleted
     */
    @Override
    public int cleanup() {
        long safetyBound = lastProcessedId.get();
        if (progressRegistry != null) {
            // Expiration is owned by registry maintenance. Never delete past any
            // still-registered coordinator, including an unknown progress fence.
            safetyBound = Math.min(safetyBound, progressRegistry.minimumReportedInvalidationId());
        }
        if (safetyBound <= 0) {
            LOG.debug("event=invalidation_cleanup_skipped reason=no_processed_entries");
            return 0;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_CLEANUP)) {
            ps.setLong(1, safetyBound);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=invalidation_cleanup deleted={} safetyBound={}", deleted, safetyBound);
            }
            return deleted;
        } catch (SQLException e) {
            LOG.warn("event=invalidation_cleanup_failed error={}", e.getMessage());
            return 0;
        }
    }
}
