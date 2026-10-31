package org.pangrova.metadata.store.jdbc.cache;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.api.cache.InvalidationEntry.InvalidationType;
import org.pangrova.metadata.exception.MetastoreException;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.pangrova.metadata.api.cache.InvalidationEntry;
import org.pangrova.metadata.api.cache.MetadataCache;

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
 * <p><strong>Lifecycle:</strong> Call {@link #initSchema()} once at bootstrap to
 * validate the migrated table, then {@link #start()} to begin polling. Call {@link #close()}
 * during shutdown to stop the poller and release the thread pool.</p>
 *
 * <p><strong>Thread safety:</strong> All mutable state ({@code lastProcessedTimestamp})
 * is accessed only from the single polling thread. The {@link #writeInvalidation}
 * method is safe to call from any thread.</p>
 */
@ThreadSafe
public final class JdbcInvalidationPoller implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcInvalidationPoller.class);

    static final long DEFAULT_POLL_INTERVAL_SECONDS = 5;

    static final int POLL_PAGE_SIZE = 1000;

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
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile org.pangrova.metadata.api.cache.InvalidationProgressRegistry progressRegistry;
    private volatile String coordinatorId;
    private volatile java.util.function.Consumer<String> resultCacheInvalidator;

    /**
     * Creates a poller with the default 5-second poll interval.
     *
     * @param dataSource JDBC data source for the invalidation_log table
     * @param cache      local metadata cache to invalidate
     */
    public JdbcInvalidationPoller(DataSource dataSource, MetadataCache cache) {
        this(dataSource, cache, DEFAULT_POLL_INTERVAL_SECONDS);
    }

    /**
     * Creates a poller with a custom poll interval.
     *
     * @param dataSource          JDBC data source for the invalidation_log table
     * @param cache               local metadata cache to invalidate
     * @param pollIntervalSeconds interval between polls in seconds
     */
    public JdbcInvalidationPoller(DataSource dataSource, MetadataCache cache,
                                  long pollIntervalSeconds) {
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

    @org.pangrova.common.VisibleForTesting
    JdbcInvalidationPoller(DataSource dataSource, MetadataCache cache,
                           long pollIntervalSeconds,
                           ScheduledExecutorService scheduler) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
        if (pollIntervalSeconds <= 0) {
            throw new IllegalArgumentException(
                    "pollIntervalSeconds must be positive, got " + pollIntervalSeconds);
        }
        this.pollIntervalSeconds = pollIntervalSeconds;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    // ── Schema Initialization ────────────────────────────────────────

    /**
     * Validates the canonical migrated schema and mandatory commit-order gate.
     * Does not create a second, competing schema during bootstrap.
     */
    public void initSchema() {
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
            throw new MetastoreException("Failed to initialize invalidation_log schema", e);
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────

    /**
     * Starts the periodic polling task. Safe to call multiple times — subsequent
     * calls are no-ops if the scheduler is already running.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        scheduler.scheduleAtFixedRate(this::poll, 0, pollIntervalSeconds, TimeUnit.SECONDS);
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
            List<InvalidationEntry> entries = fetchNewEntries();
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
            if (applied > 0) {
                LOG.info("event=invalidation_poll_applied count={} lastTimestamp={} lastId={}",
                        applied, lastProcessedTimestamp.get(), lastProcessedId.get());
            }
            reportLastProcessedToRegistry();
        } catch (Exception e) {
            LOG.warn("event=invalidation_poll_failed lastTimestamp={} error={}",
                    lastProcessedTimestamp.get(), e.getMessage());
        }
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
            case RESULT_CACHE -> applyResultCacheInvalidation(entry);
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

    private void applyResultCacheInvalidation(InvalidationEntry entry) {
        var invalidator = resultCacheInvalidator;
        if (invalidator != null) {
            invalidator.accept(entry.cacheKey());
            LOG.debug("event=invalidation_applied type=RESULT_CACHE key={}", entry.cacheKey());
        } else {
            LOG.debug("event=invalidation_skip type=RESULT_CACHE reason=no_invalidator key={}",
                    entry.cacheKey());
        }
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

    /**
     * Configures reporting of {@code lastProcessedId} after each poll cycle so
     * other coordinators can observe DDL propagation.
     *
     * @param reporter             progress reporter (nullable — disables reporting)
     * @param currentCoordinatorId this coordinator's identifier
     */
    public void setInvalidationProgressRegistry(
            org.pangrova.metadata.api.cache.InvalidationProgressRegistry reporter,
            String currentCoordinatorId) {
        this.progressRegistry = reporter;
        this.coordinatorId = currentCoordinatorId;
    }

    /**
     * Configures a callback for invalidating query result cache entries when
     * a RESULT_CACHE invalidation entry is polled. The callback receives the
     * table name (cache key) and should call QueryResultCache.invalidateByTable().
     *
     * @param invalidator callback that accepts a table name to invalidate (nullable)
     */
    public void setResultCacheInvalidator(java.util.function.Consumer<String> invalidator) {
        this.resultCacheInvalidator = invalidator;
    }

    private void reportLastProcessedToRegistry() {
        var reporter = progressRegistry;
        var id = coordinatorId;
        if (reporter == null || id == null) {
            return;
        }
        try {
            reporter.updateLastProcessedInvalidationId(id, lastProcessedId.get());
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
    public int cleanup() {
        long safetyBound = lastProcessedId.get();
        var registry = progressRegistry;
        if (registry != null) {
            // Expiration is owned by registry maintenance. Never delete past any
            // still-registered coordinator, including an unknown progress fence.
            safetyBound = Math.min(safetyBound, registry.minimumReportedInvalidationId());
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
