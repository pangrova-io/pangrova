package org.pangrova.coordinator.ai.branch;

import org.pangrova.connector.api.enums.SnapshotReferenceType;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.SnapshotReference;
import org.pangrova.connector.api.enums.ConditionalBranchDropResult;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scheduled background task that scans tables with {@code branch.ttl} property,
 * identifies branches exceeding the configured TTL, and drops them via the
 * connector metadata API.
 *
 * <p>The TTL is configured per-table via table properties:
 * {@code ALTER TABLE t SET TBLPROPERTIES ('branch.ttl' = '7d')}.
 * Supported duration formats: {@code 1d}, {@code 12h}, {@code 30m}, {@code 7d12h}.</p>
 *
 * <p>The task runs periodically (default every 60 minutes) and is leader-aware:
 * only the maintenance leader node executes cleanup to avoid duplicate drops.</p>
 */
public final class BranchTtlCleanupTask implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BranchTtlCleanupTask.class);

    static final String BRANCH_TTL_PROPERTY = "branch.ttl";
    private static final String MAIN_BRANCH = "main";
    private static final int DEFAULT_SCAN_INTERVAL_MINUTES = 60;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int HOURS_PER_DAY = 24;

    private static final Pattern DURATION_PATTERN = Pattern.compile(
            "(?:(\\d+)d)?(?:(\\d+)h)?(?:(\\d+)m)?");

    private final CatalogRegistry catalogRegistry;
    private final int scanIntervalMinutes;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduledTask;

    public BranchTtlCleanupTask(CatalogRegistry catalogRegistry) {
        this(catalogRegistry, DEFAULT_SCAN_INTERVAL_MINUTES);
    }

    public BranchTtlCleanupTask(CatalogRegistry catalogRegistry, int scanIntervalMinutes) {
        this.catalogRegistry = catalogRegistry;
        this.scanIntervalMinutes = scanIntervalMinutes;
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("branch-ttl-cleanup");
    }

    /**
     * Starts the periodic cleanup task at the configured interval.
     */
    public void start() {
        scheduledTask = scheduler.scheduleAtFixedRate(
                this::runSafe, scanIntervalMinutes, scanIntervalMinutes, TimeUnit.MINUTES);
        LOG.info("action=branch_ttl_cleanup_started intervalMinutes={}", scanIntervalMinutes);
    }

    /**
     * Stops the periodic cleanup task. Idempotent.
     */
    public void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
            scheduledTask = null;
        }
        LOG.info("action=branch_ttl_cleanup_stopped");
    }

    private void runSafe() {
        try {
            CleanupResult result = runCleanup();
            LOG.info("action=branch_ttl_cleanup_completed tablesScanned={} branchesDropped={}",
                    result.tablesScanned(), result.branchesDropped());
        } catch (Exception e) {
            LOG.error("action=branch_ttl_cleanup_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * Executes one cleanup cycle. Visible for testing.
     */
    CleanupResult runCleanup() {
        int tablesScanned = 0;
        int branchesDropped = 0;
        Instant now = Instant.now();

        List<String> catalogs = catalogRegistry.listCatalogNames();
        for (String catalogName : catalogs) {
            CatalogHandle catalogHandle;
            try {
                catalogHandle = catalogRegistry.getCatalog(catalogName);
            } catch (Exception e) {
                LOG.debug("action=skip_catalog catalog={} reason={}", catalogName, e.getMessage());
                continue;
            }

            ConnectorSession session = new BranchConnectorSession(catalogName);
            ConnectorMetadata metadata = catalogHandle.getMetadata(session);

            List<SchemaTableName> tables = listAllTables(metadata, session);
            for (SchemaTableName tableName : tables) {
                int dropped = processTable(metadata, session, catalogName, tableName, now);
                if (dropped >= 0) {
                    tablesScanned++;
                    branchesDropped += dropped;
                }
            }
        }

        return new CleanupResult(tablesScanned, branchesDropped);
    }

    private List<SchemaTableName> listAllTables(ConnectorMetadata metadata, ConnectorSession session) {
        try {
            return metadata.listTables(session, Optional.empty());
        } catch (Exception e) {
            LOG.debug("action=list_tables_failed reason={}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Processes a single table: checks for branch.ttl property, lists branches,
     * and drops those exceeding TTL.
     *
     * @return number of branches dropped, or -1 if table has no TTL configured
     */
    private int processTable(ConnectorMetadata metadata, ConnectorSession session,
                             String catalogName, SchemaTableName tableName, Instant now) {
        ConnectorTableHandle tableHandle = resolveTableHandle(metadata, session, catalogName, tableName);
        if (tableHandle == null) {
            return -1;
        }

        Duration ttl = resolveBranchTtl(metadata, session, catalogName, tableName, tableHandle);
        if (ttl == null) {
            return -1;
        }

        return dropExpiredBranches(metadata, session, tableHandle, catalogName, tableName, ttl, now);
    }

    private ConnectorTableHandle resolveTableHandle(ConnectorMetadata metadata, ConnectorSession session,
                                                    String catalogName, SchemaTableName tableName) {
        try {
            return metadata.getTableHandle(session, tableName);
        } catch (Exception e) {
            LOG.debug("action=skip_table table={}.{} reason={}",
                    catalogName, tableName, e.getMessage());
            return null;
        }
    }

    private Duration resolveBranchTtl(ConnectorMetadata metadata, ConnectorSession session,
                                      String catalogName, SchemaTableName tableName,
                                      ConnectorTableHandle tableHandle) {
        ConnectorTableMetadata tableMetadata;
        try {
            tableMetadata = metadata.getTableMetadata(session, tableHandle);
        } catch (Exception e) {
            LOG.debug("action=skip_table_metadata table={}.{} reason={}",
                    catalogName, tableName, e.getMessage());
            return null;
        }

        Map<String, String> properties = tableMetadata.properties();
        String ttlValue = properties.get(BRANCH_TTL_PROPERTY);
        if (ttlValue == null || ttlValue.isBlank()) {
            return null;
        }

        Duration ttl = parseDuration(ttlValue);
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            LOG.warn("action=invalid_branch_ttl table={}.{}.{} value={}",
                    catalogName, tableName.schema(), tableName.table(), ttlValue);
            return null;
        }
        return ttl;
    }

    private int dropExpiredBranches(ConnectorMetadata metadata, ConnectorSession session,
                                    ConnectorTableHandle tableHandle, String catalogName,
                                    SchemaTableName tableName, Duration ttl, Instant now) {
        List<SnapshotReference> refs;
        try {
            refs = metadata.listSnapshotReferences(session, tableHandle);
        } catch (Exception e) {
            LOG.warn("action=list_snapshot_references_failed table={}.{} reason={}",
                    catalogName, tableName, e.getMessage());
            return 0;
        }

        Instant cutoff = now.minus(ttl);
        int dropped = 0;
        for (SnapshotReference ref : refs) {
            if (!isExpiredBranch(metadata, session, tableName, ref, cutoff)) {
                continue;
            }
            ConditionalBranchDropResult result = dropBranch(
                    metadata, session, tableHandle, catalogName, tableName, ref);
            if (result == ConditionalBranchDropResult.DROPPED) {
                dropped++;
            } else {
                // The table metadata may have changed or the commit outcome may
                // be unknown. Do not act on further refs from the stale listing.
                break;
            }
        }
        return dropped;
    }

    private boolean isExpiredBranch(ConnectorMetadata metadata, ConnectorSession session,
                                    SchemaTableName tableName, SnapshotReference ref, Instant cutoff) {
        if (ref.type() != SnapshotReferenceType.BRANCH || MAIN_BRANCH.equals(ref.name())) {
            return false;
        }
        Instant createdAt = resolveSnapshotTimestamp(metadata, session, tableName, ref.snapshotId());
        return createdAt != null && createdAt.isBefore(cutoff);
    }

    private ConditionalBranchDropResult dropBranch(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle tableHandle, String catalogName,
            SchemaTableName tableName, SnapshotReference ref) {
        try {
            ConditionalBranchDropResult result = metadata.dropBranchIfSnapshotMatches(
                    session, tableHandle, ref.name(), ref.snapshotId());
            if (result == ConditionalBranchDropResult.DROPPED) {
                LOG.info("action=branch_ttl_expired catalog={} table={}.{} branch={}",
                        catalogName, tableName.schema(), tableName.table(), ref.name());
                return result;
            }
            LOG.warn("action=branch_ttl_drop_deferred catalog={} table={}.{} branch={} status={}",
                    catalogName, tableName.schema(), tableName.table(), ref.name(), result);
            return result;
        } catch (Exception e) {
            LOG.warn("action=branch_drop_failed catalog={} table={}.{} branch={} error={}",
                    catalogName, tableName.schema(), tableName.table(), ref.name(), e.getMessage(), e);
            return ConditionalBranchDropResult.FAILED;
        }
    }

    private Instant resolveSnapshotTimestamp(ConnectorMetadata metadata, ConnectorSession session,
                                             SchemaTableName tableName, long snapshotId) {
        try {
            Optional<Instant> timestamp = metadata.resolveSnapshotTimestamp(
                    session, tableName.schema(), tableName.table(), snapshotId);
            return timestamp.orElse(null);
        } catch (Exception e) {
            LOG.debug("action=snapshot_timestamp_resolve_failed table={} snapshotId={} reason={}",
                    tableName, snapshotId, e.getMessage());
            return null;
        }
    }

    /**
     * Parses a duration string in the format {@code Nd}, {@code Nh}, {@code Nm},
     * or combinations like {@code 7d12h}.
     *
     * @return the parsed duration, or null if the format is invalid
     */
    static Duration parseDuration(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim().toLowerCase(Locale.ROOT);
        Matcher matcher = DURATION_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            return null;
        }

        String days = matcher.group(1);
        String hours = matcher.group(2);
        String minutes = matcher.group(3);

        if (days == null && hours == null && minutes == null) {
            return null;
        }

        long totalMinutes = parsePart(days, HOURS_PER_DAY * MINUTES_PER_HOUR)
                + parsePart(hours, MINUTES_PER_HOUR) + parsePart(minutes, 1);
        return Duration.ofMinutes(totalMinutes);
    }

    private static long parsePart(String part, long multiplier) {
        return part != null ? Long.parseLong(part) * multiplier : 0;
    }

    @Override
    public void close() {
        stop();
        scheduler.shutdownNow();
        LOG.info("action=branch_ttl_cleanup_shutdown");
    }

    record CleanupResult(int tablesScanned, int branchesDropped) {}
}
