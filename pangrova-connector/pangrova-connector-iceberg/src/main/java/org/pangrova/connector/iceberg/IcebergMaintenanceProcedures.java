package org.pangrova.connector.iceberg;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.iceberg.write.IcebergCompactionExecutor;
import org.pangrova.connector.spi.CacheEvictionListener;
import org.pangrova.connector.spi.base.ConnectorException;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Iceberg table maintenance procedures.
 *
 * <p>Provides Trino/Spark-style {@code CALL catalog.procedure_name(args)} parity for
 * Iceberg tables, dispatching to real Iceberg SDK APIs. Each procedure logs structured
 * start/end events with metrics, returns one or more result rows describing what
 * happened, and throws a {@link ConnectorException} on failure (no silent no-ops).</p>
 *
 * <p>Supported procedures:</p>
 * <ul>
 *   <li>{@code compact(table_name)} — rewrites data files with associated delete
 *       files, materializing deletion vectors / position deletes into clean data.</li>
 *   <li>{@code expire_snapshots(table_name, [retain_last], [older_than_millis], [dry_run])}
 *       — uses {@link Table#expireSnapshots()} to atomically expire snapshots and
 *       clean orphaned data/manifest files. {@code retain_last} defaults to a safe window
 *       instead of the current snapshot only.</li>
 *   <li>{@code remove_orphan_files(table_name, [older_than_millis], [dry_run])} — walks the
 *       table location via {@link SupportsPrefixOperations#listPrefix(String)} and
 *       deletes files that are not reachable from any snapshot/manifest. The cutoff must be
 *       at least 24 hours old. Requires a {@link FileIO} that implements
 *       {@link SupportsPrefixOperations}.</li>
 *   <li>{@code rewrite_manifests(table_name)} — uses {@link Table#rewriteManifests()}
 *       to compact small manifests into larger ones for better planning performance.</li>
 *   <li>{@code rollback_to_snapshot(table_name, snapshot_id)} — uses
 *       {@link Table#manageSnapshots()} to roll the current ref back to a previous
 *       snapshot id.</li>
 *   <li>{@code set_current_snapshot(table_name, snapshot_id)} — fast-forwards or
 *       sets the main branch to a specific snapshot id.</li>
 *   <li>{@code cherrypick_snapshot(table_name, snapshot_id)} — applies a staged or
 *       wap snapshot's changes onto the current branch.</li>
 * </ul>
 */
public final class IcebergMaintenanceProcedures {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceProcedures.class);

    /**
     * Default retain-last value for expire_snapshots when not provided by the caller.
     * Keeping only the current snapshot removes the lineage used by commit recovery and
     * external-writer reconciliation, so the default retains a safe window.
     */
    private static final int DEFAULT_RETAIN_LAST = 10;

    /** Default older-than horizon for remove_orphan_files (3 days). */
    private static final long DEFAULT_ORPHAN_AGE_MS = 3L * 24L * 60L * 60L * 1000L;

    /** Orphan deletion never uses a cutoff younger than this, so in-flight writes stay safe. */
    private static final long MIN_ORPHAN_AGE_MS = 24L * 60L * 60L * 1000L;

    private final Catalog catalog;
    private final CacheEvictionListener evictionListener;

    public IcebergMaintenanceProcedures(Catalog catalog) {
        this(catalog, CacheEvictionListener.NOOP);
    }

    /**
     * Creates a maintenance procedures runner with a cache eviction listener.
     *
     * <p>F-CHAIN-CACHE-03: when maintenance ops remove data files,
     * eviction hints are published via the listener so stale cache entries
     * can be reclaimed across all workers.</p>
     *
     * @param catalog           the Iceberg catalog
     * @param evictionListener  listener for cache eviction events
     */
    public IcebergMaintenanceProcedures(Catalog catalog, CacheEvictionListener evictionListener) {
        this.catalog = catalog;
        this.evictionListener = evictionListener != null
                ? evictionListener : CacheEvictionListener.NOOP;
    }

    /**
     * Dispatches a procedure invocation to the matching handler.
     *
     * @param procedureName procedure name (case-insensitive)
     * @param arguments     keyed as {@code arg0..argN} per call-site convention
     * @return a list of result rows describing the outcome
     * @throws ConnectorException        on procedure failure
     * @throws UnsupportedOperationException for unknown procedure names
     */
    public List<TypedRow> execute(String procedureName, Map<String, String> arguments) {
        return switch (procedureName.toLowerCase()) {
            case "compact" -> executeCompact(arguments);
            case "expire_snapshots" -> executeExpireSnapshots(arguments);
            case "remove_orphan_files" -> executeRemoveOrphanFiles(arguments);
            case "rewrite_manifests" -> executeRewriteManifests(arguments);
            case "rollback_to_snapshot" -> executeRollbackToSnapshot(arguments);
            case "set_current_snapshot" -> executeSetCurrentSnapshot(arguments);
            case "cherrypick_snapshot" -> executeCherrypickSnapshot(arguments);
            default -> throw new UnsupportedOperationException(
                    "Unknown Iceberg procedure: " + procedureName);
        };
    }

    // -- compact ---------------------------------------------------------------

    private List<TypedRow> executeCompact(Map<String, String> args) {
        String tableName = requireTableName(args);
        Table table = loadTable(tableName);
        long startMs = System.currentTimeMillis();
        LOG.info("event=procedure_compact_start table={}", tableName);
        try {
            IcebergCompactionExecutor.CompactionSummary summary =
                    IcebergCompactionExecutor.compact(table);
            long durationMs = System.currentTimeMillis() - startMs;

            LOG.info("event=procedure_compact_done table={} filesCompacted={} "
                            + "filesProduced={} bytesRewritten={} bytesSaved={} "
                            + "deleteFilesRemoved={} durationMs={}",
                    tableName, summary.filesCompacted(), summary.filesProduced(),
                    summary.bytesRewritten(), summary.bytesSaved(),
                    summary.deleteFilesRemoved(), durationMs);
            return List.of(TypedRow.builder()
                    .putString("result", "compact succeeded")
                    .putString("table", tableName)
                    .putLong("files_compacted", summary.filesCompacted())
                    .putLong("files_produced", summary.filesProduced())
                    .putLong("bytes_rewritten", summary.bytesRewritten())
                    .putLong("bytes_saved", summary.bytesSaved())
                    .putLong("delete_files_removed", summary.deleteFilesRemoved())
                    .putLong("duration_ms", durationMs)
                    .build());
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "compact failed for table: " + tableName + " — " + e.getMessage(), e);
        }
    }

    // -- expire_snapshots ------------------------------------------------------

    private List<TypedRow> executeExpireSnapshots(Map<String, String> args) {
        String tableName = requireTableName(args);
        int retainLast = parseIntArg(args, "arg1", DEFAULT_RETAIN_LAST);
        if (retainLast < 1) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "expire_snapshots retain_last must be at least 1, got: " + retainLast);
        }
        Long olderThanMs = parseLongArgOrNull(args, "arg2");
        boolean dryRun = parseBooleanArg(args, "arg3");
        long startMs = System.currentTimeMillis();
        LOG.info("event=procedure_expire_snapshots_start table={} retainLast={} olderThanMs={} dryRun={}",
                tableName, retainLast, olderThanMs, dryRun);

        if (dryRun) {
            int wouldExpire = estimateExpiredSnapshots(loadTable(tableName), retainLast, olderThanMs);
            LOG.info("event=procedure_expire_snapshots_dry_run table={} wouldExpire={} retainLast={}",
                    tableName, wouldExpire, retainLast);
            return List.of(TypedRow.builder()
                    .putString("result", "expire_snapshots dry run")
                    .putString("table", tableName)
                    .putLong("expired_snapshots", wouldExpire)
                    .putLong("retain_last", retainLast)
                    .putString("dry_run", "true")
                    .putLong("duration_ms", System.currentTimeMillis() - startMs)
                    .build());
        }

        Table table = loadTable(tableName);
        Set<Long> snapshotsBefore = collectSnapshotIds(table);
        Set<String> deletedFiles = new HashSet<>();
        try {
            org.apache.iceberg.ExpireSnapshots expire = table.expireSnapshots()
                    .retainLast(retainLast)
                    .cleanExpiredFiles(true)
                    .deleteWith(deletedFiles::add);
            if (olderThanMs != null) {
                expire = expire.expireOlderThan(olderThanMs);
            }
            expire.commit();
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "expire_snapshots failed for table: " + tableName + " — " + e.getMessage(), e);
        }

        table.refresh();
        Set<Long> snapshotsAfter = collectSnapshotIds(table);
        int expiredCount = snapshotsBefore.size() - snapshotsAfter.size();
        long durationMs = System.currentTimeMillis() - startMs;

        // F-CHAIN-CACHE-03: publish eviction hints for deleted data files
        if (!deletedFiles.isEmpty()) {
            evictionListener.onFilesRemoved(deletedFiles, "expire_snapshots");
        }

        LOG.info("event=procedure_expire_snapshots_done table={} expiredSnapshots={} "
                        + "deletedFiles={} durationMs={}",
                tableName, expiredCount, deletedFiles.size(), durationMs);
        return List.of(TypedRow.builder()
                .putString("result", "expire_snapshots succeeded")
                .putString("table", tableName)
                .putLong("expired_snapshots", expiredCount)
                .putLong("deleted_file_count", deletedFiles.size())
                .putLong("retain_last", retainLast)
                .putString("dry_run", "false")
                .putLong("duration_ms", durationMs)
                .build());
    }

    // -- remove_orphan_files ---------------------------------------------------

    private List<TypedRow> executeRemoveOrphanFiles(Map<String, String> args) {
        String tableName = requireTableName(args);
        long now = System.currentTimeMillis();
        long olderThanMs = parseLongArg(args, "arg1", now - DEFAULT_ORPHAN_AGE_MS);
        if (olderThanMs > now - MIN_ORPHAN_AGE_MS) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files older_than_ms must be at least "
                            + Duration.ofMillis(MIN_ORPHAN_AGE_MS).toHours()
                            + " hours old so in-flight writes are never treated as orphans");
        }
        boolean dryRun = parseBooleanArg(args, "arg2");
        Table table = loadTable(tableName);
        long startMs = System.currentTimeMillis();
        LOG.info("event=procedure_remove_orphan_files_start table={} olderThanMs={} dryRun={}",
                tableName, olderThanMs, dryRun);

        FileIO io = table.io();
        if (!(io instanceof SupportsPrefixOperations prefixIO)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files requires a FileIO that supports prefix listing; "
                            + "got: " + io.getClass().getName());
        }

        IcebergOrphanFileRemover.SweepResult sweep;
        try {
            sweep = IcebergOrphanFileRemover.sweep(table, prefixIO, olderThanMs, tableName, dryRun);
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files failed for table: " + tableName
                            + " — " + e.getMessage(), e);
        }

        // F-CHAIN-CACHE-03: publish eviction hints for deleted orphan files
        if (!sweep.deletedOrphanPaths().isEmpty()) {
            evictionListener.onFilesRemoved(sweep.deletedOrphanPaths(), "remove_orphan_files");
        }

        long durationMs = System.currentTimeMillis() - startMs;
        LOG.info("event=procedure_remove_orphan_files_done table={} scanned={} orphans={} "
                        + "deleted={} bytes={} dryRun={} durationMs={}",
                tableName, sweep.scannedCount(), sweep.orphanCount(), sweep.deletedCount(),
                sweep.deletedBytes(), dryRun, durationMs);
        return List.of(TypedRow.builder()
                .putString("result", dryRun
                        ? "remove_orphan_files dry run" : "remove_orphan_files succeeded")
                .putString("table", tableName)
                .putLong("scanned_file_count", sweep.scannedCount())
                .putLong("orphan_file_count", sweep.orphanCount())
                .putLong("deleted_file_count", sweep.deletedCount())
                .putLong("deleted_bytes", sweep.deletedBytes())
                .putLong("older_than_ms", olderThanMs)
                .putString("dry_run", String.valueOf(dryRun))
                .putLong("duration_ms", durationMs)
                .build());
    }

    // -- rewrite_manifests -----------------------------------------------------

    private List<TypedRow> executeRewriteManifests(Map<String, String> args) {
        String tableName = requireTableName(args);
        Table table = loadTable(tableName);
        long startMs = System.currentTimeMillis();
        LOG.info("event=procedure_rewrite_manifests_start table={}", tableName);

        Snapshot before = table.currentSnapshot();
        int manifestsBefore = before == null
                ? 0
                : before.allManifests(table.io()).size();

        try {
            table.rewriteManifests().commit();
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "rewrite_manifests failed for table: " + tableName
                            + " — " + e.getMessage(), e);
        }

        table.refresh();
        Snapshot after = table.currentSnapshot();
        int manifestsAfter = after == null
                ? 0
                : after.allManifests(table.io()).size();
        long durationMs = System.currentTimeMillis() - startMs;
        LOG.info("event=procedure_rewrite_manifests_done table={} manifestsBefore={} "
                        + "manifestsAfter={} durationMs={}",
                tableName, manifestsBefore, manifestsAfter, durationMs);
        return List.of(TypedRow.builder()
                .putString("result", "rewrite_manifests succeeded")
                .putString("table", tableName)
                .putLong("manifests_before", manifestsBefore)
                .putLong("manifests_after", manifestsAfter)
                .putLong("duration_ms", durationMs)
                .build());
    }

    // -- rollback_to_snapshot --------------------------------------------------

    private List<TypedRow> executeRollbackToSnapshot(Map<String, String> args) {
        String tableName = requireTableName(args);
        long snapshotId = requireLongArg(args, "arg1", "snapshot_id");
        Table table = loadTable(tableName);
        LOG.info("event=procedure_rollback_to_snapshot_start table={} snapshotId={}",
                tableName, snapshotId);
        try {
            table.manageSnapshots().rollbackTo(snapshotId).commit();
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "rollback_to_snapshot failed for table: " + tableName
                            + " snapshotId=" + snapshotId + " — " + e.getMessage(), e);
        }
        LOG.info("event=procedure_rollback_to_snapshot_done table={} snapshotId={}",
                tableName, snapshotId);
        return List.of(TypedRow.builder()
                .putString("result", "rollback_to_snapshot succeeded")
                .putString("table", tableName)
                .putLong("snapshot_id", snapshotId)
                .build());
    }

    // -- set_current_snapshot --------------------------------------------------

    private List<TypedRow> executeSetCurrentSnapshot(Map<String, String> args) {
        String tableName = requireTableName(args);
        long snapshotId = requireLongArg(args, "arg1", "snapshot_id");
        Table table = loadTable(tableName);
        LOG.info("event=procedure_set_current_snapshot_start table={} snapshotId={}",
                tableName, snapshotId);
        try {
            table.manageSnapshots().setCurrentSnapshot(snapshotId).commit();
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "set_current_snapshot failed for table: " + tableName
                            + " snapshotId=" + snapshotId + " — " + e.getMessage(), e);
        }
        LOG.info("event=procedure_set_current_snapshot_done table={} snapshotId={}",
                tableName, snapshotId);
        return List.of(TypedRow.builder()
                .putString("result", "set_current_snapshot succeeded")
                .putString("table", tableName)
                .putLong("snapshot_id", snapshotId)
                .build());
    }

    // -- cherrypick_snapshot ---------------------------------------------------

    private List<TypedRow> executeCherrypickSnapshot(Map<String, String> args) {
        String tableName = requireTableName(args);
        long snapshotId = requireLongArg(args, "arg1", "snapshot_id");
        Table table = loadTable(tableName);
        LOG.info("event=procedure_cherrypick_snapshot_start table={} snapshotId={}",
                tableName, snapshotId);
        try {
            table.manageSnapshots().cherrypick(snapshotId).commit();
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "cherrypick_snapshot failed for table: " + tableName
                            + " snapshotId=" + snapshotId + " — " + e.getMessage(), e);
        }
        LOG.info("event=procedure_cherrypick_snapshot_done table={} snapshotId={}",
                tableName, snapshotId);
        return List.of(TypedRow.builder()
                .putString("result", "cherrypick_snapshot succeeded")
                .putString("table", tableName)
                .putLong("snapshot_id", snapshotId)
                .build());
    }

    // -- helpers ---------------------------------------------------------------

    private Table loadTable(String qualifiedName) {
        String[] parts = qualifiedName.split("\\.");
        if (parts.length < 2) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Procedure requires schema-qualified table name 'schema.table', got: "
                            + qualifiedName);
        }
        if (catalog == null) {
            // Defensive: should not happen at runtime — IcebergMetadata always passes
            // a valid catalog. Required only for unit tests of pure argument validation.
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "No catalog bound to maintenance procedures runner");
        }
        try {
            // Support multi-level namespaces by treating everything before the last dot
            // as namespace.
            String tableName = parts[parts.length - 1];
            String[] nsParts = new String[parts.length - 1];
            System.arraycopy(parts, 0, nsParts, 0, parts.length - 1);
            TableIdentifier id = TableIdentifier.of(
                    org.apache.iceberg.catalog.Namespace.of(nsParts), tableName);
            return catalog.loadTable(id);
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + qualifiedName, e);
        }
    }

    private static String requireTableName(Map<String, String> args) {
        String tableName = args.get("arg0");
        if (tableName == null || tableName.isBlank()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Procedure requires the table name as its first argument");
        }
        return tableName;
    }

    private static int parseIntArg(Map<String, String> args, String key, int defaultValue) {
        String raw = args.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid integer argument '" + key + "': " + raw, e);
        }
    }

    private static long parseLongArg(Map<String, String> args, String key, long defaultValue) {
        String raw = args.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid long argument '" + key + "': " + raw, e);
        }
    }

    private static Long parseLongArgOrNull(Map<String, String> args, String key) {
        String raw = args.get(key);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid long argument '" + key + "': " + raw, e);
        }
    }

    private static boolean parseBooleanArg(Map<String, String> args, String key) {
        String raw = args.get(key);
        if (raw == null || raw.isBlank()) {
            return false;
        }
        if (!"true".equalsIgnoreCase(raw) && !"false".equalsIgnoreCase(raw)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid boolean argument '" + key + "': " + raw);
        }
        return Boolean.parseBoolean(raw);
    }

    /**
     * Estimates how many snapshots expire_snapshots would remove without committing anything.
     * Expiry keeps at least {@code retainLast} recent snapshots and, when a horizon is given,
     * only removes snapshots older than it.
     */
    private static int estimateExpiredSnapshots(Table table, int retainLast, Long olderThanMs) {
        java.util.List<Snapshot> ordered = new java.util.ArrayList<>();
        table.snapshots().forEach(ordered::add);
        ordered.sort(java.util.Comparator.comparingLong(Snapshot::timestampMillis)
                .thenComparingLong(Snapshot::sequenceNumber));
        int older = Math.max(0, ordered.size() - Math.min(retainLast, ordered.size()));
        if (olderThanMs == null) {
            return older;
        }
        int candidates = 0;
        for (int index = 0; index < older; index++) {
            if (ordered.get(index).timestampMillis() < olderThanMs) {
                candidates++;
            }
        }
        return candidates;
    }

    private static long requireLongArg(Map<String, String> args, String key, String description) {
        String raw = args.get(key);
        if (raw == null || raw.isBlank()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Procedure requires the " + description + " argument");
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid " + description + " argument: " + raw, e);
        }
    }

    private static Set<Long> collectSnapshotIds(Table table) {
        Set<Long> ids = new HashSet<>();
        for (Snapshot s : table.snapshots()) {
            ids.add(s.snapshotId());
        }
        return ids;
    }
}
