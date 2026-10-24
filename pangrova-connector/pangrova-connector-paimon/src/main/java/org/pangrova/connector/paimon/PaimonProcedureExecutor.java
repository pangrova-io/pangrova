package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.data.TypedRow;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Executes Paimon maintenance procedures (compact, expire_snapshots, tags,
 * branches, repair, orphan-file cleanup, etc.) against the Paimon catalog.
 */
final class PaimonProcedureExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonProcedureExecutor.class);

    /** Paimon's own safe snapshot-retention window, used when the caller does not choose one. */
    private static final int DEFAULT_SNAPSHOT_RETAIN_MAX = 30;
    private static final int DEFAULT_SNAPSHOT_RETAIN_MIN = 10;

    /** Orphan deletion never uses a cutoff younger than this, so in-flight writes stay safe. */
    private static final long MIN_ORPHAN_AGE_MS = 24L * 60L * 60L * 1000L;

    private final Catalog catalog;

    PaimonProcedureExecutor(Catalog catalog) {
        this.catalog = catalog;
    }

    /** Dispatches a procedure invocation to the matching handler. */
    List<TypedRow> execute(String procedureName, Map<String, String> arguments) {
        String tableName = arguments.getOrDefault("arg0", "");
        return switch (procedureName.toLowerCase()) {
            case "compact" -> executeCompactProcedure(tableName);
            case "expire_snapshots" -> executeExpireSnapshotsProcedure(tableName, arguments);
            case "create_tag" -> executeCreateTagProcedure(tableName, arguments);
            case "delete_tag" -> executeDeleteTagProcedure(tableName, arguments);
            case "rollback_to" -> executeRollbackToProcedure(tableName, arguments);
            case "create_branch" -> executeCreateBranchProcedure(tableName, arguments);
            case "delete_branch" -> executeDeleteBranchProcedure(tableName, arguments);
            case "merge_branch" -> executeMergeBranchProcedure(tableName, arguments);
            case "repair" -> executeRepairProcedure(tableName);
            case "rewrite_file_index" -> executeRewriteFileIndexProcedure(tableName, arguments);
            case "remove_orphan_files" -> executeRemoveOrphanFilesProcedure(tableName, arguments);
            case "reset_consumer" -> executeResetConsumerProcedure(tableName, arguments);
            case "rescale_bucket" -> executeRescaleBucketProcedure(tableName, arguments);
            default -> throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Procedure not found: " + procedureName);
        };
    }

    private List<TypedRow> executeCompactProcedure(String tableName) {
        Table table = loadFreshTableByName(tableName);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        long startMs = System.currentTimeMillis();
        try {
            Table compactTable = resolveTableForCompact(fileStoreTable);
            List<Split> splits = compactTable.newReadBuilder().newScan().plan().splits();
            if (splits.isEmpty()) {
                LOG.info("event=procedure_compact_skipped table={} reason=no_data", tableName);
                return List.of(TypedRow.builder()
                        .putString("result", "compact succeeded")
                        .putString("table", tableName)
                        .build());
            }
            int compactedBuckets = compactSplits(compactTable, splits);
            long durationMs = System.currentTimeMillis() - startMs;
            LOG.info("event=procedure_compact_executed table={} compacted_buckets={} duration_ms={}",
                    tableName, compactedBuckets, durationMs);
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Compact failed for table: " + tableName + " — " + e.getMessage(), e);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        return List.of(TypedRow.builder()
                .putString("result", "compact succeeded")
                .putString("table", tableName)
                .putLong("duration_ms", durationMs)
                .build());
    }

    private static int compactSplits(Table compactTable, List<Split> splits) throws Exception {
        BatchWriteBuilder writeBuilder = compactTable.newBatchWriteBuilder();
        BatchTableWrite tableWrite = writeBuilder.newWrite();
        BatchTableCommit tableCommit = writeBuilder.newCommit();
        try {
            Set<String> compacted = new HashSet<>();
            for (Split split : splits) {
                if (split instanceof DataSplit dataSplit) {
                    BinaryRow partition = dataSplit.partition();
                    int bucket = dataSplit.bucket();
                    String key = partition.toString() + ":" + bucket;
                    if (compacted.add(key)) {
                        tableWrite.compact(partition, bucket, true);
                    }
                }
            }
            List<CommitMessage> messages = tableWrite.prepareCommit();
            tableCommit.commit(messages);
            return compacted.size();
        } finally {
            tableWrite.close();
            tableCommit.close();
        }
    }

    private static Table resolveTableForCompact(FileStoreTable table) {
        String bucket = table.options().getOrDefault("bucket", "1");
        if ("-2".equals(bucket)) {
            return table.copy(Map.of("bucket", "1"));
        }
        return table;
    }

    private List<TypedRow> executeExpireSnapshotsProcedure(
            String tableName, Map<String, String> args) {
        int retainMax = parseIntArgument(args, "1", "retain_max", DEFAULT_SNAPSHOT_RETAIN_MAX);
        int retainMin = parseIntArgument(args, "2", "retain_min",
                Math.min(DEFAULT_SNAPSHOT_RETAIN_MIN, retainMax));
        if (retainMax < 1 || retainMin < 1 || retainMin > retainMax) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "expire_snapshots requires 1 <= retain_min <= retain_max, got retain_min="
                            + retainMin + ", retain_max=" + retainMax);
        }
        boolean dryRun = parseBooleanArgument(args, "3", "dry_run");
        Table table = loadFreshTableByName(tableName);
        long startMs = System.currentTimeMillis();
        int expiredCount;
        try {
            if (dryRun) {
                expiredCount = estimateExpiredSnapshots(table, retainMax);
                LOG.info("event=procedure_expire_snapshots_dry_run table={} retain_max={} "
                        + "retain_min={} wouldExpire={}", tableName, retainMax, retainMin, expiredCount);
                return List.of(TypedRow.builder()
                        .putString("result", "expire_snapshots dry run")
                        .putString("table", tableName)
                        .putLong("retain_max", retainMax)
                        .putLong("retain_min", retainMin)
                        .putLong("expired_snapshots", expiredCount)
                        .putString("dry_run", "true")
                        .putLong("duration_ms", System.currentTimeMillis() - startMs)
                        .build());
            }
            org.apache.paimon.options.ExpireConfig expireConfig =
                    org.apache.paimon.options.ExpireConfig.builder()
                            .snapshotRetainMax(retainMax)
                            .snapshotRetainMin(retainMin)
                            .build();
            expiredCount = table.newExpireSnapshots()
                    .config(expireConfig)
                    .expire();
            LOG.info("event=procedure_expire_snapshots_executed table={} retain_max={} "
                            + "retain_min={} expired={}",
                    tableName, retainMax, retainMin, expiredCount);
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Expire snapshots failed for table: " + tableName + " — " + e.getMessage(), e);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        return List.of(TypedRow.builder()
                .putString("result", "expire_snapshots succeeded")
                .putString("table", tableName)
                .putLong("retain_max", retainMax)
                .putLong("retain_min", retainMin)
                .putLong("expired_snapshots", expiredCount)
                .putString("dry_run", "false")
                .putLong("duration_ms", durationMs)
                .build());
    }

    /** Counts snapshots beyond the retained window without expiring anything. */
    private static int estimateExpiredSnapshots(Table table, int retainMax) throws Exception {
        long snapshots = ((FileStoreTable) table).snapshotManager().snapshotCount();
        return (int) Math.max(0L, snapshots - retainMax);
    }

    private List<TypedRow> executeCreateTagProcedure(String tableName, Map<String, String> args) {
        if (!args.containsKey("arg1")) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "create_tag requires at least 2 arguments: table_name and tag_name, "
                            + "but only 1 argument was provided");
        }
        Table table = loadFreshTableByName(tableName);
        String tagName = args.get("arg1");
        if (args.size() > 4 || (args.containsKey("arg3") && !args.containsKey("arg2"))) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "create_tag accepts table_name, tag_name, optional snapshot_id and time_retained");
        }
        try {
            java.time.Duration retention = args.containsKey("arg3")
                    ? TimeUtils.parseDuration(args.get("arg3")) : null;
            if (args.containsKey("arg2")) {
                long snapshotId = Long.parseLong(args.get("arg2"));
                if (snapshotId <= 0) {
                    throw new IllegalArgumentException("snapshot_id must be positive");
                }
                table.createTag(tagName, snapshotId, retention);
            } else {
                table.createTag(tagName);
            }
        } catch (IllegalArgumentException invalid) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid create_tag snapshot_id or time_retained", invalid);
        }
        LOG.info("event=procedure_create_tag table={} tag={}", tableName, tagName);
        return List.of(TypedRow.builder()
                .putString("result", "create_tag succeeded")
                .putString("table", tableName)
                .putString("tag", tagName)
                .build());
    }

    private List<TypedRow> executeDeleteTagProcedure(String tableName, Map<String, String> args) {
        Table table = loadFreshTableByName(tableName);
        String tagName = args.getOrDefault("arg1", "");
        table.deleteTag(tagName);
        LOG.info("event=procedure_delete_tag table={} tag={}", tableName, tagName);
        return List.of(TypedRow.builder()
                .putString("result", "delete_tag succeeded")
                .putString("table", tableName)
                .putString("tag", tagName)
                .build());
    }

    private List<TypedRow> executeRollbackToProcedure(String tableName, Map<String, String> args) {
        Table table = loadFreshTableByName(tableName);
        String target = args.getOrDefault("arg1", "");
        try {
            long snapshotId = Long.parseLong(target);
            table.rollbackTo(snapshotId);
            LOG.info("event=procedure_rollback_to table={} snapshotId={}", tableName, snapshotId);
        } catch (NumberFormatException e) {
            table.rollbackTo(target);
            LOG.info("event=procedure_rollback_to table={} tag={}", tableName, target);
        }
        return List.of(TypedRow.builder()
                .putString("result", "rollback_to succeeded")
                .putString("table", tableName)
                .putString("target", target)
                .build());
    }

    private List<TypedRow> executeCreateBranchProcedure(String tableName, Map<String, String> args) {
        Table table = loadFreshTableByName(tableName);
        String branchName = args.getOrDefault("arg1", "");
        table.createBranch(branchName);
        LOG.info("event=procedure_create_branch table={} branch={}", tableName, branchName);
        return List.of(TypedRow.builder()
                .putString("result", "create_branch succeeded")
                .putString("table", tableName)
                .putString("branch", branchName)
                .build());
    }

    private List<TypedRow> executeDeleteBranchProcedure(String tableName, Map<String, String> args) {
        Table table = loadFreshTableByName(tableName);
        String branchName = args.getOrDefault("arg1", "");
        table.deleteBranch(branchName);
        LOG.info("event=procedure_delete_branch table={} branch={}", tableName, branchName);
        return List.of(TypedRow.builder()
                .putString("result", "delete_branch succeeded")
                .putString("table", tableName)
                .putString("branch", branchName)
                .build());
    }

    private List<TypedRow> executeMergeBranchProcedure(String tableName, Map<String, String> args) {
        throw unsupportedProcedure("merge_branch", tableName);
    }

    private List<TypedRow> executeRepairProcedure(String tableName) {
        throw unsupportedProcedure("repair", tableName);
    }

    private List<TypedRow> executeRewriteFileIndexProcedure(
            String tableName, Map<String, String> args) {
        throw unsupportedProcedure("rewrite_file_index", tableName);
    }

    private List<TypedRow> executeRemoveOrphanFilesProcedure(
            String tableName, Map<String, String> args) {
        long now = System.currentTimeMillis();
        Long requestedCutoff = parseLongArgumentOrNull(args, "1", "older_than_ms");
        long cutoff = requestedCutoff == null ? now - MIN_ORPHAN_AGE_MS : requestedCutoff;
        if (cutoff > now - MIN_ORPHAN_AGE_MS) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files older_than_ms must be at least "
                            + Duration.ofMillis(MIN_ORPHAN_AGE_MS).toHours()
                            + " hours old so in-flight writes are never treated as orphans");
        }
        boolean dryRun = parseBooleanArgument(args, "2", "dry_run");
        Table table = loadFreshTableByName(tableName);
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files requires a FileStoreTable: " + tableName);
        }
        try {
            org.apache.paimon.operation.LocalOrphanFilesClean cleaner =
                    new org.apache.paimon.operation.LocalOrphanFilesClean(
                            fileStoreTable, cutoff, dryRun);
            org.apache.paimon.operation.CleanOrphanFilesResult result = cleaner.clean();
            long candidates = result.getDeletedFileCount();
            return List.of(TypedRow.builder()
                    .putString("result", dryRun
                            ? "remove_orphan_files dry run" : "remove_orphan_files succeeded")
                    .putString("table", tableName)
                    .putLong("orphan_file_count", candidates)
                    .putLong("deleted_file_count", dryRun ? 0L : candidates)
                    .putLong("deleted_bytes", dryRun ? 0L : result.getDeletedFileTotalLenInBytes())
                    .putString("dry_run", String.valueOf(dryRun))
                    .build());
        } catch (Exception e) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files failed for " + tableName + ": " + e.getMessage(), e);
        }
    }

    private List<TypedRow> executeResetConsumerProcedure(
            String tableName, Map<String, String> args) {
        throw unsupportedProcedure("reset_consumer", tableName);
    }

    private List<TypedRow> executeRescaleBucketProcedure(
            String tableName, Map<String, String> args) {
        throw unsupportedProcedure("rescale_bucket", tableName);
    }

    /**
     * Procedures listed in {@link #execute} but not implemented by Pangrova must fail loudly.
     * Reporting "submitted" or "succeeded" for a no-op hides a maintenance gap from operators.
     */
    private static ConnectorException unsupportedProcedure(String procedure, String tableName) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                "Paimon procedure '" + procedure + "' is not implemented; refusing to report "
                        + "success for table: " + tableName);
    }

    private static int parseIntArgument(
            Map<String, String> args, String position, String alias, int defaultValue) {
        String raw = args.get("arg" + position);
        if (raw == null || raw.isBlank()) {
            raw = args.get(alias);
        }
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid integer argument '" + alias + "': " + raw, e);
        }
    }

    private static Long parseLongArgumentOrNull(
            Map<String, String> args, String position, String alias) {
        String raw = args.get("arg" + position);
        if (raw == null || raw.isBlank()) {
            raw = args.get(alias);
        }
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid long argument '" + alias + "': " + raw, e);
        }
    }

    private static boolean parseBooleanArgument(
            Map<String, String> args, String position, String alias) {
        String raw = args.get("arg" + position);
        if (raw == null || raw.isBlank()) {
            raw = args.get(alias);
        }
        if (raw == null || raw.isBlank()) {
            return false;
        }
        if (!"true".equalsIgnoreCase(raw.trim()) && !"false".equalsIgnoreCase(raw.trim())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Invalid boolean argument '" + alias + "': " + raw);
        }
        return Boolean.parseBoolean(raw.trim());
    }

    private Table loadFreshTableByName(String qualifiedName) {
        String[] parts = qualifiedName.split("\\.");
        String schema = parts.length >= 2 ? parts[0] : "default";
        String table = parts.length >= 2 ? parts[1] : parts[0];
        Identifier id = Identifier.create(schema, table);
        try {
            return catalog.getTable(id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + id, e);
        }
    }
}
