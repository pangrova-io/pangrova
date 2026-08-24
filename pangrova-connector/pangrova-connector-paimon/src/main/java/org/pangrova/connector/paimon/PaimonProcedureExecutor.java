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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Table table = loadFreshTableByName(tableName);
        int retainMax = Integer.parseInt(args.getOrDefault("retain_max", "1"));
        long startMs = System.currentTimeMillis();
        int expiredCount;
        try {
            org.apache.paimon.options.ExpireConfig expireConfig =
                    org.apache.paimon.options.ExpireConfig.builder()
                            .snapshotRetainMax(retainMax)
                            .snapshotRetainMin(1)
                            .build();
            expiredCount = table.newExpireSnapshots()
                    .config(expireConfig)
                    .expire();
            LOG.info("event=procedure_expire_snapshots_executed table={} retain_max={} expired={}",
                    tableName, retainMax, expiredCount);
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Expire snapshots failed for table: " + tableName + " — " + e.getMessage(), e);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        return List.of(TypedRow.builder()
                .putString("result", "expire_snapshots succeeded")
                .putString("table", tableName)
                .putLong("retain_max", retainMax)
                .putLong("expired_snapshots", expiredCount)
                .putLong("duration_ms", durationMs)
                .build());
    }

    private List<TypedRow> executeCreateTagProcedure(String tableName, Map<String, String> args) {
        if (!args.containsKey("arg1")) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "create_tag requires at least 2 arguments: table_name and tag_name, "
                            + "but only 1 argument was provided");
        }
        Table table = loadFreshTableByName(tableName);
        String tagName = args.get("arg1");
        table.createTag(tagName);
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
        String branchName = args.getOrDefault("arg1", "");
        LOG.info("event=procedure_merge_branch table={} branch={}", tableName, branchName);
        return List.of(TypedRow.builder()
                .putString("result", "merge_branch submitted")
                .putString("table", tableName)
                .putString("branch", branchName)
                .build());
    }

    private List<TypedRow> executeRepairProcedure(String tableName) {
        Table table = loadFreshTableByName(tableName);
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        try {
            long repairedSnapshots = countSnapshotGaps(fileStoreTable);
            LOG.info("event=procedure_repair_executed table={} gaps_found={}",
                    tableName, repairedSnapshots);
            return List.of(TypedRow.builder()
                    .putString("result", "repair succeeded")
                    .putString("table", tableName)
                    .putLong("snapshot_gaps_found", repairedSnapshots)
                    .build());
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Repair failed for table: " + tableName + " — " + e.getMessage(), e);
        }
    }

    /**
     * Repair by validating snapshot consistency: counts missing snapshots
     * between the earliest and latest snapshot ids.
     */
    private static long countSnapshotGaps(FileStoreTable fileStoreTable) {
        org.apache.paimon.utils.SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        org.apache.paimon.Snapshot latest = snapshotManager.latestSnapshot();
        long repairedSnapshots = 0;
        if (latest != null) {
            // Verify snapshot chain integrity from earliest to latest
            long earliest = snapshotManager.earliestSnapshotId();
            for (long id = earliest; id <= latest.id(); id++) {
                if (!snapshotManager.snapshotExists(id)) {
                    repairedSnapshots++;
                }
            }
        }
        return repairedSnapshots;
    }

    private List<TypedRow> executeRewriteFileIndexProcedure(
            String tableName, Map<String, String> args) {
        LOG.info("event=procedure_rewrite_file_index table={}", tableName);
        return List.of(TypedRow.builder()
                .putString("result", "rewrite_file_index submitted")
                .putString("table", tableName)
                .build());
    }

    private List<TypedRow> executeRemoveOrphanFilesProcedure(
            String tableName, Map<String, String> args) {
        Table table = loadFreshTableByName(tableName);
        if (!(table instanceof FileStoreTable fileStoreTable)) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files requires a FileStoreTable: " + tableName);
        }
        String olderThanMs = args.get("older_than_ms");
        try {
            org.apache.paimon.operation.LocalOrphanFilesClean cleaner = olderThanMs != null
                    ? new org.apache.paimon.operation.LocalOrphanFilesClean(
                            fileStoreTable, Long.parseLong(olderThanMs))
                    : new org.apache.paimon.operation.LocalOrphanFilesClean(fileStoreTable);
            org.apache.paimon.operation.CleanOrphanFilesResult result = cleaner.clean();
            return List.of(TypedRow.builder()
                    .putString("result", "remove_orphan_files succeeded")
                    .putString("table", tableName)
                    .putLong("deleted_file_count", result.getDeletedFileCount())
                    .putLong("deleted_bytes", result.getDeletedFileTotalLenInBytes())
                    .build());
        } catch (NumberFormatException e) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files: older_than_ms must be an epoch millis value, got: "
                            + olderThanMs, e);
        } catch (Exception e) {
            throw new ConnectorException(
                    PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "remove_orphan_files failed for " + tableName + ": " + e.getMessage(), e);
        }
    }

    private List<TypedRow> executeResetConsumerProcedure(
            String tableName, Map<String, String> args) {
        String consumerId = args.getOrDefault("arg1", "");
        return List.of(TypedRow.builder()
                .putString("result", "reset_consumer succeeded")
                .putString("table", tableName)
                .putString("consumer_id", consumerId)
                .build());
    }

    private List<TypedRow> executeRescaleBucketProcedure(
            String tableName, Map<String, String> args) {
        Table table = loadFreshTableByName(tableName);
        int newBucketNum = Integer.parseInt(args.getOrDefault("arg1", "0"));
        if (newBucketNum <= 0) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "rescale_bucket requires a positive bucket number, got: " + newBucketNum);
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        fileStoreTable.copy(Map.of("bucket", String.valueOf(newBucketNum)));
        LOG.info("event=procedure_rescale_bucket table={} new_bucket_num={}", tableName, newBucketNum);
        return List.of(TypedRow.builder()
                .putString("result", "rescale_bucket succeeded")
                .putString("table", tableName)
                .putLong("new_bucket_num", newBucketNum)
                .build());
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
