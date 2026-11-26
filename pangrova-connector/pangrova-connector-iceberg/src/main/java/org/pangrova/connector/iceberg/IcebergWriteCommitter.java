package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.IcebergCommitExecutor.Completion;
import org.pangrova.connector.iceberg.exception.CommitNotificationException;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.api.metadata.DeleteFilter;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes the commit leg of the Iceberg distributed write path: aggregates
 * worker fragments, commits them atomically (one snapshot) with timeout and
 * retry protection, cleans orphan files on definitive failures, and merges
 * deletion vectors targeting the same data file (Iceberg v3).
 */
final class IcebergWriteCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteCommitter.class);

    private final IcebergCommitExecutor commitExecutor;
    private final IcebergCommitNotifier notifier;
    private final Catalog catalog;

    IcebergWriteCommitter(Catalog catalog, String catalogName, long commitTimeoutMs,
            IcebergCommitNotifier notifier) {
        this.catalog = catalog;
        this.notifier = notifier;
        this.commitExecutor = new IcebergCommitExecutor(catalogName, commitTimeoutMs);
    }

    /**
     * Commits all collected fragments for a distributed write. On a DEFINITIVE
     * (non-timeout) failure the worker-written files are deleted best-effort;
     * on a timeout the outcome is AMBIGUOUS so files are left for maintenance
     * cleanup (RemoveOrphanFiles) instead of risking deletion of committed data.
     *
     * @return the commit completion and whether this INSERT has no data or delete files
     */
    DistributedWriteCompletion finishDistributedWrite(Table icebergTable,
                                   IcebergOutputTableHandle handle,
                                   List<byte[]> fragments,
                                   String tenantId,
                                   DistributedWriteCommitContext context) {
        if (!IcebergDistributedCommitMarker.isPublished(icebergTable, context)) {
            ((org.apache.iceberg.HasTableOperations) IcebergDeclarationCommit.bind(
                    catalog, icebergTable, handle)).operations().current();
        }
        IcebergCollectedFragments collected =
                IcebergFragmentParser.collectAllFragments(icebergTable, handle, fragments);
        boolean cdc = context != null && context.cdcSource() != null;
        boolean cdcMutation = cdc && !IcebergOutputTableHandle.MODE_INSERT.equals(handle.writeMode());
        boolean hasCdcFragment = IcebergFragmentParser.CDC_MERGE.equals(collected.operation());
        if (cdcMutation != hasCdcFragment || (cdcMutation && (!handle.requiresSingleCdcRange()
                || fragments.size() != 1))) {
            throw new UnsupportedOperationException("Iceberg CDC MERGE requires one typed mutation fragment");
        }
        if (cdc && !cdcMutation && collected.operation() != null && !"APPEND".equals(collected.operation())) {
            throw new UnsupportedOperationException("Iceberg CDC INSERT accepts only append fragments");
        }
        boolean emptyInsert = IcebergOutputTableHandle.MODE_INSERT.equals(handle.writeMode())
                && collected.dataFiles().isEmpty() && collected.deleteFiles().isEmpty();
        if (collected.operation() == null) {
            return new DistributedWriteCompletion(handleEmptyFragments(icebergTable, handle, tenantId, context),
                    emptyInsert);
        }
        Completion completion;
        try {
            completion = commitCollectedFragments(icebergTable, handle, collected, tenantId, context);
        } catch (RuntimeException e) {
            handleCommitFailure(icebergTable, handle, collected, e, context);
            throw e;
        }
        LOG.info("event=distributed_write_committed table={}.{}", handle.schemaName(), handle.tableName());
        return new DistributedWriteCompletion(completion, emptyInsert);
    }

    record DistributedWriteCompletion(Completion completion, boolean emptyInsert) {}

    /** An explicitly required empty INSERT still needs its own fenced, marker-bearing catalog snapshot. */
    Snapshot publishedSnapshotOrCommitEmptyInsert(Table table, IcebergOutputTableHandle handle, boolean emptyInsert,
            String tenant, DistributedWriteCommitContext context) {
        Snapshot published = IcebergDistributedCommitMarker.publishedSnapshot(table, context);
        if (published != null) {
            if (emptyInsert) {
                // Empty INSERTs bypass the ordinary commit task, including its notification.
                // Replay the exact marker even when the snapshot itself is already committed.
                notifyObservedCommit(published, handle, List.of(), CommitEventType.APPEND, tenant, context);
            }
            return published;
        }
        if (!emptyInsert) {
            throw new IllegalStateException("Required nonempty write has no exact published snapshot marker");
        }
        commitExecutor.executeIdempotently(table, context, () -> {
            var append = IcebergDeclarationCommit.bind(catalog, table, handle).newAppend();
            IcebergDistributedCommitMarker.apply(append, context);
            IcebergSnapshotDeclarations.apply(append, handle);
            append.commit();
        });
        return notifyAfterCommit(table, handle, List.of(), CommitEventType.APPEND, tenant, context);
    }

    private Completion handleEmptyFragments(
            Table icebergTable,
            IcebergOutputTableHandle handle,
            String tenantId,
            DistributedWriteCommitContext context) {
        // Empty OVERWRITE clears only the allocation's static scope, or the whole table if unscoped.
        // For INSERT/DELETE modes, this is a true noop.
        if (IcebergOutputTableHandle.MODE_OVERWRITE.equals(handle.writeMode())) {
            LOG.info("event=distributed_write_empty_overwrite table={}.{}",
                    handle.schemaName(), handle.tableName());
            return commitCollectedFragments(icebergTable, handle,
                    new IcebergCollectedFragments("OVERWRITE", List.of(), List.of()), tenantId, context);
        }
        if (context != null && context.copyFile() != null) {
            icebergTable.refresh();
            if (!IcebergDistributedCommitMarker.isPublished(icebergTable, context)) {
                throw new IllegalStateException("COPY cannot complete without a marker-bearing Iceberg snapshot");
            }
        } else {
            LOG.info("event=distributed_write_noop table={}.{} reason=no_fragments",
                    handle.schemaName(), handle.tableName());
        }
        return Completion.TASK_COMPLETED;
    }

    /**
     * F-META-001: on a DEFINITIVE (non-timeout) commit failure the worker-written
     * files were never referenced by any snapshot and would accumulate as orphans
     * in storage — delete them best-effort. On a timeout the outcome is AMBIGUOUS
     * (the commit may have landed), so leave them for maintenance cleanup.
     */
    private void handleCommitFailure(Table icebergTable, IcebergOutputTableHandle handle,
                                     IcebergCollectedFragments collected, RuntimeException e,
                                     DistributedWriteCommitContext context) {
        if (context != null || e instanceof CommitStateUnknownException
                || e instanceof CommitNotificationException) {
            LOG.warn("event=distributed_write_commit_timeout_orphans_possible table={}.{} "
                            + "dataFiles={} deleteFiles={} note=left_for_maintenance_cleanup",
                    handle.schemaName(), handle.tableName(),
                    collected.dataFiles().size(), collected.deleteFiles().size());
        } else {
            cleanupOrphanFiles(icebergTable, handle, collected);
        }
    }

    /**
     * A-EOS-02: discards worker-staged data files that were written but never
     * published (the 2PC abort/discard leg). Deletes the files referenced by the
     * fragments without touching any snapshot, so an aborted streaming prepare
     * leaves no orphans.
     */
    void abortDistributedWrite(Table icebergTable,
                               IcebergOutputTableHandle handle,
                               List<byte[]> fragments) {
        IcebergCollectedFragments collected =
                IcebergFragmentParser.collectAllFragments(icebergTable, handle, fragments);
        cleanupOrphanFiles(icebergTable, handle, collected);
        LOG.info("event=distributed_write_aborted table={}.{} dataFiles={} deleteFiles={}",
                handle.schemaName(), handle.tableName(),
                collected.dataFiles().size(), collected.deleteFiles().size());
    }

    /**
     * Deletes rows matching the filter via Iceberg's row-filter delete, committed
     * with the same timeout and retry protection as distributed writes. This path
     * carries no durable commit context, so a timed-out commit whose snapshot
     * advanced is reported as unknown rather than assumed successful.
     */
    void deleteByFilter(
            Table icebergTable, IcebergTableHandle handle,
            DeleteFilter filter, String tenantId) {
        IcebergDeclarationCommit.requireSupported(catalog, icebergTable);
        IcebergOutputTableHandle declaration = (IcebergOutputTableHandle)
                IcebergOutputHandles.forDelete(handle, icebergTable);
        List<String> changed = new ArrayList<>();
        if (!handle.tableLocation().equals(declaration.tableLocation())) changed.add("location");
        if (!handle.schemaJson().equals(declaration.schemaJson())) changed.add("schema");
        if (!handle.partitionSpecJson().equals(declaration.partitionSpecJson())) changed.add("partition spec");
        if (handle.formatVersion() != declaration.formatVersion()) changed.add("format version");
        if (!IcebergDeclarationProperties.capture(handle.properties())
                .equals(declaration.declarationProperties())) {
            changed.add("properties");
        }
        if (!changed.isEmpty()) {
            throw new IllegalStateException("Iceberg write declaration changed since allocation: "
                    + handle.schemaName() + "." + handle.tableName() + " (" + String.join(", ", changed) + ")");
        }
        Table fencedTable = IcebergDeclarationCommit.bind(catalog, icebergTable, declaration);
        Expression icebergExpr = IcebergDeleteFilterTranslator.toIcebergExpression(
                filter, fencedTable.schema());
        String tableId = handle.schemaName() + "." + handle.tableName();
        try {
            commitExecutor.execute(icebergTable, null, () ->
                    commitExecutor.retry(() -> {
                        var delete = fencedTable.newDelete().deleteFromRowFilter(icebergExpr);
                        IcebergSnapshotDeclarations.apply(delete, declaration);
                        delete.commit();
                    }), tableId);
        } catch (org.apache.iceberg.exceptions.ValidationException e) {
            throw new UnsupportedOperationException(
                    "Iceberg deleteFromRowFilter cannot handle partial-file filter: " + e.getMessage(), e);
        }
        LOG.info("event=delete_by_filter table={}.{} filter={}",
                handle.schemaName(), handle.tableName(), filter);

        icebergTable.refresh();
        Snapshot snapshot = icebergTable.currentSnapshot();
        String snapshotId = snapshot != null ? String.valueOf(snapshot.snapshotId()) : "";
        notifier.notifyCommit(tenantId, handle.schemaName(), handle.tableName(),
                List.of(), List.of(), snapshotId, CommitEventType.DELETE);
    }

    /**
     * Executes a single atomic commit for all collected files based on the
     * resolved operation type. Produces exactly one Iceberg snapshot.
     */
    private Completion commitCollectedFragments(Table icebergTable,
                                          IcebergOutputTableHandle handle,
                                          IcebergCollectedFragments collected,
                                          String tenantId,
                                          DistributedWriteCommitContext context) {
        String tableId = handle.schemaName() + "." + handle.tableName();
        return switch (collected.operation()) {
            case "APPEND" ->
                    commitExecutor.execute(icebergTable, context, () ->
                            commitAtomicAppend(
                                    icebergTable, handle, collected.dataFiles(), tenantId, context), tableId);
            case "OVERWRITE" ->
                    commitExecutor.execute(icebergTable, context, () ->
                            commitAtomicOverwrite(
                                    icebergTable, handle, collected.dataFiles(), tenantId, context), tableId);
            case "DELETE" ->
                    commitExecutor.execute(icebergTable, context, () ->
                            commitAtomicDelete(
                                    icebergTable, handle, collected.deleteFiles(), tenantId, context), tableId);
            case "MERGE", IcebergFragmentParser.CDC_MERGE ->
                    commitExecutor.execute(icebergTable, context, () ->
                            commitAtomicMerge(icebergTable, handle,
                                    collected.dataFiles(), collected.deleteFiles(), tenantId, context), tableId);
            default -> throw new IllegalStateException(
                    "Unknown operation: " + collected.operation());
        };
    }

    /**
     * F-META-001: best-effort deletes worker-written files that were never committed
     * to any snapshot after a DEFINITIVE (non-timeout) commit failure, preventing
     * orphan accumulation in object storage. Each deletion is isolated so one failure
     * does not block the rest, and every failure is logged so operators can reconcile
     * via maintenance orphan cleanup.
     */
    private void cleanupOrphanFiles(Table icebergTable, IcebergOutputTableHandle handle,
                                    IcebergCollectedFragments collected) {
        int deleted = 0;
        int failed = 0;
        for (DataFile df : collected.dataFiles()) {
            deleted++;
            if (!deleteQuietly(icebergTable, handle, df.path().toString())) {
                deleted--;
                failed++;
            }
        }
        for (DeleteFile df : collected.deleteFiles()) {
            deleted++;
            if (!deleteQuietly(icebergTable, handle, df.path().toString())) {
                deleted--;
                failed++;
            }
        }
        LOG.info("event=distributed_write_orphans_cleaned table={}.{} deleted={} failed={}",
                handle.schemaName(), handle.tableName(), deleted, failed);
    }

    private static boolean deleteQuietly(Table icebergTable, IcebergOutputTableHandle handle, String path) {
        try {
            icebergTable.io().deleteFile(path);
            return true;
        } catch (RuntimeException ex) {
            LOG.warn("event=orphan_file_delete_failed table={}.{} path={} error={}",
                    handle.schemaName(), handle.tableName(), path, ex.getMessage());
            return false;
        }
    }

    private void commitAtomicAppend(Table icebergTable,
                                    IcebergOutputTableHandle handle,
                                    List<DataFile> dataFiles,
                                    String tenantId,
                                    DistributedWriteCommitContext context) {
        if (dataFiles.isEmpty()) {
            return;
        }
        commitExecutor.executeIdempotently(icebergTable, context, () -> {
            var append = IcebergDeclarationCommit.bind(catalog, icebergTable, handle).newAppend();
            for (DataFile df : dataFiles) {
                append.appendFile(df);
            }
            IcebergDistributedCommitMarker.apply(append, context);
            IcebergSnapshotDeclarations.apply(append, handle);
            append.commit();
        });
        LOG.info("event=distributed_append_committed table={}.{} files={}",
                handle.schemaName(), handle.tableName(), dataFiles.size());

        notifyAfterCommit(
                icebergTable, handle, dataFiles, CommitEventType.APPEND, tenantId, context);
    }

    private void commitAtomicOverwrite(Table icebergTable,
                                       IcebergOutputTableHandle handle,
                                       List<DataFile> dataFiles,
                                       String tenantId,
                                       DistributedWriteCommitContext context) {
        commitExecutor.executeIdempotently(icebergTable, context, () -> {
            var overwrite = IcebergOverwriteCommit.build(
                    IcebergDeclarationCommit.bind(catalog, icebergTable, handle), handle, dataFiles);
            IcebergDistributedCommitMarker.apply(overwrite, context);
            IcebergSnapshotDeclarations.apply(overwrite, handle);
            overwrite.commit();
        });
        LOG.info("event=distributed_overwrite_committed table={}.{} files={}",
                handle.schemaName(), handle.tableName(), dataFiles.size());

        notifyAfterCommit(
                icebergTable, handle, dataFiles, CommitEventType.OVERWRITE, tenantId, context);
    }

    private void commitAtomicDelete(Table icebergTable,
                                    IcebergOutputTableHandle handle,
                                    List<DeleteFile> deleteFiles,
                                    String tenantId,
                                    DistributedWriteCommitContext context) {
        if (deleteFiles.isEmpty()) {
            return;
        }
        List<DeleteFile> mergedDeleteFiles = mergeDeletionVectorsByDataFile(
                icebergTable, handle, deleteFiles);
        // F-ICEBERG-01/04: validate from baseSnapshotId
        final long baseSnapshotId = handle.baseSnapshotId();
        commitExecutor.executeIdempotently(icebergTable, context, () -> {
            var rowDelta = IcebergDeclarationCommit.bind(catalog, icebergTable, handle).newRowDelta();
            if (baseSnapshotId != IcebergOutputTableHandle.NO_SNAPSHOT) {
                rowDelta.validateFromSnapshot(baseSnapshotId);
                rowDelta.validateNoConflictingDataFiles();
                rowDelta.validateNoConflictingDeleteFiles();
            }
            for (DeleteFile df : mergedDeleteFiles) {
                rowDelta.addDeletes(df);
            }
            IcebergDistributedCommitMarker.apply(rowDelta, context);
            IcebergSnapshotDeclarations.apply(rowDelta, handle);
            rowDelta.commit();
        });
        LOG.info("event=distributed_delete_committed table={}.{} deleteFiles={} afterMerge={}",
                handle.schemaName(), handle.tableName(), deleteFiles.size(),
                mergedDeleteFiles.size());

        Snapshot snapshot = committedSnapshot(icebergTable, context);
        String snapshotId = snapshot != null ? String.valueOf(snapshot.snapshotId()) : "";
        notifier.notifyCommit(tenantId, handle.schemaName(), handle.tableName(),
                List.of(), List.of(), snapshotId, CommitEventType.DELETE,
                context != null ? context.commitFingerprint() : null);
    }

    private void commitAtomicMerge(Table icebergTable,
                                   IcebergOutputTableHandle handle,
                                   List<DataFile> dataFiles,
                                   List<DeleteFile> deleteFiles,
                                   String tenantId,
                                   DistributedWriteCommitContext context) {
        boolean cdc = context != null && context.cdcSource() != null;
        if (!cdc && dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }
        List<DeleteFile> mergedDeleteFiles = mergeDeletionVectorsByDataFile(
                icebergTable, handle, deleteFiles);
        // F-ICEBERG-01/04: validate from baseSnapshotId
        final long baseSnapshotIdMerge = handle.baseSnapshotId();
        commitExecutor.executeIdempotently(icebergTable, context, () -> {
            var bound = cdc ? IcebergDeclarationCommit.bindCdcBase(catalog, icebergTable, handle)
                    : IcebergDeclarationCommit.bind(catalog, icebergTable, handle);
            var rowDelta = bound.newRowDelta();
            if (baseSnapshotIdMerge != IcebergOutputTableHandle.NO_SNAPSHOT) {
                rowDelta.validateFromSnapshot(baseSnapshotIdMerge);
                rowDelta.validateNoConflictingDataFiles();
                rowDelta.validateNoConflictingDeleteFiles();
            }
            for (DataFile df : dataFiles) {
                rowDelta.addRows(df);
            }
            for (DeleteFile df : mergedDeleteFiles) {
                rowDelta.addDeletes(df);
            }
            IcebergDistributedCommitMarker.apply(rowDelta, context);
            IcebergSnapshotDeclarations.apply(rowDelta, handle);
            rowDelta.commit();
        });
        LOG.info("event=distributed_merge_committed table={}.{} dataFiles={} deleteFiles={} afterMerge={}",
                handle.schemaName(), handle.tableName(), dataFiles.size(),
                deleteFiles.size(), mergedDeleteFiles.size());

        notifyAfterCommit(
                icebergTable, handle, dataFiles, CommitEventType.DELETE, tenantId, context);
    }

    /**
     * Groups deletion vector (DV) delete files by their referenced data file so
     * multiple DVs targeting the same data file are committed together — Iceberg
     * v3 allows at most one active DV per data file, and RowDelta merges them
     * into a single bitmap within a single commit. Non-DV delete files pass
     * through unchanged; tables with format version &lt; 3 return the input as-is.
     */
    private List<DeleteFile> mergeDeletionVectorsByDataFile(
            Table icebergTable,
            IcebergOutputTableHandle handle,
            List<DeleteFile> deleteFiles) {
        if (deleteFiles.size() <= 1) {
            return deleteFiles;
        }
        if (IcebergFormatVersions.extract(icebergTable) < 3) {
            return deleteFiles;
        }
        DvFilePartition partition = partitionDeleteFiles(deleteFiles);
        boolean hasDuplicates = partition.dvByDataFile().values().stream()
                .anyMatch(list -> list.size() > 1);
        if (!hasDuplicates) {
            return deleteFiles;
        }
        logDuplicateDvMerges(handle, partition.dvByDataFile());
        // Non-DV first, then DV grouped by data file; RowDelta merges the bitmaps.
        List<DeleteFile> result = new ArrayList<>(partition.nonDvFiles());
        for (List<DeleteFile> dvFiles : partition.dvByDataFile().values()) {
            result.addAll(dvFiles);
        }
        return result;
    }

    /** Counts DV files per referenced data file to detect duplicates. */
    private static DvFilePartition partitionDeleteFiles(List<DeleteFile> deleteFiles) {
        Map<String, List<DeleteFile>> dvByDataFile = new LinkedHashMap<>();
        List<DeleteFile> nonDvFiles = new ArrayList<>();
        for (DeleteFile df : deleteFiles) {
            if (isDeletionVector(df)) {
                String referencedPath = df.referencedDataFile();
                if (referencedPath != null && !referencedPath.isEmpty()) {
                    dvByDataFile.computeIfAbsent(referencedPath, k -> new ArrayList<>())
                            .add(df);
                    continue;
                }
            }
            nonDvFiles.add(df);
        }
        return new DvFilePartition(dvByDataFile, nonDvFiles);
    }

    private static void logDuplicateDvMerges(IcebergOutputTableHandle handle,
                                             Map<String, List<DeleteFile>> dvByDataFile) {
        for (var entry : dvByDataFile.entrySet()) {
            if (entry.getValue().size() > 1) {
                LOG.info("event=dv_merge_detected table={}.{} dataFile={} fragmentCount={}",
                        handle.schemaName(), handle.tableName(),
                        entry.getKey(), entry.getValue().size());
            }
        }
    }

    private static boolean isDeletionVector(DeleteFile df) {
        return df.content() == FileContent.POSITION_DELETES
                && df.format() != null
                && "puffin".equalsIgnoreCase(df.format().name());
    }

    /**
     * Sends a commit notification with the added file paths after a successful commit.
     */
    private Snapshot notifyAfterCommit(Table icebergTable,
                                   IcebergOutputTableHandle handle,
                                   List<DataFile> dataFiles,
                                   CommitEventType eventType,
                                   String tenantId,
                                   DistributedWriteCommitContext context) {
        Snapshot snapshot = committedSnapshot(icebergTable, context);
        notifyObservedCommit(snapshot, handle, dataFiles, eventType, tenantId, context);
        return snapshot;
    }

    /** Delivers the already-proven snapshot without another catalog observation. */
    private void notifyObservedCommit(Snapshot snapshot, IcebergOutputTableHandle handle,
            List<DataFile> dataFiles, CommitEventType eventType, String tenantId,
            DistributedWriteCommitContext context) {
        String snapshotId = snapshot != null ? String.valueOf(snapshot.snapshotId()) : "";
        List<String> addedPaths = dataFiles.stream()
                .map(df -> df.path().toString())
                .toList();
        notifier.notifyCommit(tenantId, handle.schemaName(), handle.tableName(),
                addedPaths, List.of(), snapshotId, eventType,
                context != null ? context.commitFingerprint() : null);
    }

    private static Snapshot committedSnapshot(
            Table table, DistributedWriteCommitContext context) {
        if (context == null) {
            return table.currentSnapshot();
        }
        table.refresh();
        Snapshot snapshot = IcebergDistributedCommitMarker.publishedSnapshot(table, context);
        if (snapshot == null) {
            throw new IllegalStateException(
                    "Iceberg committed snapshot marker is absent after write completion");
        }
        return snapshot;
    }

    /** Result of partitioning delete files into DV groups and pass-through files. */
    private record DvFilePartition(
            Map<String, List<DeleteFile>> dvByDataFile,
            List<DeleteFile> nonDvFiles) {}
}
