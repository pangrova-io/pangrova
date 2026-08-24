package org.pangrova.connector.iceberg;

import org.pangrova.common.concurrent.RetryingCommitWrapper;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.metadata.DeleteFilter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes the commit leg of the Iceberg distributed write path: aggregates
 * worker fragments, commits them atomically (one snapshot) with timeout and
 * retry protection, cleans orphan files on definitive failures, and merges
 * deletion vectors targeting the same data file (Iceberg v3).
 */
final class IcebergWriteCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteCommitter.class);

    /**
     * F-CONNECTOR-07: Bounded ceiling on the commit executor to prevent
     * unbounded thread creation under bursts of concurrent
     * {@code executeCommitWithTimeout} calls. Each commit runs the catalog
     * commit synchronously on this pool; per-catalog 8 concurrent commits is
     * sufficient for typical Iceberg deployments and bounds OS thread usage.
     */
    private static final int COMMIT_EXECUTOR_MAX_THREADS = 8;
    private static final int COMMIT_EXECUTOR_QUEUE_CAPACITY = 256;
    private static final long COMMIT_EXECUTOR_KEEP_ALIVE_SECONDS = 60L;
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    private final RetryingCommitWrapper commitRetry = RetryingCommitWrapper.withDefaults(
            e -> e instanceof CommitFailedException);
    private final CatalogExceptionClassifier exceptionClassifier = new CatalogExceptionClassifier();
    private final long commitTimeoutMs;
    private final ExecutorService commitExecutor;
    private final IcebergCommitNotifier notifier;

    IcebergWriteCommitter(String catalogName, long commitTimeoutMs, IcebergCommitNotifier notifier) {
        this.commitTimeoutMs = commitTimeoutMs;
        this.notifier = notifier;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                COMMIT_EXECUTOR_MAX_THREADS,
                COMMIT_EXECUTOR_MAX_THREADS,
                COMMIT_EXECUTOR_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(COMMIT_EXECUTOR_QUEUE_CAPACITY),
                r -> {
                    Thread t = new Thread(r, "iceberg-commit-" + catalogName);
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
        this.commitExecutor = executor;
    }

    /**
     * Commits all collected fragments for a distributed write. On a DEFINITIVE
     * (non-timeout) failure the worker-written files are deleted best-effort;
     * on a timeout the outcome is AMBIGUOUS so files are left for maintenance
     * cleanup (RemoveOrphanFiles) instead of risking deletion of committed data.
     */
    void finishDistributedWrite(Table icebergTable,
                                IcebergOutputTableHandle handle,
                                List<byte[]> fragments) {
        IcebergCollectedFragments collected =
                IcebergFragmentParser.collectAllFragments(icebergTable, handle, fragments);
        if (collected.operation() == null) {
            handleEmptyFragments(icebergTable, handle);
            return;
        }
        try {
            commitCollectedFragments(icebergTable, handle, collected);
        } catch (RuntimeException e) {
            handleCommitFailure(icebergTable, handle, collected, e);
            throw e;
        }
        LOG.info("event=distributed_write_committed table={}.{}",
                handle.schemaName(), handle.tableName());
    }

    private void handleEmptyFragments(Table icebergTable, IcebergOutputTableHandle handle) {
        // No fragments: for OVERWRITE mode, commit an empty overwrite (truncate).
        // For INSERT/DELETE modes, this is a true noop.
        if (IcebergOutputTableHandle.MODE_OVERWRITE.equals(handle.writeMode())) {
            LOG.info("event=distributed_write_empty_overwrite table={}.{}",
                    handle.schemaName(), handle.tableName());
            commitCollectedFragments(icebergTable, handle,
                    new IcebergCollectedFragments("OVERWRITE", List.of(), List.of()));
        } else {
            LOG.info("event=distributed_write_noop table={}.{} reason=no_fragments",
                    handle.schemaName(), handle.tableName());
        }
    }

    /**
     * F-META-001: on a DEFINITIVE (non-timeout) commit failure the worker-written
     * files were never referenced by any snapshot and would accumulate as orphans
     * in storage — delete them best-effort. On a timeout the outcome is AMBIGUOUS
     * (the commit may have landed), so leave them for maintenance cleanup.
     */
    private void handleCommitFailure(Table icebergTable, IcebergOutputTableHandle handle,
                                     IcebergCollectedFragments collected, RuntimeException e) {
        if (e instanceof CommitTimeoutException) {
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
     * with the same timeout and retry protection as distributed writes.
     */
    void deleteByFilter(Table icebergTable, IcebergTableHandle handle, DeleteFilter filter) {
        Expression icebergExpr = IcebergDeleteFilterTranslator.toIcebergExpression(
                filter, icebergTable.schema());
        String tableId = handle.schemaName() + "." + handle.tableName();
        try {
            executeCommitWithTimeout(() ->
                    commitRetry.execute(() ->
                            icebergTable.newDelete()
                                    .deleteFromRowFilter(icebergExpr)
                                    .commit()), tableId);
        } catch (org.apache.iceberg.exceptions.ValidationException e) {
            throw new UnsupportedOperationException(
                    "Iceberg deleteFromRowFilter cannot handle partial-file filter: " + e.getMessage(), e);
        }
        LOG.info("event=delete_by_filter table={}.{} filter={}",
                handle.schemaName(), handle.tableName(), filter);

        icebergTable.refresh();
        Snapshot snapshot = icebergTable.currentSnapshot();
        String snapshotId = snapshot != null ? String.valueOf(snapshot.snapshotId()) : "";
        notifier.notifyCommit(handle.schemaName(), handle.tableName(),
                List.of(), List.of(), snapshotId, CommitEventType.DELETE);
    }

    /**
     * Executes a single atomic commit for all collected files based on the
     * resolved operation type. Produces exactly one Iceberg snapshot.
     */
    private void commitCollectedFragments(Table icebergTable,
                                          IcebergOutputTableHandle handle,
                                          IcebergCollectedFragments collected) {
        if (collected.operation() == null) {
            return;
        }
        String tableId = handle.schemaName() + "." + handle.tableName();
        switch (collected.operation()) {
            case "APPEND" ->
                    executeCommitWithTimeout(() ->
                            commitAtomicAppend(icebergTable, handle, collected.dataFiles()), tableId);
            case "OVERWRITE" ->
                    executeCommitWithTimeout(() ->
                            commitAtomicOverwrite(icebergTable, handle, collected.dataFiles()), tableId);
            case "DELETE" ->
                    executeCommitWithTimeout(() ->
                            commitAtomicDelete(icebergTable, handle, collected.deleteFiles()), tableId);
            case "MERGE" ->
                    executeCommitWithTimeout(() ->
                            commitAtomicMerge(icebergTable, handle,
                                    collected.dataFiles(), collected.deleteFiles()), tableId);
            default -> throw new IllegalStateException(
                    "Unknown operation: " + collected.operation());
        }
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

    /**
     * Executes a commit action with timeout protection. If the catalog commit
     * hangs beyond commitTimeoutMs, throws CommitTimeoutException which routes
     * to retry via the transient error path.
     */
    private void executeCommitWithTimeout(Runnable commitAction, String tableId) {
        long startMs = System.nanoTime() / NANOS_PER_MILLISECOND;
        try {
            CompletableFuture.runAsync(commitAction, commitExecutor)
                    .get(commitTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            long elapsed = (System.nanoTime() / NANOS_PER_MILLISECOND) - startMs;
            throw new CommitTimeoutException(tableId, elapsed, commitTimeoutMs, e);
        } catch (ExecutionException e) {
            handleExecutionFailure(e, tableId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommitTimeoutException(tableId, 0, commitTimeoutMs, e);
        }
    }

    private void handleExecutionFailure(ExecutionException e, String tableId) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException re) {
            throw re;
        }
        throw exceptionClassifier.handleAndClassify(
                cause instanceof Exception ex ? ex : new RuntimeException(cause),
                "commit", tableId);
    }

    private void commitAtomicAppend(Table icebergTable,
                                    IcebergOutputTableHandle handle,
                                    List<DataFile> dataFiles) {
        if (dataFiles.isEmpty()) {
            return;
        }
        commitRetry.execute(() -> {
            icebergTable.refresh();
            var append = icebergTable.newAppend();
            for (DataFile df : dataFiles) {
                append.appendFile(df);
            }
            append.commit();
        });
        LOG.info("event=distributed_append_committed table={}.{} files={}",
                handle.schemaName(), handle.tableName(), dataFiles.size());

        notifyAfterCommit(icebergTable, handle, dataFiles, CommitEventType.APPEND);
    }

    private void commitAtomicOverwrite(Table icebergTable,
                                       IcebergOutputTableHandle handle,
                                       List<DataFile> dataFiles) {
        if (dataFiles.isEmpty()) {
            commitEmptyOverwrite(icebergTable, handle);
            return;
        }
        // F-ICEBERG-01/04: validate from baseSnapshotId to detect concurrent modifications
        final long baseSnapshotId = handle.baseSnapshotId();
        commitRetry.execute(() -> {
            icebergTable.refresh();
            var overwrite = icebergTable.newReplacePartitions();
            if (baseSnapshotId != IcebergOutputTableHandle.NO_SNAPSHOT) {
                overwrite.validateFromSnapshot(baseSnapshotId);
                overwrite.validateNoConflictingData();
                overwrite.validateNoConflictingDeletes();
            }
            for (DataFile df : dataFiles) {
                overwrite.addFile(df);
            }
            overwrite.commit();
        });
        LOG.info("event=distributed_overwrite_committed table={}.{} files={}",
                handle.schemaName(), handle.tableName(), dataFiles.size());

        notifyAfterCommit(icebergTable, handle, dataFiles, CommitEventType.OVERWRITE);
    }

    private void commitEmptyOverwrite(Table icebergTable, IcebergOutputTableHandle handle) {
        commitRetry.execute(() -> {
            icebergTable.refresh();
            icebergTable.newDelete()
                    .deleteFromRowFilter(
                            Expressions.alwaysTrue())
                    .commit();
        });
        LOG.info("event=distributed_overwrite_empty_committed table={}.{}",
                handle.schemaName(), handle.tableName());
        notifyAfterCommit(icebergTable, handle, List.of(), CommitEventType.OVERWRITE);
    }

    private void commitAtomicDelete(Table icebergTable,
                                    IcebergOutputTableHandle handle,
                                    List<DeleteFile> deleteFiles) {
        if (deleteFiles.isEmpty()) {
            return;
        }
        List<DeleteFile> mergedDeleteFiles = mergeDeletionVectorsByDataFile(
                icebergTable, handle, deleteFiles);
        // F-ICEBERG-01/04: validate from baseSnapshotId
        final long baseSnapshotId = handle.baseSnapshotId();
        commitRetry.execute(() -> {
            icebergTable.refresh();
            var rowDelta = icebergTable.newRowDelta();
            if (baseSnapshotId != IcebergOutputTableHandle.NO_SNAPSHOT) {
                rowDelta.validateFromSnapshot(baseSnapshotId);
                rowDelta.validateNoConflictingDataFiles();
                rowDelta.validateNoConflictingDeleteFiles();
            }
            for (DeleteFile df : mergedDeleteFiles) {
                rowDelta.addDeletes(df);
            }
            rowDelta.commit();
        });
        LOG.info("event=distributed_delete_committed table={}.{} deleteFiles={} afterMerge={}",
                handle.schemaName(), handle.tableName(), deleteFiles.size(),
                mergedDeleteFiles.size());

        Snapshot snapshot = icebergTable.currentSnapshot();
        String snapshotId = snapshot != null ? String.valueOf(snapshot.snapshotId()) : "";
        notifier.notifyCommit(handle.schemaName(), handle.tableName(),
                List.of(), List.of(), snapshotId, CommitEventType.DELETE);
    }

    private void commitAtomicMerge(Table icebergTable,
                                   IcebergOutputTableHandle handle,
                                   List<DataFile> dataFiles,
                                   List<DeleteFile> deleteFiles) {
        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            return;
        }
        List<DeleteFile> mergedDeleteFiles = mergeDeletionVectorsByDataFile(
                icebergTable, handle, deleteFiles);
        // F-ICEBERG-01/04: validate from baseSnapshotId
        final long baseSnapshotIdMerge = handle.baseSnapshotId();
        commitRetry.execute(() -> {
            icebergTable.refresh();
            var rowDelta = icebergTable.newRowDelta();
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
            rowDelta.commit();
        });
        LOG.info("event=distributed_merge_committed table={}.{} dataFiles={} deleteFiles={} afterMerge={}",
                handle.schemaName(), handle.tableName(), dataFiles.size(),
                deleteFiles.size(), mergedDeleteFiles.size());

        notifyAfterCommit(icebergTable, handle, dataFiles, CommitEventType.DELETE);
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
    private void notifyAfterCommit(Table icebergTable,
                                   IcebergOutputTableHandle handle,
                                   List<DataFile> dataFiles,
                                   CommitEventType eventType) {
        Snapshot snapshot = icebergTable.currentSnapshot();
        String snapshotId = snapshot != null ? String.valueOf(snapshot.snapshotId()) : "";
        List<String> addedPaths = dataFiles.stream()
                .map(df -> df.path().toString())
                .toList();
        notifier.notifyCommit(handle.schemaName(), handle.tableName(),
                addedPaths, List.of(), snapshotId, eventType);
    }

    /** Result of partitioning delete files into DV groups and pass-through files. */
    private record DvFilePartition(
            Map<String, List<DeleteFile>> dvByDataFile,
            List<DeleteFile> nonDvFiles) {}
}
