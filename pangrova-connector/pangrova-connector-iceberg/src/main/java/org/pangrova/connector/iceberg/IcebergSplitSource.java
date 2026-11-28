package org.pangrova.connector.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.iceberg.handle.IcebergSplit;

/** Owns one Iceberg file plan and converts only the requested batch of splits. */
final class IcebergSplitSource implements ConnectorSplitSource {
    private final String tableName;
    private final Supplier<CloseableIterable<FileScanTask>> planFiles;
    private final Function<FileScanTask, IcebergSplit> convert;
    private final SplitPlanningBudget budget; // Standalone consumers own their enumeration limits.
    private final long targetSize;
    private final int cacheLimit;
    private final Consumer<List<ConnectorSplit>> cacheResult;

    private CloseableIterable<FileScanTask> files;
    private CloseableIterator<FileScanTask> iterator;
    private List<ConnectorSplit> cacheCandidate;
    private IcebergSplit pendingFile;
    private List<Long> offsets;
    private int nextOffset;
    private long rangeStart;
    private int emitted;
    private boolean finished;

    IcebergSplitSource(String tableName, Supplier<CloseableIterable<FileScanTask>> planFiles,
                       Function<FileScanTask, IcebergSplit> convert, SplitPlanningBudget budget,
                       long targetSize, int cacheLimit, Consumer<List<ConnectorSplit>> cacheResult) {
        this.tableName = tableName;
        this.planFiles = planFiles;
        this.convert = convert;
        this.budget = budget;
        this.targetSize = targetSize;
        this.cacheLimit = cacheLimit;
        this.cacheResult = cacheResult;
        this.cacheCandidate = cacheResult == null ? null : new ArrayList<>();
    }

    @Override
    public synchronized List<ConnectorSplit> getNextBatch(int maxSize) {
        if (maxSize <= 0) throw new IllegalArgumentException("maxSize must be positive");
        if (finished) return List.of();
        try {
            checkBudget(emitted);
            if (files == null) {
                files = planFiles.get();
                iterator = files.iterator();
            }
            var batch = new ArrayList<ConnectorSplit>(Math.min(maxSize, 256));
            while (batch.size() < maxSize) {
                if (pendingFile == null && !iterator.hasNext()) {
                    checkBudget(emitted);
                    finishEnumeration();
                    break;
                }
                checkBudget(emitted + 1L);
                var split = nextSplit();
                checkBudget(++emitted);
                batch.add(split);
                retainForCache(split);
            }
            return batch.isEmpty() ? List.of() : Collections.unmodifiableList(batch);
        } catch (RuntimeException | Error failure) {
            try {
                close();
            } catch (RuntimeException | Error closing) {
                if (closing != failure) failure.addSuppressed(closing);
            }
            throw failure;
        }
    }

    private void checkBudget(long count) {
        if (budget != null) budget.check(count);
    }

    private ConnectorSplit nextSplit() {
        if (pendingFile == null) {
            FileScanTask task = iterator.next();
            IcebergSplit split = convert.apply(task);
            if (targetSize <= 0 || task.file().format() != FileFormat.PARQUET
                    || task.start() != 0 || task.length() != task.file().fileSizeInBytes()
                    || task.length() <= targetSize) {
                return split;
            }
            var manifestOffsets = task.file().splitOffsets();
            if (!hasUsableOffsets(manifestOffsets, task.length())) return split;
            pendingFile = split;
            offsets = manifestOffsets;
            nextOffset = 1;
            rangeStart = 0;
        }

        long end = pendingFile.fileLength();
        while (nextOffset < offsets.size()) {
            long candidate = offsets.get(nextOffset++);
            if (candidate - rangeStart >= targetSize) {
                end = candidate;
                break;
            }
        }
        if (rangeStart == 0 && end == pendingFile.fileLength()) {
            var wholeFile = pendingFile;
            clearPendingFile();
            return wholeFile;
        }
        var range = pendingFile.withRange(rangeStart, end - rangeStart);
        rangeStart = end;
        if (end == pendingFile.fileLength()) clearPendingFile();
        return range;
    }

    private static boolean hasUsableOffsets(List<Long> offsets, long fileLength) {
        if (offsets == null || offsets.size() < 2) return false;
        long previous = -1;
        for (Long offset : offsets) {
            if (offset == null || offset <= previous || offset >= fileLength) return false;
            previous = offset;
        }
        return true;
    }

    private void clearPendingFile() {
        pendingFile = null;
        offsets = null;
    }

    private void retainForCache(ConnectorSplit split) {
        if (cacheCandidate == null) return;
        if (cacheCandidate.size() < cacheLimit) cacheCandidate.add(split);
        else cacheCandidate = null;
    }

    private void finishEnumeration() {
        var candidate = cacheCandidate;
        close();
        if (candidate != null) cacheResult.accept(List.copyOf(candidate));
    }

    @Override
    public synchronized boolean isFinished() {
        return finished;
    }

    @Override
    public synchronized void close() {
        if (finished) return;
        finished = true;
        cacheCandidate = null;
        iterator = null;
        clearPendingFile();
        if (files == null) return;
        var plan = files;
        files = null;
        try {
            // The SDK's plan iterable owns its concat/parallel iterator and background work.
            plan.close();
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to close Iceberg file planning for " + tableName, failure);
        }
    }
}
