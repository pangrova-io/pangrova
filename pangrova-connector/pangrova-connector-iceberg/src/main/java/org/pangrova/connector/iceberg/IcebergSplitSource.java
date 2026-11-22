package org.pangrova.connector.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;

/** Owns one Iceberg file plan and converts only the requested batch of splits. */
final class IcebergSplitSource implements ConnectorSplitSource {
    private final String tableName;
    private final Supplier<CloseableIterable<FileScanTask>> planFiles;
    private final Function<FileScanTask, ConnectorSplit> convert;
    private final int cacheLimit;
    private final Consumer<List<ConnectorSplit>> cacheResult;

    private CloseableIterable<FileScanTask> files;
    private CloseableIterator<FileScanTask> iterator;
    private List<ConnectorSplit> cacheCandidate;
    private boolean finished;

    IcebergSplitSource(String tableName, Supplier<CloseableIterable<FileScanTask>> planFiles,
                       Function<FileScanTask, ConnectorSplit> convert) {
        this(tableName, planFiles, convert, 0, null);
    }

    IcebergSplitSource(String tableName, Supplier<CloseableIterable<FileScanTask>> planFiles,
                       Function<FileScanTask, ConnectorSplit> convert, int cacheLimit,
                       Consumer<List<ConnectorSplit>> cacheResult) {
        this.tableName = tableName;
        this.planFiles = planFiles;
        this.convert = convert;
        this.cacheLimit = cacheLimit;
        this.cacheResult = cacheResult;
        this.cacheCandidate = cacheResult == null ? null : new ArrayList<>();
    }

    @Override
    public synchronized List<ConnectorSplit> getNextBatch(int maxSize) {
        if (maxSize <= 0) throw new IllegalArgumentException("maxSize must be positive");
        if (finished) return List.of();
        try {
            if (files == null) {
                files = planFiles.get();
                iterator = files.iterator();
            }
            var batch = new ArrayList<ConnectorSplit>(Math.min(maxSize, 256));
            while (batch.size() < maxSize) {
                if (!iterator.hasNext()) {
                    finishEnumeration();
                    break;
                }
                var split = convert.apply(iterator.next());
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
