package org.pangrova.indexing.build;

import org.pangrova.common.util.DigestUtils;

import java.io.IOException;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.connector.api.index.ColumnValueReader;
import org.pangrova.connector.api.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.data.IndexData;

final class IndexFileBuilder {
    private final IndexBuildContext context;
    private final IndexStorageLayout layout;

    IndexFileBuilder(IndexBuildContext context) {
        this.context = context;
        this.layout = new IndexStorageLayout(context.config().baseIndexPath());
    }

    BuildFileMapping build(
            IndexDescriptor descriptor,
            UUID attemptId,
            DataFileInfo file,
            FileSystemAccessor fileSystemAccessor,
            ColumnValueReader reader,
            BooleanSupplier cancelled)
            throws IOException {
        requireActive(cancelled);
        IndexStrategyProvider provider = context.engineRegistry().getProvider(descriptor.indexType());
        CountingRows rows = new CountingRows(
                reader.readColumns(file.filePath(), descriptor.columnNames()), cancelled);
        requireActive(cancelled);
        IndexData data;
        try {
            data = provider.build(rows, descriptor.properties());
            if (context.snapshotPinProvider() instanceof ScalarSnapshotPinLifecycle) {
                rows.requireExhausted();
            }
        } finally {
            // A strategy must never keep reading this source after returning.
            // Synchronization waits for any concurrent iterator access to finish.
            rows.seal();
        }
        requireActive(cancelled);
        byte[] bytes = context.serializer().serialize(data);
        requireActive(cancelled);
        String indexFilePath = layout.indexFilePath(descriptor, attemptId, file.filePath());
        context.storageManager().writeIndex(fileSystemAccessor, indexFilePath, bytes);
        return new BuildFileMapping(
                attemptId,
                descriptor.indexId(),
                file.filePath(),
                indexFilePath,
                rows.count(),
                bytes.length,
                checksum(bytes));
    }

    private static void requireActive(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean()) {
            throw new CancellationException("Index build was cancelled");
        }
    }

    private static String checksum(byte[] bytes) {
        return HexFormat.of().formatHex(
                DigestUtils.sha256().digest(bytes));
    }

    private static final class CountingRows implements Iterator<List<Comparable<?>>> {
        private final Iterator<List<Comparable<?>>> delegate;
        private final BooleanSupplier cancelled;
        private long count;
        private boolean exhausted;
        private boolean sealed;

        private CountingRows(Iterator<List<Comparable<?>>> delegate, BooleanSupplier cancelled) {
            this.delegate = delegate;
            this.cancelled = cancelled;
        }

        @Override
        public synchronized boolean hasNext() {
            requireUnsealed();
            requireActive(cancelled);
            if (!exhausted) {
                exhausted = !delegate.hasNext();
            }
            return !exhausted;
        }

        @Override
        public synchronized List<Comparable<?>> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows in data file");
            }
            count++;
            return delegate.next();
        }

        private synchronized void requireExhausted() {
            if (hasNext()) {
                throw new IllegalStateException("Scalar index strategy did not consume its complete source");
            }
        }

        private synchronized void seal() {
            sealed = true;
        }

        private void requireUnsealed() {
            if (sealed) {
                throw new IllegalStateException("Scalar source cursor has been sealed");
            }
        }

        private synchronized long count() {
            return count;
        }
    }
}
