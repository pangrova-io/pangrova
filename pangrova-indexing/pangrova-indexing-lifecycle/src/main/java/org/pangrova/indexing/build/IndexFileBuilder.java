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
        IndexData data = provider.build(rows, descriptor.properties());
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

        private CountingRows(Iterator<List<Comparable<?>>> delegate, BooleanSupplier cancelled) {
            this.delegate = delegate;
            this.cancelled = cancelled;
        }

        @Override
        public boolean hasNext() {
            requireActive(cancelled);
            return delegate.hasNext();
        }

        @Override
        public List<Comparable<?>> next() {
            requireActive(cancelled);
            if (!delegate.hasNext()) {
                throw new NoSuchElementException("No more rows in data file");
            }
            count++;
            return delegate.next();
        }

        private long count() {
            return count;
        }
    }
}
