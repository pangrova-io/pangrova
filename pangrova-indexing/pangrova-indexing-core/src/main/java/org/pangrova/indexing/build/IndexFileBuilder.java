package org.pangrova.indexing.build;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.connector.spi.index.ColumnValueReader;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.spi.strategy.IndexStrategyProvider;
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
        String indexFilePath = layout.indexFilePath(descriptor, file.filePath());
        context.storageManager().writeIndex(fileSystemAccessor, indexFilePath, bytes);
        return new BuildFileMapping(
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
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
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
