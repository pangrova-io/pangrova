package org.pangrova.indexing.build;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import org.pangrova.indexing.storage.IndexStorageLayout;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexStrategyProvider;
import org.pangrova.spi.index.data.IndexData;

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
            ColumnValueReader reader)
            throws IOException {
        IndexStrategyProvider provider = context.engineRegistry().getProvider(descriptor.indexType());
        CountingRows rows = new CountingRows(reader.readColumns(file.filePath(), descriptor.columnNames()));
        IndexData data = provider.build(rows, descriptor.properties());
        byte[] bytes = context.serializer().serialize(data);
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

    private static String checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return Long.toHexString(crc.getValue());
    }

    private static final class CountingRows implements Iterator<List<Comparable<?>>> {
        private final Iterator<List<Comparable<?>>> delegate;
        private long count;

        private CountingRows(Iterator<List<Comparable<?>>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public List<Comparable<?>> next() {
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