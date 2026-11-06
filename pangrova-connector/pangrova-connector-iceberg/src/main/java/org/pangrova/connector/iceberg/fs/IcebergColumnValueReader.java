package org.pangrova.connector.iceberg.fs;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.connector.spi.index.ColumnValueReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads column values from Iceberg data files (Parquet) for index building.
 *
 * <p>Uses Iceberg's Parquet reader with column projection to minimize I/O.
 * Null values are preserved as {@code null} in the output list.</p>
 */
public final class IcebergColumnValueReader implements ColumnValueReader {

    private final org.apache.iceberg.io.FileIO fileIO;
    private final Schema tableSchema;

    /**
     * Creates a reader over the given FileIO using the provided table schema for projection.
     */
    public IcebergColumnValueReader(org.apache.iceberg.io.FileIO fileIO, Schema tableSchema) {
        if (fileIO == null) throw new IllegalArgumentException("fileIO must not be null");
        if (tableSchema == null) throw new IllegalArgumentException("tableSchema must not be null");
        this.fileIO = fileIO;
        this.tableSchema = tableSchema;
    }

    @Override
    public Iterator<List<Comparable<?>>> readColumns(String dataFilePath, List<String> columnNames)
            throws IOException {
        Schema projectedSchema = tableSchema.select(columnNames);
        InputFile inputFile = fileIO.newInputFile(dataFilePath);

        CloseableIterable<Record> records = Parquet.read(inputFile)
                .project(projectedSchema)
                .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(projectedSchema, fileSchema))
                .build();

        return createRecordIterator(records, columnNames);
    }

    private Iterator<List<Comparable<?>>> createRecordIterator(
            CloseableIterable<Record> records, List<String> columnNames) {
        Iterator<Record> recordIterator = records.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                boolean has = recordIterator.hasNext();
                if (!has) {
                    ResourceCloser.closeQuietly(records);
                }
                return has;
            }

            @Override
            public List<Comparable<?>> next() {
                if (!recordIterator.hasNext()) {
                    ResourceCloser.closeQuietly(records);
                    throw new NoSuchElementException();
                }
                return extractRow(recordIterator.next(), columnNames);
            }
        };
    }

    private static List<Comparable<?>> extractRow(Record record, List<String> columnNames) {
        List<Comparable<?>> row = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            Object value = record.get(i);
            row.add(value instanceof Comparable<?> c ? c : null);
        }
        return row;
    }

    @Override
    public void close() {
        // FileIO lifecycle managed by IcebergFileSystemAccessor
    }
}
