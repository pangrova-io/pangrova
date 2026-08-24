package org.pangrova.connector.paimon.fs;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads column values from Paimon data files for index building.
 *
 * <p>Uses Paimon's read API with column projection to minimize I/O.
 * Null values are preserved as {@code null} in the output list.</p>
 */
public final class PaimonColumnValueReader implements ColumnValueReader {

    private final Table table;

    public PaimonColumnValueReader(Table table) {
        if (table == null) throw new IllegalArgumentException("table must not be null");
        this.table = table;
    }

    @Override
    public Iterator<List<Comparable<?>>> readColumns(String dataFilePath, List<String> columnNames)
            throws IOException {
        RowType rowType = table.rowType();
        int[] projectedIndices = resolveColumnIndices(rowType, columnNames);
        DataType[] projectedTypes = resolveProjectedTypes(rowType, projectedIndices);

        ReadBuilder readBuilder = table.newReadBuilder()
                .withProjection(projectedIndices);

        try {
            RecordReader<InternalRow> reader = readBuilder.newRead()
                    .createReader(readBuilder.newScan().plan());

            return new PaimonRowIterator(reader, columnNames.size(), projectedTypes);
        } catch (Exception e) {
            throw new IOException("Failed to read columns from " + dataFilePath, e);
        }
    }

    private int[] resolveColumnIndices(RowType rowType, List<String> columnNames) {
        List<DataField> fields = rowType.getFields();
        int[] indices = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            int idx = findFieldIndex(fields, colName);
            if (idx < 0) {
                throw new IllegalArgumentException(
                        "Column '" + colName + "' not found in table schema");
            }
            indices[i] = idx;
        }
        return indices;
    }

    private static int findFieldIndex(List<DataField> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static DataType[] resolveProjectedTypes(RowType rowType, int[] indices) {
        List<DataField> fields = rowType.getFields();
        DataType[] types = new DataType[indices.length];
        for (int i = 0; i < indices.length; i++) {
            types[i] = fields.get(indices[i]).type();
        }
        return types;
    }

    @Override
    public void close() {
        // Table lifecycle managed externally
    }

    private static final class PaimonRowIterator implements Iterator<List<Comparable<?>>> {

        private final RecordReader<InternalRow> reader;
        private final int columnCount;
        private final DataType[] columnTypes;
        private RecordReader.RecordIterator<InternalRow> currentBatch;
        private InternalRow nextRow;
        private boolean exhausted;

        PaimonRowIterator(RecordReader<InternalRow> reader, int columnCount,
                          DataType[] columnTypes) throws IOException {
            this.reader = reader;
            this.columnCount = columnCount;
            this.columnTypes = columnTypes;
            this.exhausted = false;
            advanceBatch();
        }

        @Override
        public boolean hasNext() {
            if (exhausted) return false;
            if (nextRow != null) return true;
            try {
                advance();
            } catch (IOException e) {
                ResourceCloser.closeQuietly(reader);
                throw new RuntimeException("Failed to read next row", e);
            }
            return nextRow != null;
        }

        @Override
        public List<Comparable<?>> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            List<Comparable<?>> row = extractRow(nextRow);
            nextRow = null;
            return row;
        }

        private List<Comparable<?>> extractRow(InternalRow internalRow) {
            List<Comparable<?>> values = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                if (internalRow.isNullAt(i)) {
                    values.add(null);
                } else {
                    values.add(extractComparable(internalRow, i, columnTypes[i]));
                }
            }
            return values;
        }

        private static Comparable<?> extractComparable(InternalRow row, int index,
                                                        DataType dataType) {
            DataTypeRoot typeRoot = dataType.getTypeRoot();
            return switch (typeRoot) {
                case BOOLEAN -> row.getBoolean(index);
                case TINYINT -> row.getByte(index);
                case SMALLINT -> row.getShort(index);
                case INTEGER, DATE -> row.getInt(index);
                case BIGINT -> row.getLong(index);
                case FLOAT -> row.getFloat(index);
                case DOUBLE -> row.getDouble(index);
                case CHAR, VARCHAR -> row.getString(index).toString();
                case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                        row.getTimestamp(index, 6).getMillisecond();
                default -> null;
            };
        }

        private void advance() throws IOException {
            while (currentBatch != null) {
                nextRow = currentBatch.next();
                if (nextRow != null) return;
                currentBatch.releaseBatch();
                advanceBatch();
            }
            exhausted = true;
        }

        private void advanceBatch() throws IOException {
            currentBatch = reader.readBatch();
            if (currentBatch == null) {
                exhausted = true;
                ResourceCloser.closeQuietly(reader);
            }
        }
    }
}
