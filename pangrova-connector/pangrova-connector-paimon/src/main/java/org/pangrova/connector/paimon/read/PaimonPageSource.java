package org.pangrova.connector.paimon.read;

import org.pangrova.connector.paimon.PaimonSplitRegistry.ReadLease;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.PageOutputFormat;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.page.NativePageConverter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads Paimon data files via RecordReader and produces Pages of rows.
 * Paimon SDK handles LSM merge semantics transparently.
 *
 * <p>Preferred output is columnar {@link NativePage} via {@link #getNextNativePage()}.
 * Both page formats consume the same selected SDK reader.</p>
 */
public final class PaimonPageSource implements ConnectorPageSource {

    /** Default read batch size. Configurable via constructor. */
    public static final int DEFAULT_BATCH_SIZE = 1024;

    private final RecordReader<InternalRow> reader;
    private final ReadLease lease;
    private final List<PaimonColumnHandle> projectedColumns;
    private final RowType rowType;
    private final Map<Integer, Integer> fieldIdToOrdinal;
    private final int batchSize;
    private RecordReader.RecordIterator<InternalRow> currentBatch;
    private boolean finished;

    /** Creates a page source reading from the given record reader. */
    public PaimonPageSource(
            RecordReader<InternalRow> reader,
            List<PaimonColumnHandle> projectedColumns,
            RowType rowType,
            int batchSize,
            ReadLease lease) {
        this.reader = reader;
        this.lease = java.util.Objects.requireNonNull(lease, "lease");
        this.projectedColumns = List.copyOf(projectedColumns);
        this.rowType = rowType;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.finished = false;
        List<DataField> fields = rowType.getFields();
        Map<Integer, Integer> ordinalMap = new java.util.HashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            ordinalMap.put(fields.get(i).id(), i);
        }
        this.fieldIdToOrdinal = Map.copyOf(ordinalMap);
        for (PaimonColumnHandle column : projectedColumns) {
            if (!fieldIdToOrdinal.containsKey(column.fieldId())) {
                throw new IllegalArgumentException("Field ID not found: " + column.fieldId());
            }
        }
    }

    @Override
    public Page getNextPage() {
        if (finished) {
            return null;
        }
        List<TypedRow> rows = readBatch();
        if (rows.isEmpty()) {
            finished = true;
            return null;
        }
        List<String> columnNames = projectedColumns.stream()
                .map(PaimonColumnHandle::name)
                .toList();
        return new Page(columnNames, rows);
    }

    @Override
    public PageOutputFormat preferredFormat() {
        return PageOutputFormat.NATIVE;
    }

    @Override
    public NativePage getNextNativePage() {
        if (finished) {
            return null;
        }
        List<TypedRow> rows = readBatch();
        if (rows.isEmpty()) {
            finished = true;
            return null;
        }
        List<String> columnNames = projectedColumns.stream()
                .map(PaimonColumnHandle::name)
                .toList();
        Page page = new Page(columnNames, rows);
        return NativePageConverter.fromPage(page);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void close() {
        finished = true;
        releaseBatch();
        try {
            lease.close();
        } catch (IOException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to close Paimon reader", failure);
        }
    }

    private List<TypedRow> readBatch() {
        try {
            return doReadBatch();
        } catch (IOException e) {
            finished = true;
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to read Paimon data", e);
        }
    }

    private List<TypedRow> doReadBatch() throws IOException {
        List<TypedRow> rows = new ArrayList<>();
        while (rows.size() < batchSize) {
            InternalRow row = nextRow();
            if (row == null) {
                break;
            }
            rows.add(extractRow(row));
        }
        if (rows.isEmpty()) {
            finished = true;
        }
        return rows;
    }

    private InternalRow nextRow() throws IOException {
        while (true) {
            if (currentBatch != null) {
                InternalRow row = currentBatch.next();
                if (row != null) {
                    return row;
                }
                currentBatch.releaseBatch();
                currentBatch = null;
            }
            currentBatch = reader.readBatch();
            if (currentBatch == null) {
                return null;
            }
        }
    }

    private TypedRow extractRow(InternalRow row) {
        TypedRow.Builder builder = TypedRow.builder();
        List<DataField> fields = rowType.getFields();
        for (PaimonColumnHandle col : projectedColumns) {
            Integer ordinal = fieldIdToOrdinal.get(col.fieldId());
            if (ordinal == null) {
                throw new IllegalArgumentException("Field ID not found: " + col.fieldId());
            }
            builder.putRaw(col.name(),
                    PaimonRowExtractor.getValue(row, ordinal, fields.get(ordinal).type()));
        }
        return builder.build();
    }

    private void releaseBatch() {
        if (currentBatch != null) {
            currentBatch.releaseBatch();
            currentBatch = null;
        }
    }

}
