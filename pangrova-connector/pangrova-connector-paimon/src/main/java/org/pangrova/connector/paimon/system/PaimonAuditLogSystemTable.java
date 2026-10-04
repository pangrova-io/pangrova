package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon changelog as an audit log via {@code table$audit_log}.
 *
 * <p>Each row includes the row kind (INSERT, UPDATE_BEFORE, UPDATE_AFTER,
 * DELETE) and the field values as strings.</p>
 */
public final class PaimonAuditLogSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonAuditLogSystemTable.class);

    private final Table table;
    private final List<String> columnNames;

    /** Creates an audit-log view over the given Paimon table. */
    public PaimonAuditLogSystemTable(Table table) {
        this.table = table;
        List<String> names = new ArrayList<>();
        names.add("row_kind");
        for (DataField field : table.rowType().getFields()) {
            names.add(field.name());
        }
        this.columnNames = List.copyOf(names);
    }

    @Override
    public List<String> columns() {
        return List.copyOf(columnNames);
    }

    @Override
    public List<TypedRow> rows() {
        List<TypedRow> rows = new ArrayList<>();
        try {
            ReadBuilder readBuilder = table.newReadBuilder();
            List<Split> splits = readBuilder.newScan().plan().splits();
            TableRead read = readBuilder.newRead();
            List<DataField> fields = table.rowType().getFields();
            for (Split split : splits) {
                readSplit(read, split, fields, rows);
            }
        } catch (Exception e) {
            LOG.warn("event=audit_log_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }

    private void readSplit(TableRead read, Split split, List<DataField> fields, List<TypedRow> rows)
            throws Exception {
        try (RecordReader<InternalRow> reader = read.createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                collectBatch(batch, fields, rows);
            }
        }
    }

    private void collectBatch(RecordReader.RecordIterator<InternalRow> batch,
                              List<DataField> fields, List<TypedRow> rows) throws Exception {
        InternalRow row;
        while ((row = batch.next()) != null) {
            rows.add(buildTypedRow(row, fields));
        }
    }

    private TypedRow buildTypedRow(InternalRow row, List<DataField> fields) {
        TypedRow.Builder builder = TypedRow.builder();
        builder.putString("row_kind", toRowKindName(row.getRowKind()));
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).name();
            builder.putString(fieldName, row.isNullAt(i) ? null : row.getString(i).toString());
        }
        return builder.build();
    }

    private static String toRowKindName(RowKind kind) {
        return switch (kind) {
            case INSERT -> "INSERT";
            case UPDATE_BEFORE -> "UPDATE_BEFORE";
            case UPDATE_AFTER -> "UPDATE_AFTER";
            case DELETE -> "DELETE";
        };
    }
}
