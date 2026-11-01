package org.pangrova.connector.paimon.system;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

/**
 * Exposes Paimon changelog as an audit log via {@code table$audit_log}.
 *
 * <p>Each row includes the row kind (INSERT, UPDATE_BEFORE, UPDATE_AFTER,
 * DELETE) and the field values as strings.</p>
 */
public final class PaimonAuditLogSystemTable implements ConnectorSystemTable {

    private static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final HexFormat HEX = HexFormat.of();
    private static final long NANOS_PER_MILLI = 1_000_000L;

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
            throw new IllegalStateException(
                    "Failed to read Paimon audit log for table " + table.name(), e);
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
        try {
            InternalRow row;
            while ((row = batch.next()) != null) {
                rows.add(buildTypedRow(row, fields));
            }
        } finally {
            batch.releaseBatch();
        }
    }

    private TypedRow buildTypedRow(InternalRow row, List<DataField> fields) {
        TypedRow.Builder builder = TypedRow.builder();
        builder.putString("row_kind", toRowKindName(row.getRowKind()));
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            builder.putString(field.name(), formatField(row, i, field.type()));
        }
        return builder.build();
    }

    static String formatField(DataGetters row, int position, DataType type) {
        if (row.isNullAt(position)) {
            return null;
        }
        return switch (type.getTypeRoot()) {
            case BOOLEAN -> Boolean.toString(row.getBoolean(position));
            case TINYINT -> Byte.toString(row.getByte(position));
            case SMALLINT -> Short.toString(row.getShort(position));
            case INTEGER -> Integer.toString(row.getInt(position));
            case BIGINT -> Long.toString(row.getLong(position));
            case FLOAT -> Float.toString(row.getFloat(position));
            case DOUBLE -> Double.toString(row.getDouble(position));
            case CHAR, VARCHAR -> row.getString(position).toString();
            case DECIMAL -> {
                DecimalType decimalType = (DecimalType) type;
                yield row.getDecimal(position, decimalType.getPrecision(), decimalType.getScale())
                        .toBigDecimal().toPlainString();
            }
            case DATE -> LocalDate.ofEpochDay(row.getInt(position)).toString();
            case TIME_WITHOUT_TIME_ZONE -> LocalTime.ofNanoOfDay(
                    row.getInt(position) * NANOS_PER_MILLI).format(TIME_FORMAT);
            case TIMESTAMP_WITHOUT_TIME_ZONE ->
                    row.getTimestamp(position, ((TimestampType) type).getPrecision())
                            .toLocalDateTime().format(TIMESTAMP_FORMAT);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                    row.getTimestamp(position, ((LocalZonedTimestampType) type).getPrecision())
                            .toInstant().toString();
            case BINARY, VARBINARY -> HEX.formatHex(row.getBinary(position));
            case VARIANT -> String.valueOf(row.getVariant(position));
            case BLOB -> String.valueOf(row.getBlob(position));
            case VECTOR -> String.valueOf(row.getVector(position));
            case ARRAY -> formatArray(row.getArray(position), ((ArrayType) type).getElementType());
            case MAP -> formatMap(row.getMap(position), (MapType) type);
            case ROW -> formatRow(row.getRow(position, ((RowType) type).getFieldCount()),
                    (RowType) type);
            default -> throw new IllegalArgumentException(
                    "Unsupported Paimon type: " + type.getTypeRoot());
        };
    }

    private static String formatArray(InternalArray array, DataType elementType) {
        StringBuilder formatted = new StringBuilder("[");
        for (int i = 0; i < array.size(); i++) {
            if (i > 0) {
                formatted.append(", ");
            }
            formatted.append(formatField(array, i, elementType));
        }
        return formatted.append(']').toString();
    }

    private static String formatMap(InternalMap map, MapType type) {
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        StringBuilder formatted = new StringBuilder("{");
        for (int i = 0; i < map.size(); i++) {
            if (i > 0) {
                formatted.append(", ");
            }
            formatted.append(formatField(keys, i, type.getKeyType()))
                    .append('=')
                    .append(formatField(values, i, type.getValueType()));
        }
        return formatted.append('}').toString();
    }

    private static String formatRow(InternalRow row, RowType type) {
        StringBuilder formatted = new StringBuilder("{");
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i > 0) {
                formatted.append(", ");
            }
            DataField field = type.getField(i);
            formatted.append(field.name())
                    .append('=')
                    .append(formatField(row, i, field.type()));
        }
        return formatted.append('}').toString();
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
