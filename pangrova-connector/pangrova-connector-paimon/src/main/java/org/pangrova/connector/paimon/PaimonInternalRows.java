package org.pangrova.connector.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;

import java.util.List;

/**
 * Low-level helpers for copying and inspecting Paimon {@link InternalRow}s
 * used by the rewrite-based delete and overwrite paths.
 */
final class PaimonInternalRows {

    private PaimonInternalRows() {}

    /** Deep-copies an internal row into a GenericRow, preserving row kind. */
    static GenericRow copy(InternalRow row, List<DataField> fields) {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = getFieldValue(row, i, fields.get(i));
        }
        GenericRow copy = GenericRow.of(values);
        copy.setRowKind(row.getRowKind());
        return copy;
    }

    /** Extracts the typed value of one field from an internal row. */
    static Object getFieldValue(InternalRow row, int index, DataField field) {
        if (row.isNullAt(index)) {
            return null;
        }
        return switch (field.type().getTypeRoot()) {
            case BOOLEAN -> row.getBoolean(index);
            case TINYINT -> row.getByte(index);
            case SMALLINT -> row.getShort(index);
            case INTEGER, DATE, TIME_WITHOUT_TIME_ZONE -> row.getInt(index);
            case BIGINT -> row.getLong(index);
            case FLOAT -> row.getFloat(index);
            case DOUBLE -> row.getDouble(index);
            case VARCHAR, CHAR -> row.getString(index);
            case VARBINARY, BINARY -> row.getBinary(index);
            case DECIMAL -> {
                org.apache.paimon.types.DecimalType dt =
                        (org.apache.paimon.types.DecimalType) field.type();
                yield row.getDecimal(index, dt.getPrecision(), dt.getScale());
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                org.apache.paimon.types.TimestampType tt =
                        (org.apache.paimon.types.TimestampType) field.type();
                yield row.getTimestamp(index, tt.getPrecision());
            }
            default -> null;
        };
    }
}
