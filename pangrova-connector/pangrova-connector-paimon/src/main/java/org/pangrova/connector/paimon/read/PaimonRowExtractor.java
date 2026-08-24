package org.pangrova.connector.paimon.read;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;

/**
 * Extracts typed values from Paimon InternalRow by ordinal and DataType.
 * Centralizes the type-dispatch logic for row field access.
 */
final class PaimonRowExtractor {

    private PaimonRowExtractor() {}

    static Object getValue(InternalRow row, int ordinal, DataType type) {
        if (row.isNullAt(ordinal)) {
            return null;
        }
        return extractNonNull(row, ordinal, type);
    }

    private static Object extractNonNull(InternalRow row, int ordinal, DataType type) {
        return switch (type.getTypeRoot()) {
            case BOOLEAN -> row.getBoolean(ordinal);
            case TINYINT -> row.getByte(ordinal);
            case SMALLINT -> row.getShort(ordinal);
            case INTEGER, DATE -> row.getInt(ordinal);
            case BIGINT -> row.getLong(ordinal);
            case FLOAT -> row.getFloat(ordinal);
            case DOUBLE -> row.getDouble(ordinal);
            case CHAR, VARCHAR -> row.getString(ordinal).toString();
            case DECIMAL -> {
                DecimalType dt = (DecimalType) type;
                yield row.getDecimal(ordinal, dt.getPrecision(), dt.getScale()).toBigDecimal();
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE -> row.getTimestamp(ordinal, 6).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> row.getTimestamp(ordinal, 6).toLocalDateTime();
            case BINARY, VARBINARY -> row.getBinary(ordinal);
            case TIME_WITHOUT_TIME_ZONE -> row.getInt(ordinal);
            default -> throw new IllegalArgumentException("Unsupported Paimon type: " + type.getTypeRoot());
        };
    }
}
