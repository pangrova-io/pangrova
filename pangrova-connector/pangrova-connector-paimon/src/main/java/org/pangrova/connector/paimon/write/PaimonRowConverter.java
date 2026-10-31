package org.pangrova.connector.paimon.write;

import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts typed rows and detached columnar values to Paimon GenericRow.
 * Handles scalar coercion and recursive ARRAY, MAP, and ROW conversion.
 * Supports RowKind assignment for DELETE/UPDATE operations on primary key tables.
 */
final class PaimonRowConverter {

    /** Nanoseconds in one millisecond, for nano-of-day to millis-of-day conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final List<DataField> fields;

    PaimonRowConverter(RowType rowType) {
        this.fields = rowType.getFields();
    }

    GenericRow convert(TypedRow row) {
        return convert(row, RowKind.INSERT);
    }

    GenericRow convert(TypedRow row, RowKind rowKind) {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            ColumnValue cv = row.get(fields.get(i).name());
            values[i] = convertValue(ColumnValue.toRaw(cv), fields.get(i).type());
        }
        GenericRow genericRow = GenericRow.of(values);
        genericRow.setRowKind(rowKind);
        return genericRow;
    }

    int[] columnOrdinals(List<String> columns) {
        int[] ordinals = new int[fields.size()];
        for (int column = 0; column < fields.size(); column++) {
            ordinals[column] = columns.indexOf(fields.get(column).name());
        }
        return ordinals;
    }

    GenericRow convertColumnarRow(ColumnarBatch batch, int row, int[] ordinals, RowKind rowKind) {
        GenericRow result = new GenericRow(rowKind, fields.size());
        for (int column = 0; column < fields.size(); column++) {
            Object value = ordinals[column] < 0 ? null : batch.value(row, ordinals[column]);
            result.setField(column, convertValue(value, fields.get(column).type()));
        }
        return result;
    }

    private Object convertValue(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        return switch (type.getTypeRoot()) {
            case VARCHAR, CHAR -> toBinaryString(value);
            case BIGINT -> toLong(value);
            case INTEGER -> toInt(value);
            case TINYINT -> toByte(value);
            case SMALLINT -> toShort(value);
            case DOUBLE -> toDouble(value);
            case FLOAT -> toFloat(value);
            case DATE -> toEpochDay(value);
            case TIME_WITHOUT_TIME_ZONE -> toTimeMillis(value);
            case DECIMAL -> toDecimal(value, (DecimalType) type);
            case VARBINARY, BINARY -> toBytes(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                    toPaimonTimestamp(value);
            case ARRAY -> toArray(value, (ArrayType) type);
            case MAP -> toMap(value, (MapType) type);
            case ROW -> toRow(value, (RowType) type);
            default -> value;
        };
    }

    private GenericArray toArray(Object value, ArrayType type) {
        List<?> elements = (List<?>) value;
        Object[] converted = new Object[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            converted[i] = convertValue(elements.get(i), type.getElementType());
        }
        return new GenericArray(converted);
    }

    private GenericMap toMap(Object value, MapType type) {
        Map<?, ?> entries = (Map<?, ?>) value;
        Map<Object, Object> converted = new LinkedHashMap<>(entries.size());
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            converted.put(convertValue(entry.getKey(), type.getKeyType()),
                    convertValue(entry.getValue(), type.getValueType()));
        }
        return new GenericMap(converted);
    }

    private GenericRow toRow(Object value, RowType type) {
        List<DataField> nestedFields = type.getFields();
        GenericRow converted = new GenericRow(nestedFields.size());
        if (value instanceof Map<?, ?> entries) {
            for (int i = 0; i < nestedFields.size(); i++) {
                DataField field = nestedFields.get(i);
                converted.setField(i, convertValue(entries.get(field.name()), field.type()));
            }
        } else {
            List<?> values = (List<?>) value;
            for (int i = 0; i < Math.min(values.size(), nestedFields.size()); i++) {
                converted.setField(i, convertValue(values.get(i), nestedFields.get(i).type()));
            }
        }
        return converted;
    }

    private static BinaryString toBinaryString(Object value) {
        return BinaryString.fromString(value.toString());
    }

    private static long toLong(Object value) {
        return ((Number) value).longValue();
    }

    private static int toInt(Object value) {
        return ((Number) value).intValue();
    }

    private static double toDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    private static float toFloat(Object value) {
        return ((Number) value).floatValue();
    }

    private static byte toByte(Object value) {
        return ((Number) value).byteValue();
    }

    private static short toShort(Object value) {
        return ((Number) value).shortValue();
    }

    private static int toTimeMillis(Object value) {
        if (value instanceof Number n) {
            return n.intValue();
        }
        // Parse time string (e.g. "13:45:30") to millis of day
        java.time.LocalTime time = java.time.LocalTime.parse(value.toString());
        return (int) (time.toNanoOfDay() / NANOS_PER_MILLI);
    }

    private static org.apache.paimon.data.Decimal toDecimal(Object value, DecimalType type) {
        java.math.BigDecimal bd;
        if (value instanceof java.math.BigDecimal bigDec) {
            bd = bigDec;
        } else if (value instanceof Number n) {
            bd = new java.math.BigDecimal(n.toString());
        } else {
            bd = new java.math.BigDecimal(value.toString());
        }
        org.apache.paimon.data.Decimal decimal = org.apache.paimon.data.Decimal.fromBigDecimal(
            bd, type.getPrecision(), type.getScale());
        if (decimal == null) {
            throw new ArithmeticException("Decimal exceeds target precision: " + type);
        }
        return decimal;
    }

    private static byte[] toBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int toEpochDay(Object value) {
        if (value instanceof LocalDate ld) {
            return (int) ld.toEpochDay();
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        return (int) LocalDate.parse(value.toString()).toEpochDay();
    }

    private static Timestamp toPaimonTimestamp(Object value) {
        if (value instanceof LocalDateTime ldt) {
            return Timestamp.fromLocalDateTime(ldt);
        }
        if (value instanceof java.sql.Timestamp ts) {
            return Timestamp.fromSQLTimestamp(ts);
        }
        return Timestamp.fromLocalDateTime(LocalDateTime.parse(value.toString()));
    }
}
