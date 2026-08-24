package org.pangrova.connector.paimon.write;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Converts Map-based row representations to Paimon GenericRow.
 * Handles type coercion: String → BinaryString, numeric widening.
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
            values[i] = convertValue(ColumnValue.toRaw(cv), fields.get(i));
        }
        GenericRow genericRow = GenericRow.of(values);
        genericRow.setRowKind(rowKind);
        return genericRow;
    }

    private Object convertValue(Object value, DataField field) {
        if (value == null) {
            return null;
        }
        return switch (field.type().getTypeRoot()) {
            case VARCHAR, CHAR -> toBinaryString(value);
            case BIGINT -> toLong(value);
            case INTEGER -> toInt(value);
            case TINYINT -> toByte(value);
            case SMALLINT -> toShort(value);
            case DOUBLE -> toDouble(value);
            case FLOAT -> toFloat(value);
            case DATE -> toEpochDay(value);
            case TIME_WITHOUT_TIME_ZONE -> toTimeMillis(value);
            case DECIMAL -> toDecimal(value, field);
            case VARBINARY, BINARY -> toBytes(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
                    toPaimonTimestamp(value);
            default -> value;
        };
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

    private static org.apache.paimon.data.Decimal toDecimal(Object value, DataField field) {
        java.math.BigDecimal bd;
        if (value instanceof java.math.BigDecimal bigDec) {
            bd = bigDec;
        } else if (value instanceof Number n) {
            bd = new java.math.BigDecimal(n.toString());
        } else {
            bd = new java.math.BigDecimal(value.toString());
        }
        org.apache.paimon.types.DecimalType dt = (org.apache.paimon.types.DecimalType) field.type();
        return org.apache.paimon.data.Decimal.fromBigDecimal(bd, dt.getPrecision(), dt.getScale());
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
