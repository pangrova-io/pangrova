package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Coerces engine-side Java values into the representations Iceberg's
 * {@link GenericRecord} and {@code GenericParquetWriter} accept.
 *
 * <p>Extracted from {@link AbstractIcebergPageSink} to keep that class under the 500-line
 * quality gate. It was already used as a utility from outside the sink hierarchy
 * ({@link RowToRecordMapper}), so a dedicated stateless holder is the natural home.</p>
 */
final class IcebergValueCoercion {

    private IcebergValueCoercion() {
    }

    /**
     * Coerces a Java value to the type expected by Iceberg's GenericRecord.
     */
    static Object coerceValue(Object value, Type icebergType) {
        if (value == null) return null;
        return switch (icebergType.typeId()) {
            case INTEGER -> (value instanceof Number n) ? n.intValue() : value;
            case LONG -> (value instanceof Number n) ? n.longValue() : value;
            case FLOAT -> (value instanceof Number n) ? n.floatValue() : value;
            case DOUBLE -> (value instanceof Number n) ? n.doubleValue() : value;
            case TIMESTAMP, TIMESTAMP_NANO -> coerceTimestamp(value);
            case DATE -> coerceDate(value);
            case DECIMAL -> coerceDecimal(value, (Types.DecimalType) icebergType);
            case LIST -> coerceList(value, (Types.ListType) icebergType.asNestedType());
            case MAP -> coerceMap(value, (Types.MapType) icebergType.asNestedType());
            case STRUCT -> coerceStruct(value, icebergType.asStructType());
            case VARIANT -> coerceToString(value);
            case BINARY, GEOMETRY, GEOGRAPHY -> coerceToBinary(value);
            case UNKNOWN -> throw new IllegalArgumentException(
                    "Iceberg v3 unknown column accepts only NULL");
            default -> value;
        };
    }

    /**
     * Rescales a BigDecimal to the exact column scale. Iceberg's decimal
     * writer rejects values whose scale differs from the column type
     * ("Cannot write value as decimal(p,s), wrong scale"), so SQL literals
     * like 129.99 (scale 2) must be widened losslessly to e.g. scale 18.
     * Values that would require rounding are passed through unchanged and
     * fail loud in the writer — silent precision loss is never acceptable.
     */
    private static Object coerceDecimal(Object value, Types.DecimalType type) {
        if (value instanceof java.math.BigDecimal bd && bd.scale() != type.scale()) {
            try {
                return bd.setScale(type.scale());
            } catch (ArithmeticException lossy) {
                return bd;
            }
        }
        return value;
    }

    private static Object coerceToString(Object value) {
        if (value instanceof String) {
            return value;
        }
        if (value instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer bb) {
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    private static Object coerceToBinary(Object value) {
        if (value instanceof byte[] b) {
            return ByteBuffer.wrap(b);
        }
        if (value instanceof ByteBuffer) {
            return value;
        }
        if (value instanceof String s) {
            return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
        }
        return value;
    }

    private static Object coerceTimestamp(Object value) {
        if (value instanceof java.time.LocalDateTime) {
            return value;
        }
        if (value instanceof java.time.OffsetDateTime odt) {
            return odt.toLocalDateTime();
        }
        if (value instanceof String s) {
            return java.time.LocalDateTime.parse(s);
        }
        return value;
    }

    private static Object coerceDate(Object value) {
        if (value instanceof java.time.LocalDate) {
            return value;
        }
        if (value instanceof String s) {
            return java.time.LocalDate.parse(s);
        }
        return value;
    }

    private static Object coerceList(Object value, Types.ListType listType) {
        if (!(value instanceof List<?> rawList)) return value;
        Type elementType = listType.elementType();
        List<Object> coerced = new ArrayList<>(rawList.size());
        for (Object elem : rawList) {
            coerced.add(coerceValue(elem, elementType));
        }
        return coerced;
    }

    private static Object coerceMap(Object value, Types.MapType mapType) {
        if (!(value instanceof Map<?, ?> rawMap)) return value;
        Type keyType = mapType.keyType();
        Type valueType = mapType.valueType();
        Map<Object, Object> coerced = new LinkedHashMap<>(rawMap.size());
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            Object coercedKey = coerceValue(entry.getKey(), keyType);
            Object coercedVal = coerceValue(entry.getValue(), valueType);
            coerced.put(coercedKey, coercedVal);
        }
        return coerced;
    }

    private static Object coerceStruct(Object value, Types.StructType structType) {
        if (value instanceof GenericRecord) return value;
        List<Types.NestedField> structFields = structType.fields();
        // Arrow StructVector.getObject() yields a Map. Native ROW constructors
        // use canonical anonymous field names; SQL INSERT binds them to target
        // STRUCT fields by position. Named maps may omit nullable target fields,
        // but cannot supply unknown fields or omit required target fields.
        if (value instanceof java.util.Map<?, ?> map) {
            boolean named = map.keySet().stream().allMatch(key -> key instanceof String name
                    && structFields.stream().anyMatch(field -> field.name().equals(name)))
                    && structFields.stream().allMatch(field -> field.isOptional() || map.containsKey(field.name()));
            boolean positional = map.size() == structFields.size();
            for (int i = 0; i < structFields.size() && positional; i++) {
                positional = map.containsKey(FunctionBindingDeclarations.anonymousComputedField(i));
            }
            if (!named && !positional) {
                throw new IllegalArgumentException(
                        "STRUCT source fields do not match target names or canonical positions");
            }
            GenericRecord record = GenericRecord.create(structType);
            for (int i = 0; i < structFields.size(); i++) {
                Types.NestedField field = structFields.get(i);
                Object fieldValue = map.get(named ? field.name()
                        : FunctionBindingDeclarations.anonymousComputedField(i));
                record.setField(field.name(), coerceValue(fieldValue, field.type()));
            }
            return record;
        }
        if (!(value instanceof List<?> fields)) return value;
        GenericRecord record = GenericRecord.create(structType);
        int limit = Math.min(fields.size(), structFields.size());
        for (int i = 0; i < limit; i++) {
            Types.NestedField field = structFields.get(i);
            record.setField(field.name(), coerceValue(fields.get(i), field.type()));
        }
        return record;
    }

    /**
     * Creates a Parquet-compatible schema by replacing Iceberg v3 special types
     * that GenericParquetWriter does not support.
     * VARIANT → STRING (JSON text), GEOMETRY/GEOGRAPHY → BINARY (WKB).
     * Iceberg v3 UNKNOWN columns are omitted from physical Parquet files.
     */
    static Schema sanitizeSchemaForParquet(Schema schema) {
        boolean needsSanitization = schema.columns().stream()
                .anyMatch(field -> isParquetUnsupported(field.type().typeId()));
        if (!needsSanitization) {
            return schema;
        }
        List<Types.NestedField> sanitized = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            if (field.type().typeId() != Type.TypeID.UNKNOWN) {
                sanitized.add(sanitizeField(field));
            }
        }
        return new Schema(sanitized);
    }

    /** The Iceberg v3 types {@code GenericParquetWriter} cannot write directly. */
    private static boolean isParquetUnsupported(Type.TypeID typeId) {
        return typeId == Type.TypeID.VARIANT || typeId == Type.TypeID.GEOMETRY
                || typeId == Type.TypeID.GEOGRAPHY || typeId == Type.TypeID.UNKNOWN;
    }

    /**
     * Maps one field to its Parquet-writable equivalent, leaving supported types untouched.
     * Extracted from {@link #sanitizeSchemaForParquet} to keep both methods within the project
     * cyclomatic-complexity budget.
     */
    private static Types.NestedField sanitizeField(Types.NestedField field) {
        Type.TypeID typeId = field.type().typeId();
        if (typeId == Type.TypeID.VARIANT) {
            return Types.NestedField.optional(
                    field.fieldId(), field.name(), Types.StringType.get());
        }
        if (typeId == Type.TypeID.GEOMETRY || typeId == Type.TypeID.GEOGRAPHY) {
            return Types.NestedField.optional(
                    field.fieldId(), field.name(), Types.BinaryType.get());
        }
        return field;
    }
}
