/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.transport.client;

import org.pangrova.transport.client.exception.TransportException;

import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.LogicalType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Decodes Arrow IPC stream bytes into a type-erased {@link ResultBatch}.
 * All Arrow imports are confined to this class; callers in {@code client-*} modules
 * work only with {@link ResultBatch} and {@link ColumnDescriptor}.
 *
 * <p>Allocates a bounded local {@link RootAllocator} per call and releases it
 * immediately after decoding, avoiding allocator leaks.</p>
 */
public final class ArrowBatchDecoder {

    private static final long DECODE_ALLOCATOR_LIMIT = 512L * 1024 * 1024;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MILLISECOND = 1_000_000L;

    /** Reported decimal-digit precision of an Arrow {@code Int(64)} column in JDBC metadata. */
    private static final int INT64_DECIMAL_PRECISION = 19;
    /** Reported decimal-digit precision of an Arrow {@code FloatingPoint(DOUBLE)} column in JDBC metadata. */
    private static final int DOUBLE_DECIMAL_PRECISION = 15;
    /** Reported display precision of an Arrow {@code Timestamp} column (date + time + fractional digits). */
    private static final int TIMESTAMP_DISPLAY_PRECISION = 29;
    private static final String EXTENSION_NAME_KEY = "ARROW:extension:name";
    private static final String EXTENSION_METADATA_KEY = "ARROW:extension:metadata";
    private static final String VECTOR_EXTENSION_NAME = "pangrova.vector";

    static {
        ExtensionTypeRegistry.register(UuidType.INSTANCE);
    }

    private ArrowBatchDecoder() {}

    /**
     * Decodes Arrow IPC bytes into a {@link ResultBatch}.
     *
     * @param arrowIpcBytes Arrow IPC stream format bytes
     * @return decoded result batch; {@link ResultBatch#EMPTY} for null or empty input
     */
    public static ResultBatch decode(byte[] arrowIpcBytes) {
        return decode(arrowIpcBytes, Integer.MAX_VALUE, DECODE_ALLOCATOR_LIMIT);
    }

    /** Decodes under explicit row and allocation bounds, before creating Java row objects. */
    public static ResultBatch decode(byte[] arrowIpcBytes, int maxRows, long maxAllocatorBytes) {
        if (maxRows < 0 || maxAllocatorBytes <= 0) {
            throw new IllegalArgumentException("Decoder limits must be non-negative rows and positive bytes");
        }
        if (arrowIpcBytes == null || arrowIpcBytes.length == 0) {
            return ResultBatch.EMPTY;
        }
        try (BufferAllocator allocator = new RootAllocator(maxAllocatorBytes);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowIpcBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {
            return readAllBatches(reader, maxRows);
        } catch (IOException | UnsupportedOperationException e) {
            throw new TransportException("DECODE_FAILED",
                    "Failed to decode Arrow IPC bytes: " + e.getMessage(), e);
        }
    }

    private static ResultBatch readAllBatches(ArrowStreamReader reader, int maxRows) throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<ColumnDescriptor> columns = buildColumns(root.getSchema());
        if (columns.isEmpty()) {
            return ResultBatch.EMPTY;
        }
        List<Object[]> rows = new ArrayList<>();
        while (reader.loadNextBatch()) {
            if (root.getRowCount() > maxRows - rows.size()) {
                throw new TransportException("RESULT_ROW_LIMIT_EXCEEDED",
                        "Result exceeds the requested row limit", null);
            }
            appendRows(root, columns, rows);
        }
        return new ResultBatch(columns, rows);
    }

    private static void appendRows(VectorSchemaRoot root, List<ColumnDescriptor> columns, List<Object[]> rows) {
        int rowCount = root.getRowCount();
        List<FieldVector> vectors = root.getFieldVectors();
        for (int row = 0; row < rowCount; row++) {
            Object[] values = new Object[vectors.size()];
            for (int col = 0; col < vectors.size(); col++) {
                values[col] = extractValue(vectors.get(col), row, columns.get(col));
            }
            rows.add(values);
        }
    }

    static Object extractValue(FieldVector vector, int row, ColumnDescriptor column) {
        if (column.logicalType().isPresent()) {
            return decodeVectorField(vector, row, column.precision(), column.allowsNull());
        }
        if (vector.isNull(row)) {
            return null;
        }
        if (vector instanceof IntVector iv) return iv.get(row);
        if (vector instanceof BigIntVector bv) return bv.get(row);
        if (vector instanceof Float4Vector fv) return (double) fv.get(row);
        if (vector instanceof Float8Vector dv) return dv.get(row);
        if (vector instanceof VarCharVector sv) return new String(sv.get(row), StandardCharsets.UTF_8);
        if ("UUID".equals(column.typeName())) return normalizeUuidValue(vector.getObject(row));
        return extractComplexValue(vector, row);
    }

    /**
     * Returns whether this field declares the Pangrova VECTOR Arrow extension.
     *
     * @param field Arrow schema field
     * @return true only for a field with the Pangrova VECTOR extension name
     */
    public static boolean isVectorField(Field field) {
        Map<String, String> metadata = field.getMetadata();
        return metadata != null
                && VECTOR_EXTENSION_NAME.equals(metadata.get(EXTENSION_NAME_KEY));
    }

    /**
     * Decodes one annotated VECTOR field, enforcing its exact Arrow schema.
     *
     * @param vector Arrow vector with the Pangrova VECTOR extension
     * @param row zero-based row index
     * @return primitive coordinates for required children, boxed coordinates for nullable children, or null
     */
    public static Object decodeVectorField(FieldVector vector, int row) {
        if (!isVectorField(vector.getField())) {
            throw invalidVector("field does not declare the Pangrova VECTOR extension");
        }
        Field field = vector.getField();
        return decodeVectorField(vector, row, vectorDimension(field), field.isNullable());
    }

    private static Object decodeVectorField(FieldVector vector, int row, int dimension, boolean allowsNull) {
        if (vector.isNull(row)) {
            if (!allowsNull) throw invalidVector("required VECTOR row is null");
            return null;
        }
        return extractVectorValue(vector, row, dimension);
    }

    private static Object extractComplexValue(FieldVector vector, int row) {
        if (vector instanceof BitVector bv) return bv.get(row) != 0;
        return normalizeComplexValue(vector.getObject(row), vector.getField());
    }

    /** Converts Arrow nested values using the declared field type, including MAP entries. */
    public static Object normalizeComplexValue(Object value, Field field) {
        if (uuidDeclaration(field)) return normalizeUuidValue(value);
        if (value == null) return null;
        if (value instanceof Number number) {
            if (field.getType() instanceof ArrowType.Date date) {
                return switch (date.getUnit()) {
                    case DAY -> LocalDate.ofEpochDay(number.longValue());
                    case MILLISECOND -> Instant.ofEpochMilli(number.longValue()).atOffset(ZoneOffset.UTC).toLocalDate();
                };
            }
            if (field.getType() instanceof ArrowType.Time time) {
                long nanos = Math.multiplyExact(number.longValue(), switch (time.getUnit()) {
                    case SECOND -> NANOS_PER_SECOND;
                    case MILLISECOND -> NANOS_PER_MILLISECOND;
                    case MICROSECOND -> 1_000L;
                    case NANOSECOND -> 1L;
                });
                return LocalTime.ofNanoOfDay(nanos);
            }
        }
        if (value instanceof Text || value instanceof CharSequence && !(value instanceof String)) {
            return value.toString();
        }
        if (field.getType() instanceof ArrowType.Map) {
            return normalizeMapValue(value, field);
        }
        if (value instanceof Map<?, ?> struct) {
            Map<String, Object> result = new java.util.LinkedHashMap<>();
            for (Field child : field.getChildren()) {
                result.put(child.getName(), normalizeComplexValue(struct.get(child.getName()), child));
            }
            return result;
        }
        if (value instanceof List<?> list) {
            return normalizeListValue(list, field);
        }
        return value;
    }

    private static Object normalizeUuidValue(Object value) {
        if (value == null || value instanceof UUID) return value;
        if (value instanceof byte[] bytes && bytes.length == UuidType.UUID_BYTE_WIDTH) {
            ByteBuffer binary = ByteBuffer.wrap(bytes);
            return new UUID(binary.getLong(), binary.getLong());
        }
        throw new TransportException("DECODE_FAILED", "UUID requires a canonical 16-byte value",
                new IllegalArgumentException("Unexpected UUID storage value: " + value.getClass().getName()));
    }

    private static Object normalizeListValue(List<?> list, Field field) {
        if (field.getChildren().size() != 1) {
            throw new TransportException("DECODE_FAILED", "Arrow list has no unique element field", null);
        }
        Field element = field.getChildren().get(0);
        return list.stream().map(item -> normalizeComplexValue(item, element)).toList();
    }

    private static Map<Object, Object> normalizeMapValue(Object value, Field field) {
        if (!(value instanceof List<?> entries) || field.getChildren().size() != 1
                || field.getChildren().get(0).getChildren().size() != 2) {
            throw new TransportException("DECODE_FAILED", "Arrow MAP has invalid entries", null);
        }
        Field pair = field.getChildren().get(0);
        Field keyField = pair.getChildren().get(0);
        Field valueField = pair.getChildren().get(1);
        Map<Object, Object> result = new java.util.LinkedHashMap<>();
        for (Object entry : entries) {
            if (!(entry instanceof Map<?, ?> item)) {
                throw new TransportException("DECODE_FAILED", "Arrow MAP entry is not a key-value struct", null);
            }
            Object key = normalizeComplexValue(item.get(keyField.getName()), keyField);
            Object element = normalizeComplexValue(item.get(valueField.getName()), valueField);
            if (key == null || result.containsKey(key)) {
                throw new TransportException("DECODE_FAILED", "Arrow MAP contains null or duplicate key", null);
            }
            result.put(key, element);
        }
        return result;
    }

    private static Object extractVectorValue(FieldVector vector, int row, int dimension) {
        FieldVector data;
        int first;
        int end;
        if (vector instanceof ListVector list) {
            data = list.getDataVector();
            first = list.getElementStartIndex(row);
            end = list.getElementEndIndex(row);
        } else if (vector instanceof FixedSizeListVector list) {
            data = list.getDataVector();
            first = list.getElementStartIndex(row);
            end = list.getElementEndIndex(row);
        } else {
            throw invalidVector("VECTOR extension requires a list storage type");
        }
        if (first < 0 || end > data.getValueCount() || end - first != dimension) {
            throw invalidVector("VECTOR value length differs from declared dimension " + dimension);
        }
        boolean nullable = vector.getField().getChildren().get(0).isNullable();
        if (data instanceof Float4Vector floats) return floatCoordinates(floats, first, dimension, nullable);
        if (data instanceof Float8Vector doubles) return doubleCoordinates(doubles, first, dimension, nullable);
        throw invalidVector("VECTOR coordinates must match declared floating point precision");
    }

    private static Object floatCoordinates(Float4Vector values, int first, int size, boolean nullable) {
        if (nullable) {
            Float[] result = new Float[size];
            for (int index = 0; index < size; index++) result[index] = values.getObject(first + index);
            return result;
        }
        float[] result = new float[size];
        for (int index = 0; index < size; index++) {
            if (values.isNull(first + index)) throw invalidVector("required Float VECTOR coordinate is null");
            result[index] = values.get(first + index);
        }
        return result;
    }

    private static Object doubleCoordinates(Float8Vector values, int first, int size, boolean nullable) {
        if (nullable) {
            Double[] result = new Double[size];
            for (int index = 0; index < size; index++) result[index] = values.getObject(first + index);
            return result;
        }
        double[] result = new double[size];
        for (int index = 0; index < size; index++) {
            if (values.isNull(first + index)) throw invalidVector("required Double VECTOR coordinate is null");
            result[index] = values.get(first + index);
        }
        return result;
    }

    static List<ColumnDescriptor> buildColumns(Schema schema) {
        List<Field> fields = schema.getFields();
        List<ColumnDescriptor> columns = new ArrayList<>(fields.size());
        for (Field field : fields) {
            columns.add(toDescriptor(field));
        }
        return columns;
    }

    private static ColumnDescriptor toDescriptor(Field field) {
        ArrowType type = field.getType();
        if (uuidDeclaration(field)) {
            return new ColumnDescriptor(field.getName(), Types.OTHER, "UUID", 0, 0, Optional.empty());
        }
        int vectorDimension = vectorDimension(field);
        if (vectorDimension > 0) {
            LogicalType declaration = vectorDeclaration(field, vectorDimension);
            return new ColumnDescriptor(
                    field.getName(), Types.ARRAY,
                    declaration.getCanonicalSql(), vectorDimension, 0, Optional.of(declaration));
        }
        return new ColumnDescriptor(
                field.getName(),
                mapSqlType(type),
                type.toString(),
                resolvePrecision(type),
                resolveScale(type), Optional.empty());
    }

    /** A Flight reader may materialize physical storage before extension registration. */
    private static boolean uuidDeclaration(Field field) {
        if (field.getType() instanceof UuidType) return true;
        if (!UuidType.EXTENSION_NAME.equals(field.getMetadata().get(EXTENSION_NAME_KEY))) return false;
        try {
            UuidType.INSTANCE.deserialize(field.getType(),
                    field.getMetadata().getOrDefault(EXTENSION_METADATA_KEY, ""));
        } catch (UnsupportedOperationException | IllegalArgumentException failure) {
            throw new TransportException("DECODE_FAILED", "Invalid canonical UUID declaration", failure);
        }
        return true;
    }

    private static LogicalType vectorDeclaration(Field field, int dimension) {
        ArrowType.FloatingPoint element = (ArrowType.FloatingPoint) field.getChildren().get(0).getType();
        return VectorLogicalType.create(dimension,
                element.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
                        ? VectorLogicalType.FLOAT64 : VectorLogicalType.FLOAT32,
                field.isNullable(), field.getChildren().get(0).isNullable());
    }

    private static int vectorDimension(Field field) {
        if (!isVectorField(field)) {
            return 0;
        }
        Map<String, String> metadata = field.getMetadata();
        if (!(field.getType() instanceof ArrowType.List
                || field.getType() instanceof ArrowType.FixedSizeList)
                || field.getChildren().size() != 1
                || !(field.getChildren().get(0).getType() instanceof ArrowType.FloatingPoint fp)
                || (fp.getPrecision() != org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
                        && fp.getPrecision() != org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)) {
            throw invalidVector("VECTOR extension requires List<Float32|Float64> storage");
        }
        String encodedDimension = metadata.get(EXTENSION_METADATA_KEY);
        if (encodedDimension == null || !encodedDimension.matches("[1-9][0-9]*")) {
            throw invalidVector("VECTOR extension requires a positive decimal dimension");
        }
        try {
            int dimension = Integer.parseInt(encodedDimension);
            if (dimension > VectorLogicalType.MAX_DIMENSION) {
                throw invalidVector("VECTOR extension dimension exceeds canonical storage maximum 65535");
            }
            if (field.getType() instanceof ArrowType.FixedSizeList fixed
                    && fixed.getListSize() != dimension) {
                throw invalidVector("VECTOR extension dimension differs from storage width");
            }
            return dimension;
        } catch (NumberFormatException failure) {
            throw invalidVector("VECTOR extension dimension exceeds integer range");
        }
    }

    private static TransportException invalidVector(String message) {
        return new TransportException("DECODE_FAILED", message, null);
    }

    static int mapSqlType(ArrowType type) {
        if (type instanceof ArrowType.Bool) return Types.BOOLEAN;
        if (type instanceof ArrowType.Decimal) return Types.DECIMAL;
        if (type instanceof ArrowType.Int intType) return mapIntSqlType(intType);
        if (type instanceof ArrowType.FloatingPoint fp) return mapFloatSqlType(fp);
        if (type instanceof ArrowType.FixedSizeList) return Types.ARRAY;
        return mapStringBinaryTemporalSqlType(type);
    }

    private static int mapIntSqlType(ArrowType.Int intType) {
        return intType.getBitWidth() <= 32 ? Types.INTEGER : Types.BIGINT;
    }

    private static int mapFloatSqlType(ArrowType.FloatingPoint fp) {
        return fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
                ? Types.FLOAT : Types.DOUBLE;
    }

    private static int mapStringBinaryTemporalSqlType(ArrowType type) {
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) return Types.VARCHAR;
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) return Types.VARBINARY;
        if (type instanceof ArrowType.Date) return Types.DATE;
        if (type instanceof ArrowType.Time) return Types.TIME;
        if (type instanceof ArrowType.Timestamp) return Types.TIMESTAMP;
        return Types.OTHER;
    }

    private static int resolvePrecision(ArrowType type) {
        if (type instanceof ArrowType.Decimal decimal) return decimal.getPrecision();
        if (type instanceof ArrowType.Int intType) {
            return switch (intType.getBitWidth()) {
                case 8 -> 3;
                case 16 -> 5;
                case 32 -> 10;
                case 64 -> INT64_DECIMAL_PRECISION;
                default -> 0;
            };
        }
        if (type instanceof ArrowType.FloatingPoint fp) {
            return switch (fp.getPrecision()) {
                case SINGLE -> 7;
                case DOUBLE -> DOUBLE_DECIMAL_PRECISION;
                default -> 0;
            };
        }
        if (type instanceof ArrowType.FixedSizeList fsl) return fsl.getListSize();
        if (type instanceof ArrowType.Timestamp) return TIMESTAMP_DISPLAY_PRECISION;
        return 0;
    }

    private static int resolveScale(ArrowType type) {
        if (type instanceof ArrowType.Decimal decimal) return decimal.getScale();
        return 0;
    }
}
