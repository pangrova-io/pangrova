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
package org.pangrova.transport;

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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    /** Reported decimal-digit precision of an Arrow {@code Int(64)} column in JDBC metadata. */
    private static final int INT64_DECIMAL_PRECISION = 19;
    /** Reported decimal-digit precision of an Arrow {@code FloatingPoint(DOUBLE)} column in JDBC metadata. */
    private static final int DOUBLE_DECIMAL_PRECISION = 15;
    /** Reported display precision of an Arrow {@code Timestamp} column (date + time + fractional digits). */
    private static final int TIMESTAMP_DISPLAY_PRECISION = 29;
    private static final String EXTENSION_NAME_KEY = "ARROW:extension:name";
    private static final String EXTENSION_METADATA_KEY = "ARROW:extension:metadata";
    private static final String VECTOR_EXTENSION_NAME = "pangrova.vector";

    private ArrowBatchDecoder() {}

    /**
     * Decodes Arrow IPC bytes into a {@link ResultBatch}.
     *
     * @param arrowIpcBytes Arrow IPC stream format bytes
     * @return decoded result batch; {@link ResultBatch#EMPTY} for null or empty input
     */
    public static ResultBatch decode(byte[] arrowIpcBytes) {
        if (arrowIpcBytes == null || arrowIpcBytes.length == 0) {
            return ResultBatch.EMPTY;
        }
        try (BufferAllocator allocator = new RootAllocator(DECODE_ALLOCATOR_LIMIT);
             ByteArrayInputStream bais = new ByteArrayInputStream(arrowIpcBytes);
             ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {
            return readAllBatches(reader);
        } catch (IOException e) {
            throw new TransportException("DECODE_FAILED",
                    "Failed to decode Arrow IPC bytes: " + e.getMessage(), e);
        }
    }

    private static ResultBatch readAllBatches(ArrowStreamReader reader) throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<ColumnDescriptor> columns = buildColumns(root.getSchema());
        if (columns.isEmpty()) {
            return ResultBatch.EMPTY;
        }
        List<Object[]> rows = new ArrayList<>();
        while (reader.loadNextBatch()) {
            appendRows(root, rows);
        }
        return new ResultBatch(columns, rows);
    }

    private static void appendRows(VectorSchemaRoot root, List<Object[]> rows) {
        int rowCount = root.getRowCount();
        List<FieldVector> vectors = root.getFieldVectors();
        for (int row = 0; row < rowCount; row++) {
            Object[] values = new Object[vectors.size()];
            for (int col = 0; col < vectors.size(); col++) {
                values[col] = extractValue(vectors.get(col), row);
            }
            rows.add(values);
        }
    }

    private static Object extractValue(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        if (isVectorField(vector.getField())) {
            return decodeVectorField(vector, row);
        }
        if (vector instanceof IntVector iv) return iv.get(row);
        if (vector instanceof BigIntVector bv) return bv.get(row);
        if (vector instanceof Float4Vector fv) return (double) fv.get(row);
        if (vector instanceof Float8Vector dv) return dv.get(row);
        if (vector instanceof VarCharVector sv) return new String(sv.get(row), StandardCharsets.UTF_8);
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
     * @return FP32 coordinates, or {@code null} for an Arrow null
     */
    public static float[] decodeVectorField(FieldVector vector, int row) {
        if (!isVectorField(vector.getField())) {
            throw invalidVector("field does not declare the Pangrova VECTOR extension");
        }
        int dimension = vectorDimension(vector.getField());
        return vector.isNull(row) ? null : extractVectorValue(vector, row, dimension);
    }

    private static Object extractComplexValue(FieldVector vector, int row) {
        if (vector instanceof BitVector bv) return bv.get(row) != 0;
        return vector.getObject(row);
    }

    private static float[] extractVectorValue(
            FieldVector vector, int row, int dimension) {
        List<?> values;
        if (vector instanceof ListVector || vector instanceof FixedSizeListVector) {
            values = (List<?>) vector.getObject(row);
        } else {
            throw invalidVector("VECTOR extension requires a list storage type");
        }
        if (values == null || values.size() != dimension) {
            throw invalidVector("VECTOR value length differs from declared dimension " + dimension);
        }
        float[] result = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            if (!(values.get(i) instanceof Float coordinate) || !Float.isFinite(coordinate)) {
                throw invalidVector("VECTOR coordinates must be finite Float32 values");
            }
            result[i] = coordinate;
        }
        return result;
    }

    private static List<ColumnDescriptor> buildColumns(Schema schema) {
        List<Field> fields = schema.getFields();
        List<ColumnDescriptor> columns = new ArrayList<>(fields.size());
        for (Field field : fields) {
            columns.add(toDescriptor(field));
        }
        return columns;
    }

    private static ColumnDescriptor toDescriptor(Field field) {
        ArrowType type = field.getType();
        int vectorDimension = vectorDimension(field);
        if (vectorDimension > 0) {
            return new ColumnDescriptor(
                    field.getName(), Types.ARRAY,
                    "VECTOR(" + vectorDimension + ")", vectorDimension, 0);
        }
        return new ColumnDescriptor(
                field.getName(),
                mapSqlType(type),
                type.toString(),
                resolvePrecision(type),
                resolveScale(type));
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
                || fp.getPrecision()
                        != org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
            throw invalidVector("VECTOR extension requires List<Float32> storage");
        }
        String encodedDimension = metadata.get(EXTENSION_METADATA_KEY);
        if (encodedDimension == null || !encodedDimension.matches("[1-9][0-9]*")) {
            throw invalidVector("VECTOR extension requires a positive decimal dimension");
        }
        try {
            int dimension = Integer.parseInt(encodedDimension);
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
