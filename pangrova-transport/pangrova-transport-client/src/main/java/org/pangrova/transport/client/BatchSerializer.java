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

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.BatchFormat;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.batch.api.page.Page;
import org.pangrova.types.PangrovaType;
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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializes row-based {@link Page} data into Arrow IPC streaming format and
 * deserializes Arrow IPC bytes back into row-based Pages.
 *
 * <p>This class lives in {@code pangrova-transport} where Arrow imports are permitted.
 * Connector modules must not depend on this class directly — they should use
 * {@code ConnectorBinaryPageCodec} instead, which
 * produces {@link BatchFormat#CUSTOM_BINARY} without requiring Arrow on the connector
 * classpath.</p>
 *
 * <p>All Arrow allocator usage follows try-with-resources to prevent off-heap
 * memory leaks.</p>
 *
 * <p>Utility class — not instantiable.</p>
 */
public final class BatchSerializer {

    /**
     * Process-wide root allocator shared by every (de)serialize call. Per-call
     * allocations are isolated through cheap child allocators created from
     * this root, then released via try-with-resources. This avoids the
     * substantial cost of constructing a new {@link RootAllocator} per call
     * (which scans the JVM, registers Netty buffer trackers, and reserves
     * direct-memory accounting structures) while preserving leak isolation.
     */
    private static final BufferAllocator ROOT_ALLOCATOR =
            new RootAllocator(Long.MAX_VALUE);

    private BatchSerializer() {
        throw new AssertionError("Utility class");
    }

    /**
     * Serializes a Page of typed rows into an Arrow IPC stream batch,
     * producing a NativePage with {@link BatchFormat#ARROW_IPC} format.
     *
     * @param page    the row-based page to serialize
     * @param columns column metadata for schema construction
     * @return a NativePage containing Arrow IPC serialized data
     * @throws IOException if serialization fails
     */
    public static NativePage serialize(Page page, List<ColumnMeta> columns) throws IOException {
        try (BufferAllocator allocator = ROOT_ALLOCATOR.newChildAllocator(
                "BatchSerializer.serialize", 0, Long.MAX_VALUE)) {
            return serializeWithAllocator(page, columns, allocator);
        }
    }

    /**
     * Deserializes Arrow IPC bytes from a NativePage back into a row-based Page.
     *
     * @param nativePage the NativePage containing Arrow IPC data
     * @return a row-based Page with the deserialized data
     * @throws IOException if deserialization fails
     */
    public static Page deserialize(NativePage nativePage) throws IOException {
        byte[] data = nativePage.serializedBatch();
        try (BufferAllocator allocator = ROOT_ALLOCATOR.newChildAllocator(
                     "BatchSerializer.deserialize", 0, Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(
                     new ByteArrayInputStream(data), allocator)) {
            return readPage(reader);
        }
    }

    private static NativePage serializeWithAllocator(
            Page page, List<ColumnMeta> columns, BufferAllocator allocator) throws IOException {
        Schema schema = buildSchema(columns);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            populateVectors(root, page, columns);
            return writeToBytes(root, page.getPositionCount(), columns.size());
        }
    }

    private static Schema buildSchema(List<ColumnMeta> columns) {
        List<Field> fields = new ArrayList<>(columns.size());
        for (ColumnMeta col : columns) {
            fields.add(buildField(col));
        }
        return new Schema(fields);
    }

    private static Field buildField(ColumnMeta col) {
        if (col.type() == PangrovaType.VECTOR && col.dimension() > 0) {
            Field child = new Field("value",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    null);
            return new Field(col.name(),
                    FieldType.nullable(new ArrowType.FixedSizeList(col.dimension())),
                    List.of(child));
        }
        return new Field(col.name(), FieldType.nullable(toArrowType(col.type())), null);
    }

    private static void populateVectors(
            VectorSchemaRoot root, Page page, List<ColumnMeta> columns) {
        int rowCount = page.getPositionCount();
        root.setRowCount(rowCount);
        List<TypedRow> rows = page.rows();

        for (int colIdx = 0; colIdx < columns.size(); colIdx++) {
            ColumnMeta col = columns.get(colIdx);
            FieldVector vector = root.getVector(colIdx);
            vector.allocateNew();
            populateColumn(vector, col, rows, rowCount);
            vector.setValueCount(rowCount);
        }
    }

    private static void populateColumn(
            FieldVector vector, ColumnMeta col, List<TypedRow> rows, int rowCount) {
        for (int row = 0; row < rowCount; row++) {
            Object raw = ColumnValue.toRaw(rows.get(row).get(col.name()));
            if (raw == null) {
                setNull(vector, row);
            } else {
                setValue(vector, row, raw, col.type());
            }
        }
    }

    private static void setNull(FieldVector vector, int index) {
        if (vector instanceof IntVector iv) {
            iv.setNull(index);
        } else if (vector instanceof BigIntVector bv) {
            bv.setNull(index);
        } else if (vector instanceof Float8Vector fv) {
            fv.setNull(index);
        } else if (vector instanceof VarCharVector vc) {
            vc.setNull(index);
        } else if (vector instanceof BitVector bv) {
            bv.setNull(index);
        } else if (vector instanceof FixedSizeListVector lv) {
            lv.setNull(index);
        }
    }

    private static void setValue(FieldVector vector, int index, Object raw, PangrovaType type) {
        switch (type) {
            case INTEGER -> ((IntVector) vector).setSafe(index, ((Number) raw).intValue());
            case BIGINT -> ((BigIntVector) vector).setSafe(index, ((Number) raw).longValue());
            case DOUBLE, FLOAT -> ((Float8Vector) vector).setSafe(index, ((Number) raw).doubleValue());
            case VARCHAR -> ((VarCharVector) vector).setSafe(index, raw.toString().getBytes());
            case BOOLEAN -> ((BitVector) vector).setSafe(index, Boolean.TRUE.equals(raw) ? 1 : 0);
            case VECTOR -> writeVector((FixedSizeListVector) vector, index, (float[]) raw);
            default -> {
                if (vector instanceof VarCharVector vc) {
                    vc.setSafe(index, raw.toString().getBytes());
                }
            }
        }
    }

    private static void writeVector(FixedSizeListVector listVector, int index, float[] values) {
        Float4Vector child = (Float4Vector) listVector.getDataVector();
        int dim = listVector.getListSize();
        int offset = index * dim;
        for (int i = 0; i < dim && i < values.length; i++) {
            child.setSafe(offset + i, values[i]);
        }
        listVector.setNotNull(index);
    }

    private static NativePage writeToBytes(
            VectorSchemaRoot root, int rowCount, int columnCount) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(
                root, null, Channels.newChannel(baos))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return new NativePage(baos.toByteArray(), rowCount, columnCount, BatchFormat.ARROW_IPC);
    }

    private static Page readPage(ArrowStreamReader reader) throws IOException {
        if (!reader.loadNextBatch()) {
            return new Page(List.of(), List.of());
        }
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        List<String> columnNames = schema.getFields().stream()
                .map(Field::getName)
                .toList();
        List<TypedRow> rows = extractRows(root, columnNames);
        return new Page(columnNames, rows);
    }

    private static List<TypedRow> extractRows(VectorSchemaRoot root, List<String> columnNames) {
        int rowCount = root.getRowCount();
        List<TypedRow> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            rows.add(extractRow(root, columnNames, r));
        }
        return rows;
    }

    private static TypedRow extractRow(VectorSchemaRoot root, List<String> columnNames, int rowIdx) {
        TypedRow.Builder builder = TypedRow.builder();
        for (int c = 0; c < columnNames.size(); c++) {
            FieldVector vector = root.getVector(c);
            Object value = readValue(vector, rowIdx);
            builder.putRaw(columnNames.get(c), value);
        }
        return builder.build();
    }

    private static Object readValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }
        if (vector instanceof IntVector iv) {
            return iv.get(index);
        } else if (vector instanceof BigIntVector bv) {
            return bv.get(index);
        } else if (vector instanceof Float8Vector fv) {
            return fv.get(index);
        } else if (vector instanceof VarCharVector vc) {
            Text text = vc.getObject(index);
            return text != null ? text.toString() : null;
        }
        return readComplexValue(vector, index);
    }

    private static Object readComplexValue(FieldVector vector, int index) {
        if (vector instanceof BitVector bv) {
            return bv.get(index) == 1;
        }
        if (vector instanceof FixedSizeListVector lv) {
            return readVectorValue(lv, index);
        }
        Object value = vector.getObject(index);
        return value != null ? value.toString() : null;
    }

    private static float[] readVectorValue(FixedSizeListVector lv, int index) {
        int dim = lv.getListSize();
        float[] result = new float[dim];
        Float4Vector child = (Float4Vector) lv.getDataVector();
        int offset = index * dim;
        for (int i = 0; i < dim; i++) {
            result[i] = child.get(offset + i);
        }
        return result;
    }

    private static ArrowType toArrowType(PangrovaType type) {
        return switch (type) {
            case INTEGER -> new ArrowType.Int(32, true);
            case BIGINT -> new ArrowType.Int(64, true);
            case DOUBLE, FLOAT -> new ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case VARCHAR -> ArrowType.Utf8.INSTANCE;
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            default -> ArrowType.Utf8.INSTANCE;
        };
    }

    /**
     * Column metadata for schema construction during serialization.
     *
     * @param name      the column name
     * @param type      the Pangrova type of the column
     * @param dimension vector dimension (only meaningful when type is VECTOR; 0 otherwise)
     */
    public record ColumnMeta(String name, PangrovaType type, int dimension) {

        public ColumnMeta(String name, PangrovaType type) {
            this(name, type, 0);
        }
    }
}
