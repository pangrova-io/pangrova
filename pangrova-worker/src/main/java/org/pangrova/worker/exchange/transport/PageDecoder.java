package org.pangrova.worker.exchange.transport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

/**
 * Decodes columnar IPC stream bytes into typed {@link Page} instances.
 *
 * <p>Converts each row in the deserialized {@link VectorSchemaRoot} into a
 * {@link TypedRow} via {@link ColumnValue#of(Object)}, producing a fully
 * typed page suitable for the Java-side data path.</p>
 *
 * <p>Thread safety: stateless utility class, safe for concurrent use
 * provided each call uses its own allocator or a thread-safe child.</p>
 */
public final class PageDecoder {

    /** Mask to read a single unsigned byte. */
    private static final int BYTE_MASK = 0xFF;

    private PageDecoder() {}

    /**
     * Decodes columnar IPC stream bytes into a {@link Page}.
     *
     * @param batchData columnar IPC stream format byte array
     * @param allocator buffer allocator for deserialization (caller-owned)
     * @return decoded page with column names and typed rows
     * @throws IOException if deserialization fails
     */
    public static Page decode(byte[] batchData, BufferAllocator allocator) throws IOException {
        if (batchData == null || batchData.length == 0) {
            return new Page(List.of(), List.of());
        }

        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(batchData), allocator)) {

            if (!reader.loadNextBatch()) {
                return new Page(List.of(), List.of());
            }

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> columns = extractColumnNames(root);
            List<TypedRow> rows = extractRows(root, columns);
            return new Page(columns, rows);
        }
    }

    /**
     * Zero-copy variant: decodes from a direct {@link ByteBuffer} without
     * copying to a heap byte array first (L4-3A optimization).
     *
     * <p>The ByteBuffer is typically a JNI DirectByteBuffer wrapping native
     * memory. Reading directly from it avoids one full memcpy of the IPC payload.</p>
     *
     * @param buffer    direct ByteBuffer containing columnar IPC stream bytes
     * @param allocator buffer allocator for deserialization (caller-owned)
     * @return decoded page with column names and typed rows
     * @throws IOException if deserialization fails
     */
    public static Page decodeDirect(ByteBuffer buffer, BufferAllocator allocator) throws IOException {
        if (buffer == null || !buffer.hasRemaining()) {
            return new Page(List.of(), List.of());
        }

        // Wrap ByteBuffer as an InputStream via Channels for ArrowStreamReader.
        // For direct buffers this avoids heap copy; the Arrow reader pulls bytes
        // directly from the native-backed buffer.
        java.io.InputStream is = new ByteBufferInputStream(buffer);
        try (ArrowStreamReader reader = new ArrowStreamReader(is, allocator)) {
            if (!reader.loadNextBatch()) {
                return new Page(List.of(), List.of());
            }

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> columns = extractColumnNames(root);
            List<TypedRow> rows = extractRows(root, columns);
            return new Page(columns, rows);
        }
    }

    /**
     * Lightweight InputStream backed by a ByteBuffer.
     * Avoids the overhead of Channels.newInputStream which synchronizes on every read.
     */
    private static final class ByteBufferInputStream extends java.io.InputStream {
        private final ByteBuffer buf;

        ByteBufferInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int read() {
            if (!buf.hasRemaining()) return -1;
            return buf.get() & BYTE_MASK;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buf.hasRemaining()) return -1;
            int toRead = Math.min(len, buf.remaining());
            buf.get(b, off, toRead);
            return toRead;
        }

        @Override
        public int available() {
            return buf.remaining();
        }
    }

    private static List<String> extractColumnNames(VectorSchemaRoot root) {
        List<Field> fields = root.getSchema().getFields();
        List<String> names = new ArrayList<>(fields.size());
        for (Field field : fields) {
            names.add(field.getName());
        }
        return names;
    }

    private static List<TypedRow> extractRows(VectorSchemaRoot root, List<String> columns) {
        int rowCount = root.getRowCount();
        List<FieldVector> vectors = root.getFieldVectors();
        List<TypedRow> rows = new ArrayList<>(rowCount);

        for (int row = 0; row < rowCount; row++) {
            TypedRow.Builder builder = TypedRow.builder();
            for (int col = 0; col < vectors.size(); col++) {
                FieldVector vector = vectors.get(col);
                Object value;
                if (vector.isNull(row)) {
                    value = null;
                } else {
                    value = toJavaValue(vector.getObject(row), vector.getField());
                }
                builder.putRaw(columns.get(col), value);
            }
            rows.add(builder.build());
        }
        return rows;
    }

    private static Object toJavaValue(Object value, Field field) {
        if (value == null) return null;
        if (value instanceof Text || value instanceof CharSequence && !(value instanceof String)) {
            return value.toString();
        }
        if (field.getType() instanceof ArrowType.Map) {
            return toJavaMap(value, field);
        }
        if (value instanceof java.util.Map<?, ?> struct) {
            java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
            for (Field child : field.getChildren()) {
                result.put(child.getName(), toJavaValue(struct.get(child.getName()), child));
            }
            return result;
        }
        if (value instanceof List<?> list) {
            if (field.getChildren().size() != 1) {
                throw new IllegalArgumentException("Arrow list has no unique element field");
            }
            Field element = field.getChildren().get(0);
            return list.stream().map(item -> toJavaValue(item, element)).toList();
        }
        return value;
    }

    private static java.util.Map<Object, Object> toJavaMap(Object value, Field field) {
        if (!(value instanceof List<?> entries) || field.getChildren().size() != 1
                || field.getChildren().get(0).getChildren().size() != 2) {
            throw new IllegalArgumentException("Arrow MAP has invalid entries");
        }
        Field pair = field.getChildren().get(0);
        Field keyField = pair.getChildren().get(0);
        Field valueField = pair.getChildren().get(1);
        java.util.Map<Object, Object> result = new java.util.LinkedHashMap<>();
        for (Object entry : entries) {
            if (!(entry instanceof java.util.Map<?, ?> item)) {
                throw new IllegalArgumentException("Arrow MAP entry is not a key-value struct");
            }
            Object key = toJavaValue(item.get(keyField.getName()), keyField);
            Object element = toJavaValue(item.get(valueField.getName()), valueField);
            if (key == null || result.containsKey(key)) {
                throw new IllegalArgumentException("Arrow MAP contains null or duplicate key");
            }
            result.put(key, element);
        }
        return result;
    }

}
