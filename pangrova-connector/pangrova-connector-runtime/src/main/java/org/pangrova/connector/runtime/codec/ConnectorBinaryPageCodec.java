package org.pangrova.connector.runtime.codec;

import org.pangrova.batch.api.page.Page;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.batch.api.page.BatchFormat;

import org.pangrova.common.api.annotation.Experimental;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;

/**
 * Bidirectional converter between row-based {@link Page} and custom binary
 * {@link NativePage} representations.
 *
 * <p>Uses a lightweight custom binary format for serialization without an Arrow SDK dependency. The format is:</p>
 * <pre>
 *   [version:byte][rowCount:int][columnCount:int]
 *   [columnName1Len:int][columnName1:bytes]...
 *   For each row, for each column:
 *     [typeTag:byte][valueBytes...]
 * </pre>
 *
 * <p>This converter enables connectors that only support row-based Pages to
 * interoperate with engine paths that prefer NativePage, and vice versa.</p>
 *
 * <p>Utility class — not instantiable.</p>
 */
@Experimental
public final class ConnectorBinaryPageCodec {

    private static final byte TAG_NULL = 0;
    private static final byte TAG_STRING = 1;
    private static final byte TAG_INT = 2;
    private static final byte TAG_LONG = 3;
    private static final byte TAG_DOUBLE = 4;
    private static final byte TAG_BOOLEAN = 5;

    /** Format version byte written at the start of serialized data. */
    private static final byte FORMAT_VERSION = 1;
    private static final int INTEGER_BYTES = Integer.BYTES;
    private static final int INTEGER_HIGHEST_BIT_SHIFT = Integer.SIZE - Byte.SIZE;
    private static final int LONG_HIGHEST_BIT_SHIFT = Long.SIZE - Byte.SIZE;

    private ConnectorBinaryPageCodec() {
        throw new AssertionError("Utility class");
    }

    /**
     * Converts an {@link NativePage} back to a row-based {@link Page}.
     *
     * @param nativePage the NativePage to convert
     * @return a row-based Page
     */
    public static Page toPage(NativePage nativePage) {
        Objects.requireNonNull(nativePage, "nativePage must not be null");
        if (nativePage.format() != BatchFormat.CUSTOM_BINARY) {
            throw new IllegalArgumentException("Expected CUSTOM_BINARY, got " + nativePage.format());
        }
        Page page = deserialize(nativePage.serializedBatch());
        if ((nativePage.getRowCount() >= 0 && nativePage.getRowCount() != page.rows().size())
                || (nativePage.getColumnCount() >= 0 && nativePage.getColumnCount() != page.columns().size())) {
            throw new IllegalArgumentException("NativePage dimensions do not match its payload");
        }
        return page;
    }

    /**
     * Converts a row-based {@link Page} to an {@link NativePage}.
     *
     * @param page the Page to convert
     * @return a NativePage with serialized batch data in CUSTOM_BINARY format
     */
    public static NativePage fromPage(Page page) {
        Objects.requireNonNull(page, "page must not be null");
        byte[] serialized = serialize(page);
        return new NativePage(serialized, page.getPositionCount(),
                page.columns().size(), BatchFormat.CUSTOM_BINARY);
    }

    private static byte[] serialize(Page page) {
        List<String> columns = page.columns();
        List<TypedRow> rows = page.rows();
        validateColumns(columns);
        if (columns.isEmpty() && !rows.isEmpty()) {
            throw new IllegalArgumentException("CUSTOM_BINARY requires columns for nonempty rows");
        }
        ByteArrayWriter writer = new ByteArrayWriter();
        writer.writeByte(FORMAT_VERSION);
        writer.writeInt(rows.size());
        writer.writeInt(columns.size());
        for (String col : columns) {
            writer.writeString(col);
        }
        for (TypedRow row : rows) {
            serializeRow(writer, row, columns);
        }
        return writer.toByteArray();
    }

    private static void serializeRow(
            ByteArrayWriter writer, TypedRow row, List<String> columns) {
        for (String col : columns) {
            serializeValue(writer, row.get(col));
        }
    }

    private static void serializeValue(ByteArrayWriter writer, ColumnValue cv) {
        Object raw = ColumnValue.toRaw(cv);
        if (raw == null) {
            writer.writeByte(TAG_NULL);
            return;
        }
        writeTypedValue(writer, raw);
    }

    private static void writeTypedValue(ByteArrayWriter writer, Object raw) {
        if (raw instanceof String s) {
            writer.writeByte(TAG_STRING);
            writer.writeString(s);
        } else if (raw instanceof Integer i) {
            writer.writeByte(TAG_INT);
            writer.writeInt(i);
        } else if (raw instanceof Long l) {
            writer.writeByte(TAG_LONG);
            writer.writeLong(l);
        } else {
            writeRemainingTypes(writer, raw);
        }
    }

    private static void writeRemainingTypes(ByteArrayWriter writer, Object raw) {
        if (raw instanceof Double d) {
            writer.writeByte(TAG_DOUBLE);
            writer.writeDouble(d);
        } else if (raw instanceof Boolean b) {
            writer.writeByte(TAG_BOOLEAN);
            writer.writeByte(b ? (byte) 1 : (byte) 0);
        } else {
            throw new IllegalArgumentException("CUSTOM_BINARY does not support " + raw.getClass().getName());
        }
    }

    private static Page deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        try {
            byte version = buf.get();
            if (version != FORMAT_VERSION) {
                throw new IllegalArgumentException("Unsupported CUSTOM_BINARY version: " + version);
            }
            int rowCount = buf.getInt();
            int colCount = buf.getInt();
            if (rowCount < 0 || colCount < 0 || colCount > buf.remaining() / Integer.BYTES) {
                throw new IllegalArgumentException("Invalid CUSTOM_BINARY dimensions");
            }
            List<String> columns = readColumnNames(buf, colCount);
            validateColumns(columns);
            // Every cell requires at least one tag byte. Check before allocating rows.
            if ((long) rowCount * colCount > buf.remaining() || (colCount == 0 && rowCount != 0)) {
                throw new IllegalArgumentException("CUSTOM_BINARY dimensions exceed payload");
            }
            List<TypedRow> rows = readRows(buf, rowCount, columns);
            if (buf.hasRemaining()) {
                throw new IllegalArgumentException("Trailing CUSTOM_BINARY data");
            }
            return new Page(columns, rows);
        } catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("Truncated CUSTOM_BINARY payload", e);
        }
    }

    private static void validateColumns(List<String> columns) {
        if (columns.stream().anyMatch(Objects::isNull)
                || new java.util.HashSet<>(columns).size() != columns.size()) {
            throw new IllegalArgumentException("CUSTOM_BINARY requires distinct non-null column names");
        }
    }

    private static List<String> readColumnNames(ByteBuffer buf, int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add(readString(buf));
        }
        return names;
    }

    private static List<TypedRow> readRows(
            ByteBuffer buf, int rowCount, List<String> columns) {
        List<TypedRow> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            rows.add(readRow(buf, columns));
        }
        return rows;
    }

    private static TypedRow readRow(ByteBuffer buf, List<String> columns) {
        TypedRow.Builder builder = TypedRow.builder();
        for (String col : columns) {
            builder.put(col, readColumnValue(buf));
        }
        return builder.build();
    }

    private static ColumnValue readColumnValue(ByteBuffer buf) {
        byte tag = buf.get();
        return switch (tag) {
            case TAG_NULL -> new ColumnValue.NullValue();
            case TAG_STRING -> new ColumnValue.StringValue(readString(buf));
            case TAG_INT -> new ColumnValue.IntValue(buf.getInt());
            case TAG_LONG -> new ColumnValue.LongValue(buf.getLong());
            case TAG_DOUBLE -> new ColumnValue.DoubleValue(buf.getDouble());
            case TAG_BOOLEAN -> {
                byte value = buf.get();
                if (value != 0 && value != 1) {
                    throw new IllegalArgumentException("Invalid CUSTOM_BINARY boolean: " + value);
                }
                yield new ColumnValue.BooleanValue(value == 1);
            }
            default -> throw new IllegalArgumentException(
                    "Unknown type tag: " + tag);
        };
    }

    private static String readString(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0 || len > buf.remaining()) {
            throw new IllegalArgumentException("Invalid CUSTOM_BINARY string length: " + len);
        }
        ByteBuffer encoded = buf.slice();
        encoded.limit(len);
        buf.position(buf.position() + len);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(encoded).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid CUSTOM_BINARY UTF-8", e);
        }
    }

    /**
     * Simple growable byte array writer for serialization.
     */
    private static final class ByteArrayWriter {
        private byte[] buffer = new byte[256];
        private int position;

        void writeByte(byte b) {
            ensureCapacity(1);
            buffer[position++] = b;
        }

        void writeInt(int value) {
            ensureCapacity(INTEGER_BYTES);
            buffer[position++] = (byte) (value >> INTEGER_HIGHEST_BIT_SHIFT);
            buffer[position++] = (byte) (value >> 16);
            buffer[position++] = (byte) (value >> 8);
            buffer[position++] = (byte) value;
        }

        void writeLong(long value) {
            ensureCapacity(8);
            for (int i = LONG_HIGHEST_BIT_SHIFT; i >= 0; i -= Byte.SIZE) {
                buffer[position++] = (byte) (value >> i);
            }
        }

        void writeDouble(double value) {
            writeLong(Double.doubleToRawLongBits(value));
        }

        void writeString(String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            writeInt(bytes.length);
            ensureCapacity(bytes.length);
            System.arraycopy(bytes, 0, buffer, position, bytes.length);
            position += bytes.length;
        }

        byte[] toByteArray() {
            byte[] result = new byte[position];
            System.arraycopy(buffer, 0, result, 0, position);
            return result;
        }

        private void ensureCapacity(int needed) {
            int required = Math.addExact(position, needed);
            if (required > buffer.length) {
                int newLen = (int) Math.min(Integer.MAX_VALUE, Math.max((long) buffer.length * 2, required));
                byte[] newBuf = new byte[newLen];
                System.arraycopy(buffer, 0, newBuf, 0, position);
                buffer = newBuf;
            }
        }
    }
}
