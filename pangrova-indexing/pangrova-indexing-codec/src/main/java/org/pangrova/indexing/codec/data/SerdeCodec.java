package org.pangrova.indexing.codec.data;

import org.pangrova.indexing.api.data.CompositeIndexKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared codec utilities for type-specific serializers.
 *
 * <p>Comparable values are encoded with a type tag byte:
 * 0=null, 1=String, 2=Integer, 3=Long, 4=Double, 5=Float, 6=Short, 7=Byte,
 * 8=BigDecimal, 9=BigInteger, 10=UUID, 11=complete composite index key.
 */
public final class SerdeCodec {

    /** Current serialization format version. Increment on breaking changes. */
    public static final byte SERDE_VERSION = 1;

    private static final byte TAG_NULL = 0;
    private static final byte TAG_STRING = 1;
    private static final byte TAG_INT = 2;
    private static final byte TAG_LONG = 3;
    private static final byte TAG_DOUBLE = 4;
    private static final byte TAG_FLOAT = 5;
    private static final byte TAG_SHORT = 6;
    private static final byte TAG_BYTE = 7;
    private static final byte TAG_BIG_DECIMAL = 8;
    private static final byte TAG_BIG_INTEGER = 9;
    private static final byte TAG_UUID = 10;
    private static final byte TAG_COMPOSITE = 11;

    private SerdeCodec() {}

    /**
     * Writes the serde version header. Must be the first byte in any serialized payload.
     */
    public static void writeVersionHeader(DataOutputStream out) throws IOException {
        out.writeByte(SERDE_VERSION);
    }

    /**
     * Reads and validates the serde version header.
     *
     * @throws IOException if the version is unsupported
     */
    public static byte readVersionHeader(DataInputStream in) throws IOException {
        byte version = in.readByte();
        if (version < 1 || version > SERDE_VERSION) {
            throw new IOException("Unsupported serde version: " + version
                    + " (current: " + SERDE_VERSION + ")");
        }
        return version;
    }

    /**
     * Writes a nullable comparable value using its type tag.
     *
     * @param out   target stream
     * @param value value to write, or {@code null}
     * @throws IOException if the underlying stream fails
     */
    public static void writeComparable(DataOutputStream out, Comparable<?> value) throws IOException {
        if (value == null) {
            out.writeByte(TAG_NULL);
            return;
        }
        writeNonNullComparable(out, value);
    }

    private static void writeNonNullComparable(DataOutputStream out, Comparable<?> value) throws IOException {
        if (value instanceof Number n) {
            writeNumber(out, n);
        } else if (value instanceof String s) {
            out.writeByte(TAG_STRING);
            out.writeUTF(s);
        } else if (value instanceof UUID uuid) {
            writeUuid(out, uuid);
        } else if (value instanceof CompositeIndexKey key) {
            out.writeByte(TAG_COMPOSITE);
            out.writeInt(key.components().size());
            for (Comparable<?> component : key.components()) writeComparable(out, component);
        } else {
            throw new IOException("Unsupported Comparable type for serialization: "
                    + value.getClass().getName()
                    + " — add a native type tag to SerdeCodec");
        }
    }

    private static void writeUuid(DataOutputStream out, UUID uuid) throws IOException {
        out.writeByte(TAG_UUID);
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    private static void writeNumber(DataOutputStream out, Number n) throws IOException {
        if (n instanceof Integer i) {
            out.writeByte(TAG_INT);
            out.writeInt(i);
        } else if (n instanceof Long l) {
            out.writeByte(TAG_LONG);
            out.writeLong(l);
        } else if (n instanceof Double d) {
            out.writeByte(TAG_DOUBLE);
            out.writeDouble(d);
        } else {
            writeOtherNumber(out, n);
        }
    }

    private static void writeOtherNumber(DataOutputStream out, Number n) throws IOException {
        if (n instanceof Float f) {
            out.writeByte(TAG_FLOAT);
            out.writeFloat(f);
        } else if (n instanceof Short s) {
            out.writeByte(TAG_SHORT);
            out.writeShort(s);
        } else if (n instanceof Byte b) {
            out.writeByte(TAG_BYTE);
            out.writeByte(b);
        } else {
            writeBigNumber(out, n);
        }
    }

    private static void writeBigNumber(DataOutputStream out, Number n) throws IOException {
        if (n instanceof BigDecimal bd) {
            writeBigDecimal(out, bd);
        } else if (n instanceof BigInteger bi) {
            out.writeByte(TAG_BIG_INTEGER);
            writeByteArray(out, bi.toByteArray());
        } else {
            throw new IOException("Unsupported Number type: " + n.getClass().getName());
        }
    }

    private static void writeBigDecimal(DataOutputStream out, BigDecimal bd) throws IOException {
        out.writeByte(TAG_BIG_DECIMAL);
        byte[] unscaled = bd.unscaledValue().toByteArray();
        writeByteArray(out, unscaled);
        out.writeInt(bd.scale());
    }

    /**
     * Reads a nullable comparable value written by {@link #writeComparable}.
     *
     * @param in source stream
     * @return the decoded value, or {@code null}
     * @throws IOException if the tag is unknown or the stream fails
     */
    public static Comparable<?> readComparable(DataInputStream in) throws IOException {
        return readComparable(in, true);
    }

    private static Comparable<?> readComparable(DataInputStream in, boolean compositeAllowed) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case TAG_NULL -> null;
            case TAG_STRING -> in.readUTF();
            case TAG_INT -> in.readInt();
            case TAG_LONG -> in.readLong();
            case TAG_DOUBLE -> in.readDouble();
            case TAG_FLOAT -> in.readFloat();
            case TAG_SHORT -> in.readShort();
            case TAG_BYTE -> in.readByte();
            case TAG_BIG_DECIMAL -> {
                byte[] unscaled = readByteArray(in);
                int scale = in.readInt();
                yield new BigDecimal(new BigInteger(unscaled), scale);
            }
            case TAG_BIG_INTEGER -> new BigInteger(readByteArray(in));
            case TAG_UUID -> new UUID(in.readLong(), in.readLong());
            case TAG_COMPOSITE -> {
                if (!compositeAllowed) throw new IOException("Nested composite index keys are not supported");
                yield readComposite(in);
            }
            default -> throw new IOException("Unknown comparable type tag: " + tag);
        };
    }

    private static CompositeIndexKey readComposite(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 2 || count > CompositeIndexKey.MAX_COMPONENTS) {
            throw new IOException("Invalid composite index key arity: " + count);
        }
        var components = new ArrayList<Comparable<?>>(count);
        for (int i = 0; i < count; i++) {
            Comparable<?> component = readComparable(in, false);
            if (component == null) throw new IOException("Null composite index key component");
            components.add(component);
        }
        return new CompositeIndexKey(components);
    }

    public static void writeByteArray(DataOutputStream out, byte[] data) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    /** Maximum allowed byte array length during deserialization (50 MB). */
    private static final int MAX_BYTE_ARRAY_LENGTH = 50 * 1024 * 1024;

    /**
     * Reads a length-prefixed byte array with a defensive size bound.
     *
     * @param in source stream
     * @return the decoded bytes
     * @throws IOException if the length is invalid or the stream fails
     */
    public static byte[] readByteArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > MAX_BYTE_ARRAY_LENGTH) {
            throw new IOException("Invalid byte array length: " + len
                    + " (max " + MAX_BYTE_ARRAY_LENGTH + ")");
        }
        byte[] data = new byte[len];
        in.readFully(data);
        return data;
    }

    /**
     * Writes a list of integers as a length-prefixed sequence.
     *
     * @param out  target stream
     * @param list values to write
     * @throws IOException if the underlying stream fails
     */
    public static void writeIntList(DataOutputStream out, List<Integer> list) throws IOException {
        out.writeInt(list.size());
        for (int v : list) {
            out.writeInt(v);
        }
    }

    /**
     * Reads a length-prefixed integer list written by {@link #writeIntList}.
     *
     * @param in source stream
     * @return the decoded values
     * @throws IOException if the stream fails
     */
    public static List<Integer> readIntList(DataInputStream in) throws IOException {
        int size = in.readInt();
        var list = new ArrayList<Integer>(size);
        for (int i = 0; i < size; i++) {
            list.add(in.readInt());
        }
        return List.copyOf(list);
    }

    /**
     * Writes string postings, optionally including row positions.
     *
     * @param out            target stream
     * @param postings       term-to-positions mapping
     * @param storePositions whether positions are written after each term
     * @throws IOException if the underlying stream fails
     */
    public static void writeStringPostings(DataOutputStream out, Map<String, List<Integer>> postings,
                                     boolean storePositions) throws IOException {
        out.writeInt(postings.size());
        for (var entry : postings.entrySet()) {
            out.writeUTF(entry.getKey());
            if (storePositions) {
                writeIntList(out, entry.getValue());
            }
        }
    }

    /**
     * Reads string postings written by {@link #writeStringPostings}.
     *
     * @param in             source stream
     * @param storePositions whether positions follow each term
     * @return the decoded postings
     * @throws IOException if the stream fails
     */
    public static Map<String, List<Integer>> readStringPostings(DataInputStream in,
                                                          boolean storePositions) throws IOException {
        int size = in.readInt();
        var map = new LinkedHashMap<String, List<Integer>>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            List<Integer> positions = storePositions ? readIntList(in) : List.of();
            map.put(key, positions);
        }
        return Map.copyOf(map);
    }

    public static DataOutputStream newOutput(ByteArrayOutputStream baos) {
        return new DataOutputStream(baos);
    }

    public static DataInputStream newInput(byte[] payload) {
        return new DataInputStream(new ByteArrayInputStream(payload));
    }

    /**
     * Encodes a payload through a writer callback.
     *
     * @param writer callback writing to the provided stream
     * @return the encoded bytes
     */
    public static byte[] toBytes(IOWriter writer) {
        try {
            var baos = new ByteArrayOutputStream();
            var out = new DataOutputStream(baos);
            writer.write(out);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Decodes a payload through a reader callback.
     *
     * @param payload encoded bytes
     * @param reader  callback reading from the provided stream
     * @param <T>     decoded type
     * @return the decoded value
     */
    public static <T> T fromBytes(byte[] payload, IOReader<T> reader) {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(payload));
            return reader.read(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @FunctionalInterface
    public interface IOWriter {
        void write(DataOutputStream out) throws IOException;
    }

    @FunctionalInterface
    public interface IOReader<T> {
        T read(DataInputStream in) throws IOException;
    }
}
