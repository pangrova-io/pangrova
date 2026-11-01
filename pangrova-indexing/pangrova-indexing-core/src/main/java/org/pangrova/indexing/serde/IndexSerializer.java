package org.pangrova.indexing.serde;

import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.IndexData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Serializes and deserializes {@link IndexData} with a common binary header.
 *
 * <p>Wire format:
 * <pre>
 * [magic: 4B] [version: 2B] [typeStableId: 2B] [reserved: 4B] [payloadLen: 4B] [payload: N B] [crc32: 4B]
 * </pre>
 *
 * <p>Delegates type-specific serialization to {@link TypeSerializer} implementations.
 */
public final class IndexSerializer {

    /** Stable identity of the common on-storage index envelope. */
    public static final String ARTIFACT_FORMAT_ID = "pangrova.index-file";
    public static final int ARTIFACT_FORMAT_MAJOR_VERSION = 1;
    public static final int ARTIFACT_FORMAT_MINOR_VERSION = 0;
    public static final String ARTIFACT_FORMAT_SCHEMA_SHA256 =
            "0505ea37ea32f59c6b216d3b4cb2c87e757e461859a6dc016ea3a19014cee7e6";
    public static final String WRITER_BUILD_ID = "pangrova-indexing-0.1.0";

    private static final Logger LOG = LoggerFactory.getLogger(IndexSerializer.class);

    static final int MAGIC = 0x41455448; // "AETH"
    static final short CURRENT_FORMAT_VERSION = 1;
    static final int HEADER_SIZE = 16; // 4 + 2 + 2 + 4 + 4
    static final int CRC_SIZE = 4;

    private final TypeSerializerRegistry registry;
    private final boolean checksumEnabled;

    /** Backward-compatible constructor — checksum enabled by default. */
    public IndexSerializer(TypeSerializerRegistry registry) {
        this(registry, true);
    }

    /**
     * Creates a serializer with explicit checksum control.
     *
     * @param registry        type serializer registry
     * @param checksumEnabled whether CRC32 checksum is written and verified
     */
    public IndexSerializer(TypeSerializerRegistry registry, boolean checksumEnabled) {
        this.registry = registry;
        this.checksumEnabled = checksumEnabled;
    }

    /**
     * Serializes index data to bytes with header and CRC32 trailer.
     *
     * @param data the index data to serialize
     * @return serialized bytes
     */
    public byte[] serialize(IndexData data) {
        TypeSerializer<?> serializer = registry.get(data.indexType());
        byte[] payload = serializePayload(serializer, data);

        int totalSize = checksumEnabled
                ? HEADER_SIZE + payload.length + CRC_SIZE
                : HEADER_SIZE + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(MAGIC);
        buf.putShort(CURRENT_FORMAT_VERSION);
        buf.putShort((short) data.indexType().getStableId());
        buf.putInt(0); // reserved
        buf.putInt(payload.length);
        buf.put(payload);

        if (checksumEnabled) {
            // CRC32 covers BOTH the header and the payload so that any bit
            // flip in the magic/version/typeId/length fields is detected on
            // read instead of producing a misinterpreted (and potentially
            // crashing) deserialization. Previously only the payload was
            // checksummed.
            CRC32 crc = new CRC32();
            crc.update(buf.array(), 0, HEADER_SIZE + payload.length);
            // CRC32.getValue() returns unsigned 32-bit value as long; narrowing is lossless
            buf.putInt((int) crc.getValue());
        }

        return buf.array();
    }

    /**
     * Deserializes index data from bytes, validating header and CRC32.
     *
     * <p>Returns {@link Optional#empty()} with a WARN log if the format version
     * is newer than {@link #CURRENT_FORMAT_VERSION} (forward-compatible skip).
     *
     * @param bytes the serialized bytes
     * @return the deserialized index data, or empty if version is unsupported
     * @throws IllegalArgumentException if the data is malformed
     */
    public Optional<IndexData> deserialize(byte[] bytes) {
        validateMinLength(bytes, checksumEnabled);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        validateMagic(buf);
        short version = buf.getShort();
        if (version > CURRENT_FORMAT_VERSION) {
            LOG.warn("action=skip_future_version version={} current={}",
                    version, CURRENT_FORMAT_VERSION);
            return Optional.empty();
        }
        IndexType indexType = readIndexType(buf);
        byte[] payload = readPayload(buf, bytes, checksumEnabled);
        return Optional.of(deserializeByVersion(version, indexType, payload));
    }

    private static void validateMinLength(byte[] bytes, boolean checksumEnabled) {
        int minLength = checksumEnabled ? HEADER_SIZE + CRC_SIZE : HEADER_SIZE;
        if (bytes.length < minLength) {
            throw new IllegalArgumentException(
                    "Data too short: expected at least " + minLength
                            + " bytes, got " + bytes.length);
        }
    }

    private static void validateMagic(ByteBuffer buf) {
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException(
                    "Invalid magic bytes: expected 0x" + Integer.toHexString(MAGIC)
                            + ", got 0x" + Integer.toHexString(magic));
        }
    }

    private static IndexType readIndexType(ByteBuffer buf) {
        short stableId = buf.getShort();
        return IndexType.fromStableId(stableId);
    }

    private static byte[] readPayload(ByteBuffer buf, byte[] bytes, boolean checksumEnabled) {
        buf.getInt(); // skip reserved
        int payloadLen = buf.getInt();
        int expectedLength = checksumEnabled
                ? HEADER_SIZE + payloadLen + CRC_SIZE
                : HEADER_SIZE + payloadLen;
        if (bytes.length != expectedLength) {
            throw new IllegalArgumentException(
                    "Payload length mismatch: header says " + payloadLen
                            + " but total bytes = " + bytes.length);
        }
        byte[] payload = new byte[payloadLen];
        buf.get(payload);
        if (checksumEnabled) {
            validateCrc(buf, bytes, payloadLen);
        }
        return payload;
    }

    private static void validateCrc(ByteBuffer buf, byte[] bytes, int payloadLen) {
        int storedCrc = buf.getInt();
        CRC32 crc = new CRC32();
        // Recompute over the on-wire header + payload (every byte that the
        // serializer fed into CRC32). Skipping the header would let a corrupt
        // magic/version/typeId slip through.
        crc.update(bytes, 0, HEADER_SIZE + payloadLen);
        int computedCrc = (int) crc.getValue();
        if (storedCrc != computedCrc) {
            throw new IllegalArgumentException(
                    "CRC32 mismatch: stored=0x" + Integer.toHexString(storedCrc)
                            + ", computed=0x" + Integer.toHexString(computedCrc));
        }
    }

    private IndexData deserializeByVersion(short version, IndexType indexType, byte[] payload) {
        // Version-specific deserialization dispatch
        return switch (version) {
            case 1 -> registry.get(indexType).deserialize(payload);
            default -> throw new IllegalArgumentException(
                    "Unsupported format version for deserialization: " + version);
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends IndexData> byte[] serializePayload(
            TypeSerializer<T> serializer, IndexData data) {
        return serializer.serialize((T) data);
    }
}
