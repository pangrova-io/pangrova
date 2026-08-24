package org.pangrova.worker.shuffle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates and extracts metadata from PrestoPage wire-format shuffle bytes.
 *
 * <p>The native engine (ShuffleBatchSerializer) serializes RowVectors using
 * {@code PrestoVectorSerde}. The resulting bytes are transferred via JNI and
 * the transport layer to downstream workers. This class provides lightweight
 * validation and metadata extraction without full deserialization — the
 * actual deserialization back to columnar vectors happens on the native side.</p>
 *
 * <h3>Shuffle page wire format (header layout):</h3>
 * <pre>
 *   [4 bytes] numRows        — int32 big-endian, number of rows in the page
 *   [1 byte]  codec          — compression codec id (0=NONE, 1=LZ4, 2=ZSTD)
 *   [4 bytes] uncompressedSize — int32 big-endian, size before compression
 *   [4 bytes] compressedSize   — int32 big-endian, size after compression
 *   [N bytes] payload         — serialized column data
 * </pre>
 *
 * <p>Thread safety: stateless utility class, all methods are safe for
 * concurrent use.</p>
 */
public final class ShufflePageValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ShufflePageValidator.class);

    /**
     * Minimum header size: 4 (numRows) + 1 (codec) + 4 (uncompressed) + 4 (compressed).
     */
    static final int MIN_HEADER_SIZE = 13;

    static final byte CODEC_NONE = 0;
    static final byte CODEC_LZ4 = 1;
    static final byte CODEC_ZSTD = 2;

    private ShufflePageValidator() {}

    /**
     * Extracts the row count from a shuffle page header.
     *
     * @param data serialized shuffle page bytes (must not be null)
     * @return number of rows encoded in the page header
     * @throws IllegalArgumentException if data is null or too short
     */
    public static int extractRowCount(byte[] data) {
        validateMinLength(data);
        return readInt32BigEndian(data, 0);
    }

    /**
     * Extracts the compression codec identifier from the page header.
     *
     * @param data serialized shuffle page bytes
     * @return codec byte: 0=NONE, 1=LZ4, 2=ZSTD
     * @throws IllegalArgumentException if data is null or too short
     */
    public static byte extractCodec(byte[] data) {
        validateMinLength(data);
        return data[4];
    }

    /**
     * Extracts the uncompressed payload size from the page header.
     *
     * @param data serialized shuffle page bytes
     * @return uncompressed size in bytes
     * @throws IllegalArgumentException if data is null or too short
     */
    public static int extractUncompressedSize(byte[] data) {
        validateMinLength(data);
        return readInt32BigEndian(data, 5);
    }

    /**
     * Extracts the compressed payload size from the page header.
     *
     * @param data serialized shuffle page bytes
     * @return compressed size in bytes
     * @throws IllegalArgumentException if data is null or too short
     */
    public static int extractCompressedSize(byte[] data) {
        validateMinLength(data);
        return readInt32BigEndian(data, 9);
    }

    /**
     * Validates whether the given bytes represent a structurally valid
     * shuffle page payload.
     *
     * <p>Checks performed:</p>
     * <ul>
     *   <li>Minimum header length (13 bytes)</li>
     *   <li>Row count is non-negative</li>
     *   <li>Codec byte is a known value (0, 1, or 2)</li>
     *   <li>Uncompressed and compressed sizes are non-negative</li>
     *   <li>Total length is consistent with header + compressed payload</li>
     * </ul>
     *
     * @param data bytes to validate
     * @return true if the data passes all structural checks
     */
    public static boolean isValidShufflePage(byte[] data) {
        if (data == null || data.length < MIN_HEADER_SIZE) {
            return false;
        }
        if (readInt32BigEndian(data, 0) < 0) {
            return false;
        }
        if (!isKnownCodec(data[4])) {
            return false;
        }
        return hasValidPayloadSizes(data);
    }

    private static boolean isKnownCodec(byte codec) {
        return codec == CODEC_NONE || codec == CODEC_LZ4 || codec == CODEC_ZSTD;
    }

    private static boolean hasValidPayloadSizes(byte[] data) {
        int uncompressedSize = readInt32BigEndian(data, 5);
        int compressedSize = readInt32BigEndian(data, 9);
        if (uncompressedSize < 0 || compressedSize < 0) {
            return false;
        }
        return data.length >= MIN_HEADER_SIZE + compressedSize;
    }

    /**
     * Returns a human-readable summary of the shuffle page header for
     * diagnostic logging.
     *
     * @param data serialized shuffle page bytes
     * @return summary string, or "invalid" if data is malformed
     */
    public static String headerSummary(byte[] data) {
        if (!isValidShufflePage(data)) {
            return "invalid";
        }
        int rows = readInt32BigEndian(data, 0);
        byte codec = data[4];
        int uncompressed = readInt32BigEndian(data, 5);
        int compressed = readInt32BigEndian(data, 9);
        String codecName = switch (codec) {
            case CODEC_NONE -> "NONE";
            case CODEC_LZ4 -> "LZ4";
            case CODEC_ZSTD -> "ZSTD";
            default -> "UNKNOWN(" + codec + ")";
        };
        return "rows=" + rows
                + " codec=" + codecName
                + " uncompressed=" + uncompressed
                + " compressed=" + compressed
                + " totalBytes=" + data.length;
    }

    private static void validateMinLength(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        if (data.length < MIN_HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "data too short for shuffle page header: length="
                            + data.length + " required=" + MIN_HEADER_SIZE);
        }
    }

    private static int readInt32BigEndian(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24)
                | ((data[offset + 1] & 0xFF) << 16)
                | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }
}
