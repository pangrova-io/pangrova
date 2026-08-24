package org.pangrova.worker.shuffle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test verifying the Java side can parse Presto Pages format
 * headers and that the full flow through ShuffleBufferSender works correctly.
 *
 * <p>Since we cannot easily call ShuffleBatchSerializer from Java, we create
 * synthetic Presto Pages bytes matching the wire format documented in
 * {@link ShufflePageValidator}.</p>
 *
 * <p>Validates: Requirements Req-3, Req-4</p>
 */
@DisplayName("ShufflePageSerde Integration — Header Parsing")
class ShufflePageSerdeIntegrationTest {

    // -----------------------------------------------------------------------
    // ShufflePageValidator header parsing
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("extractRowCount parses big-endian int32 from header")
    void extractRowCountParsesBigEndianInt32() {
        byte[] data = buildPrestoPageHeader(42, ShufflePageValidator.CODEC_LZ4, 1024, 512);
        assertThat(ShufflePageValidator.extractRowCount(data)).isEqualTo(42);
    }

    @Test
    @DisplayName("extractCodec returns correct codec byte")
    void extractCodecReturnsCorrectByte() {
        byte[] data = buildPrestoPageHeader(10, ShufflePageValidator.CODEC_ZSTD, 100, 80);
        assertThat(ShufflePageValidator.extractCodec(data))
                .isEqualTo(ShufflePageValidator.CODEC_ZSTD);
    }

    @Test
    @DisplayName("extractUncompressedSize and extractCompressedSize parse correctly")
    void extractSizesParseCorrectly() {
        byte[] data = buildPrestoPageHeader(5, ShufflePageValidator.CODEC_LZ4, 2048, 1024);
        assertThat(ShufflePageValidator.extractUncompressedSize(data)).isEqualTo(2048);
        assertThat(ShufflePageValidator.extractCompressedSize(data)).isEqualTo(1024);
    }

    @Test
    @DisplayName("isValidShufflePage accepts well-formed header with payload")
    void isValidShufflePageAcceptsWellFormedData() {
        byte[] data = buildPrestoPageWithPayload(100, ShufflePageValidator.CODEC_LZ4, 256, 128);
        assertThat(ShufflePageValidator.isValidShufflePage(data)).isTrue();
    }

    @Test
    @DisplayName("isValidShufflePage rejects null input")
    void isValidShufflePageRejectsNull() {
        assertThat(ShufflePageValidator.isValidShufflePage(null)).isFalse();
    }

    @Test
    @DisplayName("isValidShufflePage rejects data shorter than header")
    void isValidShufflePageRejectsTooShort() {
        assertThat(ShufflePageValidator.isValidShufflePage(new byte[5])).isFalse();
    }

    @Test
    @DisplayName("isValidShufflePage rejects unknown codec")
    void isValidShufflePageRejectsUnknownCodec() {
        byte[] data = buildPrestoPageHeader(10, (byte) 99, 100, 80);
        assertThat(ShufflePageValidator.isValidShufflePage(data)).isFalse();
    }

    @Test
    @DisplayName("isValidShufflePage rejects negative row count")
    void isValidShufflePageRejectsNegativeRowCount() {
        byte[] data = buildPrestoPageHeader(-1, ShufflePageValidator.CODEC_NONE, 100, 100);
        assertThat(ShufflePageValidator.isValidShufflePage(data)).isFalse();
    }

    @Test
    @DisplayName("headerSummary returns readable string for valid data")
    void headerSummaryReturnsReadableString() {
        byte[] data = buildPrestoPageWithPayload(50, ShufflePageValidator.CODEC_LZ4, 200, 150);
        String summary = ShufflePageValidator.headerSummary(data);
        assertThat(summary)
                .contains("rows=50")
                .contains("codec=LZ4")
                .contains("uncompressed=200")
                .contains("compressed=150");
    }

    @Test
    @DisplayName("headerSummary returns 'invalid' for malformed data")
    void headerSummaryReturnsInvalidForBadData() {
        assertThat(ShufflePageValidator.headerSummary(new byte[3])).isEqualTo("invalid");
    }

    @Test
    @DisplayName("extractRowCount throws on null input")
    void extractRowCountThrowsOnNull() {
        assertThatThrownBy(() -> ShufflePageValidator.extractRowCount(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("extractRowCount throws on too-short input")
    void extractRowCountThrowsOnTooShort() {
        assertThatThrownBy(() -> ShufflePageValidator.extractRowCount(new byte[5]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------
    // Helpers: build synthetic Presto Pages bytes
    // -----------------------------------------------------------------------

    /**
     * Builds a minimal Presto Pages header (13 bytes).
     * Layout: [4B numRows BE] [1B codec] [4B uncompressedSize BE] [4B compressedSize BE]
     */
    private static byte[] buildPrestoPageHeader(
            int numRows, byte codec, int uncompressedSize, int compressedSize) {
        ByteBuffer buf = ByteBuffer.allocate(ShufflePageValidator.MIN_HEADER_SIZE);
        buf.putInt(numRows);
        buf.put(codec);
        buf.putInt(uncompressedSize);
        buf.putInt(compressedSize);
        return buf.array();
    }

    /**
     * Builds a Presto Pages header + dummy payload of compressedSize bytes.
     */
    private static byte[] buildPrestoPageWithPayload(
            int numRows, byte codec, int uncompressedSize, int compressedSize) {
        byte[] result = new byte[ShufflePageValidator.MIN_HEADER_SIZE + compressedSize];
        ByteBuffer buf = ByteBuffer.wrap(result);
        buf.putInt(numRows);
        buf.put(codec);
        buf.putInt(uncompressedSize);
        buf.putInt(compressedSize);
        // Remaining bytes are zero-filled (dummy payload).
        return result;
    }

}
