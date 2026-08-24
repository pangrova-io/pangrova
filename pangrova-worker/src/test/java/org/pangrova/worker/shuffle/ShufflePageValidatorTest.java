package org.pangrova.worker.shuffle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies Presto Pages header parsing and validation logic in
 * {@link ShufflePageValidator}.
 *
 * <p><b>Validates: Requirements Req-3</b> — Java-side Presto Pages
 * deserialization support for the native shuffle path.</p>
 */
class ShufflePageValidatorTest {

    @Test
    void extractRowCount_readsFromHeader() {
        byte[] page = buildPage(42, ShufflePageValidator.CODEC_LZ4, 100, 80, new byte[80]);
        assertThat(ShufflePageValidator.extractRowCount(page)).isEqualTo(42);
    }

    @Test
    void extractCodec_readsFromHeader() {
        byte[] page = buildPage(10, ShufflePageValidator.CODEC_ZSTD, 50, 40, new byte[40]);
        assertThat(ShufflePageValidator.extractCodec(page))
                .isEqualTo(ShufflePageValidator.CODEC_ZSTD);
    }

    @Test
    void extractUncompressedSize_readsFromHeader() {
        byte[] page = buildPage(5, ShufflePageValidator.CODEC_NONE, 256, 256, new byte[256]);
        assertThat(ShufflePageValidator.extractUncompressedSize(page)).isEqualTo(256);
    }

    @Test
    void extractCompressedSize_readsFromHeader() {
        byte[] page = buildPage(5, ShufflePageValidator.CODEC_LZ4, 256, 128, new byte[128]);
        assertThat(ShufflePageValidator.extractCompressedSize(page)).isEqualTo(128);
    }

    @Test
    void isValidShufflePage_acceptsValidPage() {
        byte[] page = buildPage(100, ShufflePageValidator.CODEC_LZ4, 1024, 512, new byte[512]);
        assertThat(ShufflePageValidator.isValidShufflePage(page)).isTrue();
    }

    @Test
    void isValidShufflePage_acceptsUncompressedPage() {
        byte[] page = buildPage(10, ShufflePageValidator.CODEC_NONE, 200, 200, new byte[200]);
        assertThat(ShufflePageValidator.isValidShufflePage(page)).isTrue();
    }

    @Test
    void isValidShufflePage_acceptsZeroRowPage() {
        byte[] page = buildPage(0, ShufflePageValidator.CODEC_NONE, 0, 0, new byte[0]);
        assertThat(ShufflePageValidator.isValidShufflePage(page)).isTrue();
    }

    @Test
    void isValidShufflePage_rejectsNull() {
        assertThat(ShufflePageValidator.isValidShufflePage(null)).isFalse();
    }

    @Test
    void isValidShufflePage_rejectsTooShort() {
        assertThat(ShufflePageValidator.isValidShufflePage(new byte[12])).isFalse();
    }

    @Test
    void isValidShufflePage_rejectsUnknownCodec() {
        byte[] page = buildPage(1, (byte) 99, 10, 10, new byte[10]);
        assertThat(ShufflePageValidator.isValidShufflePage(page)).isFalse();
    }

    @Test
    void isValidShufflePage_rejectsTruncatedPayload() {
        // Header says compressed=100 but only 50 bytes of payload present
        byte[] page = buildPage(1, ShufflePageValidator.CODEC_LZ4, 200, 100, new byte[50]);
        assertThat(ShufflePageValidator.isValidShufflePage(page)).isFalse();
    }

    @Test
    void extractRowCount_rejectsNull() {
        assertThatThrownBy(() -> ShufflePageValidator.extractRowCount(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null");
    }

    @Test
    void extractRowCount_rejectsTooShort() {
        assertThatThrownBy(() -> ShufflePageValidator.extractRowCount(new byte[5]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too short");
    }

    @Test
    void headerSummary_validPage() {
        byte[] page = buildPage(50, ShufflePageValidator.CODEC_LZ4, 1000, 600, new byte[600]);
        String summary = ShufflePageValidator.headerSummary(page);
        assertThat(summary).contains("rows=50");
        assertThat(summary).contains("codec=LZ4");
        assertThat(summary).contains("uncompressed=1000");
        assertThat(summary).contains("compressed=600");
    }

    @Test
    void headerSummary_invalidPage() {
        assertThat(ShufflePageValidator.headerSummary(new byte[3])).isEqualTo("invalid");
    }

    @Test
    void headerSummary_noCompression() {
        byte[] page = buildPage(10, ShufflePageValidator.CODEC_NONE, 200, 200, new byte[200]);
        String summary = ShufflePageValidator.headerSummary(page);
        assertThat(summary).contains("codec=NONE");
    }

    @Test
    void headerSummary_zstdCompression() {
        byte[] page = buildPage(10, ShufflePageValidator.CODEC_ZSTD, 200, 150, new byte[150]);
        String summary = ShufflePageValidator.headerSummary(page);
        assertThat(summary).contains("codec=ZSTD");
    }

    /**
     * Builds a synthetic Presto Pages byte array with the given header values.
     *
     * <p>Layout: [4B numRows] [1B codec] [4B uncompressed] [4B compressed] [payload]</p>
     */
    private static byte[] buildPage(int numRows, byte codec,
                                    int uncompressedSize, int compressedSize,
                                    byte[] payload) {
        byte[] result = new byte[ShufflePageValidator.MIN_HEADER_SIZE + payload.length];
        writeInt32BigEndian(result, 0, numRows);
        result[4] = codec;
        writeInt32BigEndian(result, 5, uncompressedSize);
        writeInt32BigEndian(result, 9, compressedSize);
        System.arraycopy(payload, 0, result, ShufflePageValidator.MIN_HEADER_SIZE, payload.length);
        return result;
    }

    private static void writeInt32BigEndian(byte[] buf, int offset, int value) {
        buf[offset] = (byte) ((value >> 24) & 0xFF);
        buf[offset + 1] = (byte) ((value >> 16) & 0xFF);
        buf[offset + 2] = (byte) ((value >> 8) & 0xFF);
        buf[offset + 3] = (byte) (value & 0xFF);
    }
}
