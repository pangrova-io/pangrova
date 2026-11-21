package org.pangrova.indexing.strategies.test.engine.bitmap;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.strategies.bitmap.serializer.RoaringBitmapCodec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden-byte tests for the RoaringBitmap on-wire format (26.11).
 *
 * <p>Pins the magic + version header so native readers can detect bare
 * RoaringBitmap payloads (no header) written by older builds. The native
 * decoder has its own mirror tests against the same expected bytes.</p>
 */
class RoaringBitmapCodecGoldenTest {

    @Test
    void headerIsAsciiMagicAndBigEndianVersion() {
        BitSet positions = new BitSet();
        positions.set(0);
        positions.set(42);
        positions.set(65535);

        byte[] bytes = RoaringBitmapCodec.serialize(positions);

        assertThat(bytes[0]).isEqualTo((byte) 'A');
        assertThat(bytes[1]).isEqualTo((byte) 'R');
        assertThat(bytes[2]).isEqualTo((byte) 'B');
        assertThat(bytes[3]).isEqualTo((byte) 'M');

        ByteBuffer hdr = ByteBuffer.wrap(bytes, 4, 4).order(ByteOrder.BIG_ENDIAN);
        assertThat(hdr.getInt()).isEqualTo(RoaringBitmapCodec.VERSION);

        BitSet decoded = RoaringBitmapCodec.deserialize(bytes);
        assertThat(decoded).isEqualTo(positions);
    }

    @Test
    void corruptMagicIsRejected() {
        BitSet positions = new BitSet();
        positions.set(1);
        byte[] bytes = RoaringBitmapCodec.serialize(positions);
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;
        bytes[3] = 0;

        Throwable thrown = catchThrowable(() -> RoaringBitmapCodec.deserialize(bytes));
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        assertThat(thrown.getMessage()).contains("magic");
    }

    @Test
    void unsupportedVersionIsRejected() {
        BitSet positions = new BitSet();
        positions.set(7);
        byte[] bytes = RoaringBitmapCodec.serialize(positions);
        // Bump version field (offset 4-7) to a future value
        ByteBuffer.wrap(bytes, 4, 4).order(ByteOrder.BIG_ENDIAN).putInt(999);

        Throwable thrown = catchThrowable(() -> RoaringBitmapCodec.deserialize(bytes));
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        assertThat(thrown.getMessage()).contains("version");
    }

    private static Throwable catchThrowable(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
