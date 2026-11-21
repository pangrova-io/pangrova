package org.pangrova.common.bloom;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SplitBlockBloomBufferTest {

    @Test
    void decodesHeapDirectAndReadOnlySlicesWithoutChangingCallerState() {
        byte[] encoded = SplitBlockBloomFilter.builder(2, 0.01).put("one").put("two").build().toByteArray();
        ByteBuffer heap = ByteBuffer.allocate(encoded.length + 8);
        ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length + 8);
        for (ByteBuffer parent : List.of(heap, direct)) {
            parent.position(4);
            parent.put(encoded);
            parent.flip().position(4);
            ByteBuffer input = parent.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
            input.mark();
            var decoded = SplitBlockBloomFilter.readBuffer(input);
            assertThat(input.position()).isZero();
            assertThat(input.remaining()).isEqualTo(encoded.length);
            assertThat(input.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
            input.reset();
            parent.putInt(4, 0);
            assertThat(decoded.mightContain("one")).isTrue();
            assertThat(decoded.mightContain("two")).isTrue();
            assertThat(decoded.toByteArray()).containsExactly(encoded);
        }
    }

    @Test
    void rejectsOverflowingOrTruncatedLengthsBeforeAllocation() {
        ByteBuffer header = ByteBuffer.allocate(16).putInt(0x53424246).putInt(1).putInt(1 << 30).putInt(0);
        header.flip();
        assertThatThrownBy(() -> SplitBlockBloomFilter.readBuffer(header))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] encoded = SplitBlockBloomFilter.builder(1, 0.01).put("one").build().toByteArray();
        for (int limit = 0; limit < encoded.length; limit++) {
            ByteBuffer truncated = ByteBuffer.wrap(encoded).limit(limit);
            assertThatThrownBy(() -> SplitBlockBloomFilter.readBuffer(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        ByteBuffer negativeCount = ByteBuffer.wrap(encoded);
        negativeCount.putInt(12, -1);
        assertThatThrownBy(() -> SplitBlockBloomFilter.readBuffer(negativeCount))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
