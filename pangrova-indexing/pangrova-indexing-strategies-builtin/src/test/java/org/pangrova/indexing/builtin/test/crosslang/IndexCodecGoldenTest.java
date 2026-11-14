package org.pangrova.indexing.builtin.test.crosslang;


import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden-byte tests that pin the on-wire format of Java-produced index files.
 *
 * <p>Acts as a stand-in for full Java-writer / native-reader integration when
 * the native build is unavailable in CI. The native side has its own decoder
 * tests against the same hex fixtures (mirrored under pangrova-worker-native).
 * Any divergence between writer and reader will break this test, gating the
 * change before it reaches the cross-language pipeline.</p>
 */
class IndexCodecGoldenTest {

    // -----------------------------------------------------------------
    // IndexSerializer: header layout AEHT + version + typeId + reserved + len + payload + CRC32(header+payload)
    // -----------------------------------------------------------------

    @Test
    void indexSerializerHeaderLayoutIsStable() {
        TypeSerializerRegistry registry = TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class));
        IndexSerializer serializer = new IndexSerializer(registry, true);
        BloomFilterIndexData bloom = makeMinimalBloom();

        byte[] bytes = serializer.serialize(bloom);

        // Magic 'AETH' (0x41455448) big-endian at offset 0
        ByteBuffer hdr = ByteBuffer.wrap(bytes, 0, 16).order(ByteOrder.BIG_ENDIAN);
        assertThat(hdr.getInt()).isEqualTo(0x41455448);
        assertThat(hdr.getShort()).isEqualTo((short) 1); // version
        assertThat(hdr.getShort()).isEqualTo((short) IndexType.BLOOM_FILTER.getStableId());
        assertThat(hdr.getInt()).isEqualTo(0); // reserved
        int payloadLen = hdr.getInt();
        assertThat(payloadLen).isGreaterThan(0);
        assertThat(bytes.length).isEqualTo(16 + payloadLen + 4);

        // CRC32 covers header + payload (NOT payload alone) — 26.13 fix
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, 16 + payloadLen);
        int expected = (int) crc.getValue();
        int stored = ByteBuffer.wrap(bytes, 16 + payloadLen, 4)
                .order(ByteOrder.BIG_ENDIAN).getInt();
        assertThat(stored)
                .as("CRC32 must cover header+payload (26.13)")
                .isEqualTo(expected);
    }

    @Test
    void indexSerializerDetectsHeaderCorruption() {
        TypeSerializerRegistry registry = TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class));
        IndexSerializer serializer = new IndexSerializer(registry, true);
        BloomFilterIndexData bloom = makeMinimalBloom();
        byte[] bytes = serializer.serialize(bloom);

        // Flip a bit in the typeId field (offset 6, 2 bytes) — header-only corruption
        bytes[7] ^= 0x01;

        Throwable thrown = catchThrowable(() -> serializer.deserialize(bytes));
        assertThat(thrown)
                .as("CRC32 over header+payload must catch header bit-flip (26.13)")
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------

    private static BloomFilterIndexData makeMinimalBloom() {
        // 256-byte aligned filter (single block of 32 longs) keeps the test fixture small.
        byte[] filter = new byte[256];
        return new BloomFilterIndexData(filter, 3, 256L * 8);
    }

    private static Throwable catchThrowable(Runnable runnable) {
        try {
            runnable.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
