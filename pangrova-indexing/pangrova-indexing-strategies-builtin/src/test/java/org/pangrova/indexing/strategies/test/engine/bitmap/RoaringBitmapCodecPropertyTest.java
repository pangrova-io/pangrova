package org.pangrova.indexing.strategies.test.engine.bitmap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.indexing.strategies.bitmap.serializer.RoaringBitmapCodec;

import java.util.BitSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link RoaringBitmapCodec} container-based encoding.
 *
 * <p><b>Validates: Requirements 7.1, 7.2</b></p>
 *
 * <p>Core properties:
 * <ol>
 *   <li>Round-trip: deserialize(serialize(positions)) equals the original BitSet</li>
 *   <li>Contains: for any set position, contains(serialized, pos) returns true</li>
 *   <li>Not-contains: for any unset position, contains(serialized, pos) returns false</li>
 *   <li>Size bound: 10K positions in a single chunk serialized ≤ 8KB</li>
 *   <li>Empty BitSet round-trip</li>
 * </ol>
 */
class RoaringBitmapCodecPropertyTest {

    // ---------------------------------------------------------------
    // Property 1: Round-trip preserves all positions
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void roundTripPreservesAllPositions(@ForAll("randomBitSet") BitSet original) {
        byte[] serialized = RoaringBitmapCodec.serialize(original);
        BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);

        assertThat(deserialized)
                .as("deserialize(serialize(positions)) must equal original (cardinality=%d)",
                        original.cardinality())
                .isEqualTo(original);
    }

    // ---------------------------------------------------------------
    // Property 2: Contains returns true for every set position
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void containsReturnsTrueForEverySetPosition(@ForAll("randomBitSet") BitSet positions) {
        if (positions.isEmpty()) return;

        byte[] serialized = RoaringBitmapCodec.serialize(positions);

        for (int pos = positions.nextSetBit(0); pos >= 0; pos = positions.nextSetBit(pos + 1)) {
            assertThat(RoaringBitmapCodec.contains(serialized, pos))
                    .as("contains(serialized, %d) must be true for a set position", pos)
                    .isTrue();
        }
    }

    // ---------------------------------------------------------------
    // Property 3: Contains returns false for unset positions
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void containsReturnsFalseForUnsetPositions(@ForAll("randomBitSet") BitSet positions) {
        byte[] serialized = RoaringBitmapCodec.serialize(positions);

        // Check a few positions that are NOT set
        int maxCheck = positions.isEmpty() ? 100 : positions.length() + 10;
        int checked = 0;
        for (int pos = 0; pos < maxCheck && checked < 20; pos++) {
            if (!positions.get(pos)) {
                assertThat(RoaringBitmapCodec.contains(serialized, pos))
                        .as("contains(serialized, %d) must be false for an unset position", pos)
                        .isFalse();
                checked++;
            }
        }
    }

    // ---------------------------------------------------------------
    // Property 4: Size bound — 10K positions in single chunk ≤ 8KB
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void singleChunkTenThousandPositionsFitInEightKb(
            @ForAll @IntRange(min = 4096, max = 10000) int count) {
        // All positions in range [0, 9999] — single chunk (high bits = 0)
        BitSet positions = new BitSet();
        for (int i = 0; i < count; i++) {
            positions.set(i);
        }

        byte[] serialized = RoaringBitmapCodec.serialize(positions);

        // Bitmap container: 8KB fixed + 5 bytes header + 4 bytes numChunks = 8201 bytes,
        // plus the 8-byte Pangrova codec header (magic + version) added in 26.11 = 8209.
        assertThat(serialized.length)
                .as("Serialized size for %d positions in single chunk must be ≤ 8209 bytes " +
                        "(bitmap container 8KB + 9 bytes overhead + 8B codec header)", count)
                .isLessThanOrEqualTo(8209);
    }

    // ---------------------------------------------------------------
    // Property 5: Empty BitSet round-trip
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void emptyBitSetRoundTrip() {
        BitSet empty = new BitSet();
        byte[] serialized = RoaringBitmapCodec.serialize(empty);
        BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);

        assertThat(deserialized).isEqualTo(empty);
        assertThat(deserialized.isEmpty()).isTrue();
        // Official RoaringBitmap empty serialization is a small fixed header,
        // plus the 8-byte Pangrova codec header (magic + version).
        assertThat(serialized.length).isLessThanOrEqualTo(24);
    }

    // ---------------------------------------------------------------
    // Additional: array container positions round-trip (sparse)
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void sparsePositionsUseArrayContainerAndRoundTrip(
            @ForAll @IntRange(min = 1, max = 4095) int count) {
        // Fewer than 4096 positions → array container in RoaringBitmap
        BitSet positions = new BitSet();
        int step = Math.max(1, 65535 / count);
        for (int i = 0; i < count; i++) {
            positions.set(i * step);
        }

        byte[] serialized = RoaringBitmapCodec.serialize(positions);
        BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);

        assertThat(serialized.length)
                .as("Sparse bitmap with %d positions should be compact (+8B codec header)", count)
                .isLessThan(count * 4 + 64 + 8);
        assertThat(deserialized).isEqualTo(positions);
    }

    // ---------------------------------------------------------------
    // Additional: multi-chunk round-trip
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void multiChunkPositionsRoundTrip(
            @ForAll @IntRange(min = 1, max = 5) int numChunks,
            @ForAll @IntRange(min = 1, max = 100) int positionsPerChunk) {
        BitSet positions = new BitSet();
        for (int chunk = 0; chunk < numChunks; chunk++) {
            int base = chunk * 65536; // each chunk is a 16-bit high segment
            for (int i = 0; i < positionsPerChunk; i++) {
                positions.set(base + i);
            }
        }

        byte[] serialized = RoaringBitmapCodec.serialize(positions);
        BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);

        assertThat(deserialized)
                .as("Multi-chunk round-trip with %d chunks × %d positions",
                        numChunks, positionsPerChunk)
                .isEqualTo(positions);
    }

    // ---------------------------------------------------------------
    // Generators
    // ---------------------------------------------------------------

    @Provide
    Arbitrary<BitSet> randomBitSet() {
        return Arbitraries.integers().between(0, 500)
                .flatMap(count -> {
                    if (count == 0) {
                        return Arbitraries.just(new BitSet());
                    }
                    return Arbitraries.integers().between(0, 100_000)
                            .list().ofSize(count)
                            .map(positions -> {
                                BitSet bs = new BitSet();
                                for (int pos : positions) {
                                    bs.set(pos);
                                }
                                return bs;
                            });
                });
    }
}
