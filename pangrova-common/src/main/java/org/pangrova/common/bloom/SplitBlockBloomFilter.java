package org.pangrova.common.bloom;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Split Block Bloom Filter (SBBF) — Parquet-compatible probabilistic filter.
 *
 * <p>The filter is divided into fixed-size 256-bit blocks (8 words × 32 bits).
 * Each value is mapped to exactly one block via primary hash, then 8 bit positions
 * are set within that block using 8 independent salt-derived sub-hashes. All memory
 * accesses for a single probe hit one cache line (32 bytes).</p>
 *
 * <p>Algorithm uses the same salt constants and block structure as the Apache Parquet
 * SBBF spec. The hash function is xxHash64 (seed=0), matching Parquet's Bloom filter
 * format for wire-compatible runtime filter push-down to Parquet row group statistics.</p>
 *
 * <p>Immutable after construction via {@link Builder#build()} or {@link #readFrom}.
 * Thread-safe for concurrent {@link #mightContain} calls.</p>
 */
@ThreadSafe
public final class SplitBlockBloomFilter {

    /** 8 fixed salts for intra-block bit selection (from Parquet spec). */
    private static final int[] SALTS = {
            0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    private static final int WORDS_PER_BLOCK = 8;
    private static final int BYTES_PER_BLOCK = WORDS_PER_BLOCK * Integer.BYTES; // 32
    private static final int HEADER_MAGIC = 0x53424246; // "SBBF"
    private static final int VERSION = 1;

    private final int[] blocks; // flat array: numBlocks * 8 words
    private final int numBlocks;
    private final int insertedCount;

    private SplitBlockBloomFilter(int[] blocks, int numBlocks, int insertedCount) {
        this.blocks = blocks;
        this.numBlocks = numBlocks;
        this.insertedCount = insertedCount;
    }

    /**
     * Creates a new SBBF and inserts all provided string values.
     *
     * @param expectedEntries expected number of distinct values
     * @param fpp             desired false positive probability (0, 1)
     * @return builder for inserting values
     */
    public static Builder builder(int expectedEntries, double fpp) {
        if (expectedEntries <= 0) {
            throw new IllegalArgumentException("expectedEntries must be positive, got " + expectedEntries);
        }
        if (fpp <= 0.0 || fpp >= 1.0) {
            throw new IllegalArgumentException("fpp must be in (0, 1), got " + fpp);
        }
        return new Builder(expectedEntries, fpp);
    }

    /**
     * Tests whether the given value might be contained in this filter.
     *
     * @param value the value to test (converted to UTF-8 bytes)
     * @return true if the value might be present; false if definitely absent
     */
    public boolean mightContain(CharSequence value) {
        if (value == null) {
            return false;
        }
        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        return mightContainBytes(bytes);
    }

    /**
     * Tests whether the given raw bytes might be contained in this filter.
     * Uses xxHash64 over the provided bytes directly — suitable for probing
     * with pre-encoded binary values (e.g. Parquet wire-format column bytes).
     *
     * @param bytes the raw bytes to probe
     * @return true if the value might be present; false if definitely absent
     */
    public boolean mightContainBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }
        long hash = hash64(bytes);
        int blockIndex = blockIndex(hash);
        int key = (int) (hash & 0xFFFFFFFFL);
        int wordOffset = blockIndex * WORDS_PER_BLOCK;

        for (int i = 0; i < WORDS_PER_BLOCK; i++) {
            int bitIndex = saltedBitIndex(key, i);
            if ((blocks[wordOffset + i] & (1 << bitIndex)) == 0) {
                return false;
            }
        }
        return true;
    }

    /** Returns the number of 256-bit blocks. */
    public int numBlocks() {
        return numBlocks;
    }

    /** Returns the number of distinct values inserted. */
    public int insertedCount() {
        return insertedCount;
    }

    /** Returns the total size in bytes of the block array. */
    public int sizeInBytes() {
        return numBlocks * BYTES_PER_BLOCK;
    }

    /**
     * Serializes this filter to a byte array.
     * Format: [magic:4][version:4][numBlocks:4][insertedCount:4][blocks...]
     */
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(16 + blocks.length * 4);
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(HEADER_MAGIC);
            dos.writeInt(VERSION);
            dos.writeInt(numBlocks);
            dos.writeInt(insertedCount);
            for (int word : blocks) {
                dos.writeInt(word);
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream never throws IOException
            throw new IllegalStateException("Unexpected serialization error", e);
        }
    }

    /**
     * Deserializes a filter from a byte array produced by {@link #toByteArray()}.
     *
     * @param data serialized SBBF bytes
     * @return deserialized filter
     * @throws IllegalArgumentException if data is malformed
     */
    public static SplitBlockBloomFilter readFrom(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("SBBF data must not be null");
        }
        return readBuffer(ByteBuffer.wrap(data));
    }

    /**
     * Decodes the remaining bytes without copying the encoded payload or changing
     * the caller's cursor, limit, or byte order. The returned filter owns its
     * decoded words and retains no reference to the input. Callers must not
     * mutate writable aliases during decoding. The wire order is big endian.
     *
     * @throws IllegalArgumentException if the header or body is malformed
     */
    public static SplitBlockBloomFilter readBuffer(ByteBuffer data) {
        if (data == null || data.remaining() < 16) {
            throw new IllegalArgumentException("SBBF data too short");
        }
        ByteBuffer input = data.duplicate().order(ByteOrder.BIG_ENDIAN);
        int magic = input.getInt();
        int version = input.getInt();
        int numBlocks = input.getInt();
        int insertedCount = input.getInt();
        validateHeader(magic, version, numBlocks);
        // Use long arithmetic before allocation: a malicious power-of-two block
        // count must not overflow into a tiny allocation or bypass truncation checks.
        long requiredBytes = (long) numBlocks * BYTES_PER_BLOCK;
        if (insertedCount < 0 || requiredBytes > input.remaining()) {
            throw new IllegalArgumentException("Invalid or truncated SBBF body");
        }
        int[] blocks = new int[numBlocks * WORDS_PER_BLOCK];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = input.getInt();
        }
        return new SplitBlockBloomFilter(blocks, numBlocks, insertedCount);
    }

    private static void validateHeader(int magic, int version, int numBlocks) {
        if (magic != HEADER_MAGIC) {
            throw new IllegalArgumentException(
                    "Invalid SBBF magic: expected 0x53424246, got 0x" + Integer.toHexString(magic));
        }
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported SBBF version: " + version);
        }
        if (numBlocks <= 0 || (numBlocks & (numBlocks - 1)) != 0) {
            throw new IllegalArgumentException("numBlocks must be a positive power of 2, got " + numBlocks);
        }
    }

    // --- Internal helpers ---

    private int blockIndex(long hash) {
        return ((int) (hash >>> 32)) & (numBlocks - 1);
    }

    private static int saltedBitIndex(int key, int saltIndex) {
        // Unsigned multiply, take top 5 bits → [0, 31]
        return ((int) ((key & 0xFFFFFFFFL) * (SALTS[saltIndex] & 0xFFFFFFFFL) >>> 27)) & 0x1F;
    }

    // xxHash64 constants (seed=0)
    private static final long XX_PRIME1 = 0x9E3779B185EBCA87L;
    private static final long XX_PRIME2 = 0x14DEF9DEA2F79CD6L;
    private static final long XX_PRIME3 = 0x165667B19E3779F9L;
    private static final long XX_PRIME4 = 0xC2B2AE3D27D4EB4FL;
    private static final long XX_PRIME5 = 0x27D4EB2F165667C5L;

    /**
     * xxHash64 (seed=0) — Parquet-compatible 64-bit hash function.
     * Matches the hash used by Apache Parquet's Split Block Bloom Filter spec.
     */
    static long hash64(byte[] data) {
        int len = data.length;
        int offset = 0;
        long h64;

        if (len >= 32) {
            h64 = hashLargeInput(data, len);
            offset = len & ~31; // advance past processed 32-byte chunks
        } else {
            h64 = XX_PRIME5;
        }

        h64 += len;
        h64 = hashTail(data, offset, len, h64);
        return avalanche(h64);
    }

    private static long hashLargeInput(byte[] data, int len) {
        long v1 = XX_PRIME1 + XX_PRIME2;
        long v2 = XX_PRIME2;
        long v3 = 0;
        long v4 = -XX_PRIME1;
        int offset = 0;

        do {
            v1 = xxRound(v1, getLong(data, offset));
            v2 = xxRound(v2, getLong(data, offset + 8));
            v3 = xxRound(v3, getLong(data, offset + 16));
            v4 = xxRound(v4, getLong(data, offset + 24));
            offset += 32;
        } while (offset <= len - 32);

        long h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7)
                + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
        h64 = mergeRound(h64, v1);
        h64 = mergeRound(h64, v2);
        h64 = mergeRound(h64, v3);
        return mergeRound(h64, v4);
    }

    private static long hashTail(byte[] data, int offset, int len, long h64) {
        while (offset <= len - 8) {
            long k1 = getLong(data, offset) * XX_PRIME2;
            k1 = Long.rotateLeft(k1, 31) * XX_PRIME1;
            h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * XX_PRIME1 + XX_PRIME4;
            offset += 8;
        }
        while (offset <= len - 4) {
            h64 ^= (getInt(data, offset) & 0xFFFFFFFFL) * XX_PRIME1;
            h64 = Long.rotateLeft(h64, 23) * XX_PRIME2 + XX_PRIME3;
            offset += 4;
        }
        while (offset < len) {
            h64 ^= (data[offset] & 0xFFL) * XX_PRIME5;
            h64 = Long.rotateLeft(h64, 11) * XX_PRIME1;
            offset++;
        }
        return h64;
    }

    private static long avalanche(long h64) {
        h64 ^= h64 >>> 33;
        h64 *= XX_PRIME2;
        h64 ^= h64 >>> 29;
        h64 *= XX_PRIME3;
        h64 ^= h64 >>> 32;
        return h64;
    }

    private static long xxRound(long acc, long input) {
        acc += input * XX_PRIME2;
        return Long.rotateLeft(acc, 31) * XX_PRIME1;
    }

    private static long mergeRound(long acc, long val) {
        acc ^= xxRound(0, val);
        return acc * XX_PRIME1 + XX_PRIME4;
    }

    private static long getLong(byte[] data, int offset) {
        return (data[offset] & 0xFFL)
                | ((data[offset + 1] & 0xFFL) << 8)
                | ((data[offset + 2] & 0xFFL) << 16)
                | ((data[offset + 3] & 0xFFL) << 24)
                | ((data[offset + 4] & 0xFFL) << 32)
                | ((data[offset + 5] & 0xFFL) << 40)
                | ((data[offset + 6] & 0xFFL) << 48)
                | ((data[offset + 7] & 0xFFL) << 56);
    }

    private static int getInt(byte[] data, int offset) {
        return (data[offset] & 0xFF)
                | ((data[offset + 1] & 0xFF) << 8)
                | ((data[offset + 2] & 0xFF) << 16)
                | ((data[offset + 3] & 0xFF) << 24);
    }

    /**
     * Mutable builder for constructing a {@link SplitBlockBloomFilter}.
     * Not thread-safe — use from a single thread, then call {@link #build()}.
     */
    public static final class Builder {

        private final int[] blocks;
        private final int numBlocks;
        private int insertedCount;

        private Builder(int expectedEntries, double fpp) {
            // Optimal total bits: m = -n·ln(p) / (ln2)²
            double m = -expectedEntries * Math.log(fpp) / (Math.log(2) * Math.log(2));
            // Each block = 256 bits; numBlocks must be power of 2
            int rawBlocks = Math.max(1, (int) Math.ceil(m / 256.0));
            this.numBlocks = nextPowerOfTwo(rawBlocks);
            this.blocks = new int[this.numBlocks * WORDS_PER_BLOCK];
        }

        /**
         * Inserts a value into the filter being built.
         *
         * @param value the value to insert (converted to UTF-8 bytes)
         * @return this builder
         */
        public Builder put(CharSequence value) {
            if (value == null) {
                return this;
            }
            byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
            return putBytes(bytes);
        }

        /**
         * Inserts raw bytes into the filter being built.
         * Uses xxHash64 over the provided bytes directly — suitable for
         * pre-encoded binary values (e.g. Parquet wire-format column bytes).
         *
         * @param bytes the raw bytes to insert
         * @return this builder
         */
        public Builder putBytes(byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return this;
            }
            long hash = hash64(bytes);
            int blockIndex = ((int) (hash >>> 32)) & (numBlocks - 1);
            int key = (int) (hash & 0xFFFFFFFFL);
            int wordOffset = blockIndex * WORDS_PER_BLOCK;

            for (int i = 0; i < WORDS_PER_BLOCK; i++) {
                int bitIndex = saltedBitIndex(key, i);
                blocks[wordOffset + i] |= (1 << bitIndex);
            }
            insertedCount++;
            return this;
        }

        /**
         * Builds an immutable {@link SplitBlockBloomFilter} from the inserted values.
         *
         * @return immutable SBBF instance
         */
        public SplitBlockBloomFilter build() {
            return new SplitBlockBloomFilter(blocks.clone(), numBlocks, insertedCount);
        }

        private static int nextPowerOfTwo(int value) {
            int highBit = Integer.highestOneBit(value);
            return (highBit == value) ? value : highBit << 1;
        }
    }
}
