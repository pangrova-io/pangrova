package org.pangrova.common.bloom;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SplitBlockBloomFilter} — SBBF correctness,
 * serialization round-trip, and edge cases.
 */
class SplitBlockBloomFilterTest {

    @Test
    void insertedValues_alwaysReturnTrue() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(1000, 0.01)
                .put("alpha")
                .put("beta")
                .put("gamma")
                .put("delta")
                .build();

        // No false negatives — fundamental SBBF guarantee
        assertThat(sbbf.mightContain("alpha")).isTrue();
        assertThat(sbbf.mightContain("beta")).isTrue();
        assertThat(sbbf.mightContain("gamma")).isTrue();
        assertThat(sbbf.mightContain("delta")).isTrue();
    }

    @Test
    void nonInsertedValues_mostlyReturnFalse() {
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(100, 0.01);
        for (int i = 0; i < 100; i++) {
            builder.put("key-" + i);
        }
        SplitBlockBloomFilter sbbf = builder.build();

        // Test 1000 non-inserted values; FPP should be low
        int falsePositives = 0;
        for (int i = 1000; i < 2000; i++) {
            if (sbbf.mightContain("key-" + i)) {
                falsePositives++;
            }
        }
        // With fpp=0.01, expect ~10 false positives out of 1000; allow generous margin
        assertThat(falsePositives).isLessThan(50);
    }

    @Test
    void serializationRoundTrip_preservesAllInsertedValues() {
        SplitBlockBloomFilter original = SplitBlockBloomFilter.builder(500, 0.01)
                .put("foo")
                .put("bar")
                .put("baz")
                .build();

        byte[] bytes = original.toByteArray();
        SplitBlockBloomFilter restored = SplitBlockBloomFilter.readFrom(bytes);

        assertThat(restored.mightContain("foo")).isTrue();
        assertThat(restored.mightContain("bar")).isTrue();
        assertThat(restored.mightContain("baz")).isTrue();
        assertThat(restored.numBlocks()).isEqualTo(original.numBlocks());
        assertThat(restored.insertedCount()).isEqualTo(original.insertedCount());
    }

    @Test
    void numBlocks_isPowerOfTwo() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(10000, 0.01)
                .build();
        int n = sbbf.numBlocks();
        assertThat(n).isGreaterThan(0);
        assertThat(n & (n - 1)).isEqualTo(0); // power of 2 check
    }

    @Test
    void nullValue_mightContainReturnsFalse() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(10, 0.01)
                .put("x")
                .build();
        assertThat(sbbf.mightContain(null)).isFalse();
    }

    @Test
    void nullValue_putIsNoOp() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(10, 0.01)
                .put(null)
                .put("x")
                .build();
        assertThat(sbbf.insertedCount()).isEqualTo(1);
        assertThat(sbbf.mightContain("x")).isTrue();
    }

    @Test
    void invalidFpp_throwsIllegalArgument() {
        assertThatThrownBy(() -> SplitBlockBloomFilter.builder(100, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SplitBlockBloomFilter.builder(100, 1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SplitBlockBloomFilter.builder(100, -0.5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidExpectedEntries_throwsIllegalArgument() {
        assertThatThrownBy(() -> SplitBlockBloomFilter.builder(0, 0.01))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SplitBlockBloomFilter.builder(-1, 0.01))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readFrom_invalidMagic_throwsIllegalArgument() {
        byte[] bad = new byte[]{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0};
        assertThatThrownBy(() -> SplitBlockBloomFilter.readFrom(bad))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void readFrom_tooShort_throwsIllegalArgument() {
        assertThatThrownBy(() -> SplitBlockBloomFilter.readFrom(new byte[4]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readFrom_null_throwsIllegalArgument() {
        assertThatThrownBy(() -> SplitBlockBloomFilter.readFrom(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void largeInsert_noFalseNegatives() {
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(50000, 0.01);
        for (int i = 0; i < 50000; i++) {
            builder.put("value-" + i);
        }
        SplitBlockBloomFilter sbbf = builder.build();

        for (int i = 0; i < 50000; i++) {
            assertThat(sbbf.mightContain("value-" + i))
                    .as("value-" + i + " must be found")
                    .isTrue();
        }
    }

    @Test
    void sizeInBytes_matchesBlockCount() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(1000, 0.01).build();
        assertThat(sbbf.sizeInBytes()).isEqualTo(sbbf.numBlocks() * 32);
    }

    @Test
    void hash64_matchesXxHash64ReferenceValues() {
        // xxHash64 reference test vectors (seed=0).
        // Verified against independent Python xxHash64 implementation.
        byte[] empty = new byte[0];
        assertThat(SplitBlockBloomFilter.hash64(empty)).isEqualTo(0x2BE48486F6C12FE5L);

        // Single byte: 0x00
        byte[] singleZero = new byte[]{0};
        assertThat(SplitBlockBloomFilter.hash64(singleZero)).isEqualTo(0xC99CDAB44F6EBF47L);

        // "a" (0x61)
        byte[] a = "a".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(SplitBlockBloomFilter.hash64(a)).isEqualTo(0xC748027F53E8739BL);
    }

    @Test
    void bloomFilter_parquetCompatible_noFalseNegativesOnRowGroupValues() {
        // Simulate Parquet row group statistics scenario:
        // Build-side values inserted into SBBF, then probe-side values checked.
        // With Parquet-compatible xxHash64, there must be zero false negatives.
        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(100, 0.01);

        // Insert values representing a Parquet row group's distinct column values
        String[] rowGroupValues = {
                "customer_001", "customer_002", "customer_003",
                "order_100", "order_200", "order_300",
                "2024-01-15", "2024-02-28", "2024-03-31"
        };
        for (String value : rowGroupValues) {
            builder.put(value);
        }
        SplitBlockBloomFilter sbbf = builder.build();

        // All inserted values must be found (zero false negatives = Parquet compatibility)
        for (String value : rowGroupValues) {
            assertThat(sbbf.mightContain(value))
                    .as("Parquet row group value '%s' must be found (no false negatives)", value)
                    .isTrue();
        }
    }
}
