package org.pangrova.planner.runtimefilter;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying the RuntimeFilter mechanism works end-to-end
 * for a TPC-H Q5 multi-table join pattern.
 *
 * <p>TPC-H Q5 joins REGION → NATION → SUPPLIER → LINEITEM → ORDERS → CUSTOMER
 * with a selective region filter on the build side. This test simulates the
 * nation-supplier join where the build side (nation keys filtered by region)
 * produces a runtime filter that prunes probe-side (supplier) rows.</p>
 *
 * <p>Validates:
 * <ul>
 *   <li>RuntimeFilterGenerator produces a filter from build-side values</li>
 *   <li>RuntimeFilterRegistry correctly stores the generated filter</li>
 *   <li>The bloom filter hit rate is &gt; 0 (actually filters rows)</li>
 *   <li>Binary encoding (LE int64) is consistent between insert and probe</li>
 * </ul>
 */
class RuntimeFilterJoinIntegrationTest {

    private final RuntimeFilterGenerator generator = new RuntimeFilterGenerator();
    private final RuntimeFilterRegistry registry = new RuntimeFilterRegistry();

    @Test
    @DisplayName("TPC-H Q5 join: nation build-side filter prunes supplier probe-side rows")
    void tpchQ5Join_runtimeFilterHitRatePositive() {
        // --- Setup: simulate TPC-H Q5 build side ---
        // TPC-H Q5 filters nations by region (e.g., ASIA = region_key 2).
        // After the region filter, only 5 nation keys remain on the build side.
        // These nation keys are: 8 (India), 9 (Indonesia), 12 (Japan), 18 (China), 21 (Vietnam)
        List<Comparable<?>> buildSideNationKeys = List.of(8L, 9L, 12L, 18L, 21L);

        // --- Act: generate runtime filter from build-side values ---
        // NDV = 5 (< 1000 threshold) → produces MinMax filter
        Optional<RuntimeFilter> filterOpt = generator.generate(
                "n_nationkey", buildSideNationKeys, PangrovaType.BIGINT);

        // --- Assert: filter generated and registered ---
        assertThat(filterOpt).isPresent();
        RuntimeFilter filter = filterOpt.get();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
        assertThat(filter.column()).isEqualTo("n_nationkey");

        // Register in registry (simulates coordinator pipeline)
        registry.register("probe-stage-1", filter);
        assertThat(registry.getFilter("probe-stage-1")).isPresent();

        // --- Verify hit rate > 0: filter actually prunes rows ---
        // Probe side: supplier table has s_nationkey values 0..24 (25 nations total).
        // The MinMax filter [8, 21] should pass values in range and reject values outside.
        int totalProbeRows = 25;
        int filteredOut = 0;
        for (long nationKey = 0; nationKey < totalProbeRows; nationKey++) {
            if (!filter.mightContain(nationKey)) {
                filteredOut++;
            }
        }

        // Hit rate = filteredOut / totalProbeRows > 0
        // Values 0-7 and 22-24 should be filtered out (11 rows)
        assertThat(filteredOut).isGreaterThan(0)
                .as("RuntimeFilter must filter at least some probe-side rows (hit rate > 0)");
        assertThat(filteredOut).isEqualTo(11); // 8 below min + 3 above max
    }

    @Test
    @DisplayName("TPC-H Q5 join with large build side: bloom filter hit rate > 0")
    void tpchQ5Join_bloomFilter_hitRatePositive() {
        // --- Setup: simulate a larger join scenario ---
        // Suppose the build side has > 1000 distinct supplier keys after filtering
        // (e.g., suppliers in ASIA region). This triggers Bloom filter generation.
        List<Comparable<?>> buildSideSupplierKeys = new ArrayList<>();
        // Build side: 1500 supplier keys (IDs 1000..2499) from the filtered region
        IntStream.range(1000, 2500).forEach(i -> buildSideSupplierKeys.add((long) i));

        // --- Act: generate runtime filter ---
        Optional<RuntimeFilter> filterOpt = generator.generate(
                "s_suppkey", buildSideSupplierKeys, PangrovaType.BIGINT);

        // --- Assert: bloom filter generated ---
        assertThat(filterOpt).isPresent();
        RuntimeFilter filter = filterOpt.get();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);
        assertThat(filter.column()).isEqualTo("s_suppkey");
        assertThat(filter.bloomFilter()).isNotNull();

        // Register in registry
        registry.register("probe-stage-2", filter);
        assertThat(registry.getFilter("probe-stage-2")).isPresent();
        assertThat(registry.registeredStageIds()).contains("probe-stage-2");

        // --- Verify bloom filter hit rate > 0 ---
        // Probe side: lineitem table references supplier keys 0..9999 (10000 total).
        // Only keys 1000..2499 are in the bloom filter.
        // Keys outside this range should be filtered out (with possible false positives).
        SplitBlockBloomFilter bloomFilter = filter.bloomFilter();
        int totalProbeRows = 10000;
        int filteredOut = 0;
        int falseNegatives = 0;

        for (int suppKey = 0; suppKey < totalProbeRows; suppKey++) {
            byte[] encoded = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(suppKey)
                    .array();
            boolean mightContain = bloomFilter.mightContainBytes(encoded);

            if (!mightContain) {
                filteredOut++;
                // Values in [1000, 2499] must NOT be filtered out (no false negatives)
                if (suppKey >= 1000 && suppKey < 2500) {
                    falseNegatives++;
                }
            }
        }

        // Hit rate = filteredOut / totalProbeRows must be > 0
        assertThat(filteredOut).isGreaterThan(0)
                .as("Bloom filter must filter at least some probe-side rows (hit rate > 0)");

        // No false negatives: all build-side values must pass the filter
        assertThat(falseNegatives).isZero()
                .as("Bloom filter must not produce false negatives for inserted values");

        // Selectivity check: at least 50% of non-matching rows should be filtered
        // (8500 rows are not in the filter; with FPP=0.03, expect ~97% filtered)
        int nonMatchingRows = totalProbeRows - 1500; // 8500
        double filterRate = (double) filteredOut / nonMatchingRows;
        assertThat(filterRate).isGreaterThan(0.5)
                .as("Bloom filter selectivity should filter >50%% of non-matching rows, got %.2f%%",
                        filterRate * 100);
    }

    @Test
    @DisplayName("TPC-H Q5 join: end-to-end binary encoding consistency between build and probe")
    void tpchQ5Join_binaryEncodingConsistency() {
        // --- Setup: build side with typed BIGINT values ---
        List<Comparable<?>> buildValues = new ArrayList<>();
        IntStream.range(0, 1500).forEach(i -> buildValues.add((long) i));

        // --- Act: generate bloom filter with BIGINT encoding ---
        RuntimeFilter filter = generator.generate("join_key", buildValues, PangrovaType.BIGINT)
                .orElseThrow();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);

        SplitBlockBloomFilter bloomFilter = filter.bloomFilter();

        // --- Verify: probe with same LE encoding finds all build-side values ---
        // This validates that encodeForBloom (build) and mightContainBytes (probe)
        // use the same binary format — the core fix from Req 1.1
        for (int i = 0; i < 1500; i++) {
            byte[] probeBytes = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(i)
                    .array();
            assertThat(bloomFilter.mightContainBytes(probeBytes))
                    .as("Build-side value %d must be found in bloom filter via LE binary probe", i)
                    .isTrue();
        }

        // Verify some values NOT in the filter are rejected (hit rate > 0)
        int rejected = 0;
        for (int suppKey = 5000; suppKey < 6000; suppKey++) {
            byte[] probeBytes = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(suppKey)
                    .array();
            if (!bloomFilter.mightContainBytes(probeBytes)) {
                rejected++;
            }
        }
        assertThat(rejected).isGreaterThan(0)
                .as("Values not in the filter must be rejected (hit rate > 0)");
    }

    @Test
    @DisplayName("TPC-H Q5 join: DOUBLE type encoding for revenue aggregation join")
    void tpchQ5Join_doubleTypeEncoding_hitRatePositive() {
        // TPC-H Q5 also involves revenue computation with DOUBLE values.
        // Verify DOUBLE encoding works correctly for bloom filter.
        List<Comparable<?>> buildValues = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            buildValues.add(i * 1.5); // revenue-like double values
        }

        RuntimeFilter filter = generator.generate("l_extendedprice", buildValues, PangrovaType.DOUBLE)
                .orElseThrow();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);

        SplitBlockBloomFilter bloomFilter = filter.bloomFilter();

        // Verify all build-side values are found
        for (int i = 0; i < 1500; i++) {
            double value = i * 1.5;
            byte[] probeBytes = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble(value)
                    .array();
            assertThat(bloomFilter.mightContainBytes(probeBytes))
                    .as("Build-side DOUBLE value %f must be found in bloom filter", value)
                    .isTrue();
        }

        // Verify non-matching values are filtered
        int rejected = 0;
        for (int i = 5000; i < 6000; i++) {
            double value = i * 1.5;
            byte[] probeBytes = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble(value)
                    .array();
            if (!bloomFilter.mightContainBytes(probeBytes)) {
                rejected++;
            }
        }
        assertThat(rejected).isGreaterThan(0)
                .as("Non-matching DOUBLE values must be rejected (hit rate > 0)");
    }
}
