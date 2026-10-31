package org.pangrova.planner.runtimefilter;

import org.pangrova.connector.spi.data.type.PangrovaType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeFilterGeneratorTest {

    private final RuntimeFilterGenerator generator = new RuntimeFilterGenerator();

    @Test
    void emptyInput_returnsEmpty() {
        assertThat(generator.generate("col", Collections.emptyList(), PangrovaType.INTEGER)).isEmpty();
    }

    @Test
    void nullInput_returnsEmpty() {
        assertThat(generator.generate("col", null, PangrovaType.INTEGER)).isEmpty();
    }

    @Test
    void smallDataset_producesMinMaxFilter() {
        List<Integer> values = List.of(5, 3, 8, 1, 7);
        Optional<RuntimeFilter> result = generator.generate("id", values, PangrovaType.INTEGER);

        assertThat(result).isPresent();
        RuntimeFilter filter = result.get();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
        assertThat(filter.column()).isEqualTo("id");
        assertThat((Comparable) filter.min().orElse(null)).isEqualTo(1);
        assertThat((Comparable) filter.max().orElse(null)).isEqualTo(8);
    }

    @Test
    void singleValue_producesMinMaxWithEqualBounds() {
        Optional<RuntimeFilter> result = generator.generate("k", List.of(42), PangrovaType.INTEGER);

        assertThat(result).isPresent();
        RuntimeFilter filter = result.get();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
        assertThat((Comparable) filter.min().orElse(null)).isEqualTo(42);
        assertThat((Comparable) filter.max().orElse(null)).isEqualTo(42);
    }

    @Test
    void largeDataset_producesBloomFilter() {
        List<Integer> values = new ArrayList<>();
        IntStream.rangeClosed(1, 1500).forEach(values::add);

        Optional<RuntimeFilter> result = generator.generate("key", values, PangrovaType.INTEGER);

        assertThat(result).isPresent();
        RuntimeFilter filter = result.get();
        assertThat(filter.type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);
        assertThat(filter.column()).isEqualTo("key");
        assertThat(filter.bloomFilter()).isNotNull();
    }

    @Test
    void exactlyThreshold_producesMinMax() {
        List<Integer> values = new ArrayList<>();
        IntStream.rangeClosed(1, 1000).forEach(values::add);

        Optional<RuntimeFilter> result = generator.generate("col", values, PangrovaType.INTEGER);

        assertThat(result).isPresent();
        assertThat(result.get().type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
    }

    @Test
    void justAboveThreshold_producesBloom() {
        List<Integer> values = new ArrayList<>();
        IntStream.rangeClosed(1, 1001).forEach(values::add);

        Optional<RuntimeFilter> result = generator.generate("col", values, PangrovaType.INTEGER);

        assertThat(result).isPresent();
        assertThat(result.get().type()).isEqualTo(RuntimeFilter.FilterType.BLOOM);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void minMaxBounds_coverAllValues() {
        List<Integer> values = List.of(10, 3, 7, 15, 1, 20);
        RuntimeFilter filter = generator.generate("v", values, PangrovaType.INTEGER).orElseThrow();

        Comparable min = filter.min().orElse(null);
        Comparable max = filter.max().orElse(null);
        for (Integer v : values) {
            assertThat(min.compareTo(v)).isLessThanOrEqualTo(0);
            assertThat(max.compareTo(v)).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void duplicateValues_ndvCountedCorrectly() {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            IntStream.rangeClosed(1, 500).forEach(values::add);
        }

        Optional<RuntimeFilter> result = generator.generate("col", values, PangrovaType.INTEGER);

        assertThat(result).isPresent();
        assertThat(result.get().type()).isEqualTo(RuntimeFilter.FilterType.MIN_MAX);
    }

    @Test
    void bloomFilter_containsBuildSideValues() {
        List<Integer> values = new ArrayList<>();
        IntStream.rangeClosed(1, 1500).forEach(values::add);

        RuntimeFilter filter = generator.generate("key", values, PangrovaType.INTEGER).orElseThrow();

        // SBBF: no false negatives — all inserted values must return true.
        // INTEGER values are encoded as little-endian 8-byte longs (Parquet wire format)
        // per RuntimeFilterGenerator.encodeForBloom — see Task 1.1.
        for (int v = 1; v <= 1500; v++) {
            byte[] encoded = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array();
            assertThat(filter.bloomFilter().mightContainBytes(encoded)).isTrue();
        }
    }
}
