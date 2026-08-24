package org.pangrova.planner.runtimefilter;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.spi.connector.metadata.DynamicFilter;

import java.util.Optional;

/**
 * Runtime filter generated from build-side join key values.
 * Implements {@link DynamicFilter} SPI for connector push-down.
 *
 * <p>BLOOM filters use Split Block Bloom Filter (SBBF) — Parquet-compatible,
 * cache-friendly probabilistic membership testing.</p>
 */
public final class RuntimeFilter implements DynamicFilter {

    private final FilterType type;
    private final String column;
    private final Comparable<?> min;
    private final Comparable<?> max;
    private final SplitBlockBloomFilter bloomFilter;

    private RuntimeFilter(FilterType type, String column,
                          Comparable<?> min, Comparable<?> max,
                          SplitBlockBloomFilter bloomFilter) {
        this.type = type;
        this.column = column;
        this.min = min;
        this.max = max;
        this.bloomFilter = bloomFilter;
    }

    public static RuntimeFilter minMax(String column, Comparable<?> min, Comparable<?> max) {
        return new RuntimeFilter(FilterType.MIN_MAX, column, min, max, null);
    }

    public static RuntimeFilter bloom(String column, SplitBlockBloomFilter filter) {
        return new RuntimeFilter(FilterType.BLOOM, column, null, null, filter);
    }

    public enum FilterType { MIN_MAX, BLOOM }

    public FilterType type() { return type; }

    @Override
    public DynamicFilter.Type filterType() {
        return type == FilterType.BLOOM ? DynamicFilter.Type.BLOOM : DynamicFilter.Type.MIN_MAX;
    }

    @Override
    public String column() { return column; }

    @Override
    public Optional<Comparable<?>> min() { return Optional.ofNullable(min); }

    @Override
    public Optional<Comparable<?>> max() { return Optional.ofNullable(max); }

    @Override
    @SuppressWarnings("unchecked")
    public boolean mightContain(Object value) {
        if (value == null) {
            return true;
        }
        if (type == FilterType.BLOOM && bloomFilter != null) {
            return bloomFilter.mightContain(value.toString());
        }
        if (type == FilterType.MIN_MAX && min != null && max != null) {
            Comparable<Object> cMin = (Comparable<Object>) min;
            Comparable<Object> cMax = (Comparable<Object>) max;
            Comparable<Object> cVal = (Comparable<Object>) value;
            return cVal.compareTo(cMin) >= 0 && cVal.compareTo(cMax) <= 0;
        }
        return true;
    }

    /** Returns the raw min value (package-visible for DPP internals). */
    public Comparable<?> rawMin() { return min; }

    /** Returns the raw max value (package-visible for DPP internals). */
    public Comparable<?> rawMax() { return max; }

    /** Returns the SBBF bloom filter, or null for non-BLOOM filters. */
    public SplitBlockBloomFilter bloomFilter() { return bloomFilter; }
}
