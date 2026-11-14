package org.pangrova.planner.runtimefilter;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.types.enums.PangrovaType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
    private final PangrovaType valueType;

    private RuntimeFilter(FilterType type, String column,
                          Comparable<?> min, Comparable<?> max,
                          SplitBlockBloomFilter bloomFilter,
                          PangrovaType valueType) {
        this.type = type;
        this.column = column;
        this.min = min;
        this.max = max;
        this.bloomFilter = bloomFilter;
        this.valueType = valueType;
    }

    public static RuntimeFilter minMax(String column, Comparable<?> min, Comparable<?> max) {
        return minMax(column, min, max, inferType(min));
    }

    public static RuntimeFilter minMax(String column, Comparable<?> min, Comparable<?> max,
                                       PangrovaType valueType) {
        return new RuntimeFilter(FilterType.MIN_MAX, column, min, max, null, valueType);
    }

    public static RuntimeFilter bloom(String column, SplitBlockBloomFilter filter) {
        return bloom(column, filter, PangrovaType.VARCHAR);
    }

    public static RuntimeFilter bloom(String column, SplitBlockBloomFilter filter,
                                      PangrovaType valueType) {
        return new RuntimeFilter(FilterType.BLOOM, column, null, null, filter, valueType);
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
            return bloomFilter.mightContainBytes(encodeBloomValue(value, valueType));
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

    /** Declared value type used for lossless cross-process filter encoding. */
    public PangrovaType valueType() { return valueType; }

    private static PangrovaType inferType(Object value) {
        if (value instanceof Byte) return PangrovaType.TINYINT;
        if (value instanceof Short) return PangrovaType.SMALLINT;
        if (value instanceof Integer) return PangrovaType.INTEGER;
        if (value instanceof Long) return PangrovaType.BIGINT;
        if (value instanceof Float) return PangrovaType.FLOAT;
        if (value instanceof Double) return PangrovaType.DOUBLE;
        if (value instanceof BigDecimal) return PangrovaType.DECIMAL;
        return PangrovaType.VARCHAR;
    }

    private static byte[] encodeBloomValue(Object value, PangrovaType valueType) {
        return switch (valueType) {
            case BIGINT, INTEGER, SMALLINT, TINYINT -> ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(((Number) value).longValue())
                    .array();
            case DOUBLE, FLOAT -> ByteBuffer.allocate(Double.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble(((Number) value).doubleValue())
                    .array();
            case DECIMAL -> ((BigDecimal) value).unscaledValue().toByteArray();
            default -> value.toString().getBytes(StandardCharsets.UTF_8);
        };
    }
}
