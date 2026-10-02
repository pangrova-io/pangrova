package org.pangrova.planner.runtimefilter;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.spi.connector.data.type.PangrovaType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Generates runtime filters from build-side join key values.
 * NDV &gt; 1000 → SBBF bloom filter; NDV &lt;= 1000 → Min/Max range filter.
 *
 * <p>Bloom filter values are encoded using their Parquet wire-format byte
 * representation (little-endian integers, IEEE-754 doubles, UTF-8 strings)
 * to align with native {@code ParquetBloomFilter} which uses xxHash64 over
 * the same wire format.</p>
 */
public final class RuntimeFilterGenerator {

    private static final int BLOOM_THRESHOLD = 1000;
    private static final double BLOOM_FPP = 0.03;

    /**
     * Generate a runtime filter from build-side join key values.
     *
     * @param column          join key column name
     * @param buildSideValues values collected from the build side
     * @param columnType      declared column type for type-aware binary encoding
     * @return filter, or empty if input is empty
     */
    public Optional<RuntimeFilter> generate(String column,
                                            List<? extends Comparable<?>> buildSideValues,
                                            PangrovaType columnType) {
        if (buildSideValues == null || buildSideValues.isEmpty()) {
            return Optional.empty();
        }

        Set<Comparable<?>> distinct = new HashSet<>(buildSideValues);
        return distinct.size() > BLOOM_THRESHOLD
                ? Optional.of(createBloomFilter(column, distinct, columnType))
                : Optional.of(createMinMaxFilter(column, buildSideValues, columnType));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RuntimeFilter createMinMaxFilter(String column,
                                             List<? extends Comparable<?>> values,
                                             PangrovaType columnType) {
        Comparable min = Collections.min((List) values);
        Comparable max = Collections.max((List) values);
        return RuntimeFilter.minMax(column, min, max, columnType);
    }

    private RuntimeFilter createBloomFilter(String column,
                                            Set<Comparable<?>> distinct,
                                            PangrovaType columnType) {
        SplitBlockBloomFilter.Builder builder =
                SplitBlockBloomFilter.builder(distinct.size(), BLOOM_FPP);
        for (Comparable<?> v : distinct) {
            byte[] encoded = encodeForBloom(v, columnType);
            if (encoded.length > 0) {
                builder.putBytes(encoded);
            }
        }
        return RuntimeFilter.bloom(column, builder.build(), columnType);
    }

    /**
     * Encode a value to its little-endian binary representation based on the
     * declared column type. The resulting bytes are compatible with native
     * xxHash64 probing in ParquetBloomFilter, ensuring cross-process bloom
     * filter hit rates are correct.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>BIGINT/INTEGER/SMALLINT/TINYINT → LE 8-byte int64</li>
     *   <li>DOUBLE/FLOAT → LE 8-byte IEEE-754 double</li>
     *   <li>VARCHAR → UTF-8 bytes</li>
     *   <li>DECIMAL → unscaled value two's-complement bytes</li>
     *   <li>default → toString().getBytes(UTF-8)</li>
     * </ul>
     *
     * @param value      the build-side value to encode
     * @param columnType the declared column type from the query plan
     * @return binary encoding suitable for bloom filter insertion
     */
    byte[] encodeForBloom(Object value, PangrovaType columnType) {
        if (value == null) {
            return new byte[0];
        }
        return switch (columnType) {
            case BIGINT, INTEGER, SMALLINT, TINYINT -> ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(((Number) value).longValue())
                    .array();
            case DOUBLE, FLOAT -> ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble(((Number) value).doubleValue())
                    .array();
            case VARCHAR -> ((String) value).getBytes(StandardCharsets.UTF_8);
            case DECIMAL -> ((BigDecimal) value).unscaledValue().toByteArray();
            default -> value.toString().getBytes(StandardCharsets.UTF_8);
        };
    }
}
