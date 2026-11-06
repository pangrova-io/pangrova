package org.pangrova.indexing.builtin.bloom.provider;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.indexing.builtin.normalize.IndexValueNormalizer;
import org.pangrova.indexing.builtin.bloom.data.CachedBloomFilterIndexData;
import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.indexing.spi.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IndexPredicate;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Bloom filter index strategy using Split Block Bloom Filter (SBBF).
 *
 * <p>Supports EqualTo and InList predicates with probabilistic membership testing.
 * No false negatives: inserted values always return KEEP.</p>
 */
public final class BloomFilterStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_EXPECTED_ENTRIES = "bloom.expected_entries";
    private static final String PROP_FPP = "bloom.fpp";
    private static final int DEFAULT_EXPECTED_ENTRIES = 10_000;
    private static final double DEFAULT_FPP = 0.01;

    @Override
    public IndexType supportedType() {
        return IndexType.BLOOM_FILTER;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        int expectedEntries = intProp(properties, PROP_EXPECTED_ENTRIES, DEFAULT_EXPECTED_ENTRIES);
        double fpp = doubleProp(properties, PROP_FPP, DEFAULT_FPP);

        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(expectedEntries, fpp);
        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (!row.isEmpty() && row.get(0) != null) {
                builder.put(IndexValueNormalizer.toCanonicalString(row.get(0)));
            }
        }
        SplitBlockBloomFilter filter = builder.build();
        byte[] serialized = filter.toByteArray();
        return new BloomFilterIndexData(serialized, 8, (long) filter.numBlocks() * 256);
    }

    @Override
    public IndexData prepareForEvaluation(IndexData data) {
        if (data instanceof CachedBloomFilterIndexData) {
            return data;
        }
        if (data instanceof BloomFilterIndexData bloom) {
            return new CachedBloomFilterIndexData(bloom, SplitBlockBloomFilter.readBuffer(bloom.filterDataView()));
        }
        throw new IllegalArgumentException("Expected Bloom filter index data");
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        SplitBlockBloomFilter filter = extractFilter(indexData);
        if (filter == null) {
            return IndexEvalResult.UNKNOWN;
        }
        if (predicate instanceof EqualTo eq) {
            return filter.mightContain(IndexValueNormalizer.toCanonicalString(eq.value()))
                    ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
        }
        if (predicate instanceof InList inList) {
            return evaluateInList(filter, inList);
        }
        return IndexEvalResult.UNKNOWN;
    }

    private static IndexEvalResult evaluateInList(SplitBlockBloomFilter filter, InList inList) {
        for (Comparable<?> val : inList.values()) {
            if (filter.mightContain(IndexValueNormalizer.toCanonicalString(val))) {
                return IndexEvalResult.KEEP;
            }
        }
        return IndexEvalResult.SKIP;
    }

    /**
     * Extracts the deserialized filter, preferring the cached version from
     * {@link CachedBloomFilterIndexData} to avoid per-evaluation deserialization.
     */
    private static SplitBlockBloomFilter extractFilter(IndexData indexData) {
        if (indexData instanceof CachedBloomFilterIndexData cached) {
            return cached.filter();
        }
        if (indexData instanceof BloomFilterIndexData bloom) {
            return SplitBlockBloomFilter.readBuffer(bloom.filterDataView());
        }
        return null;
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }

    private static double doubleProp(Map<String, String> props, String key, double defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Double.parseDouble(val);
    }
}
