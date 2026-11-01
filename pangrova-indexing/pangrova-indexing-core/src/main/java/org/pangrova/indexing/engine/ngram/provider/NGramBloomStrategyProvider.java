package org.pangrova.indexing.engine.ngram.provider;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.indexing.engine.ngram.serializer.NGramTokenizer;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.IndexStrategyProvider;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.IndexData;
import org.pangrova.spi.indexing.data.text.NGramBloomIndexData;
import org.pangrova.spi.connector.index.predicate.IndexPredicate;
import org.pangrova.spi.connector.index.predicate.LikePattern;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * N-gram bloom filter strategy for substring (LIKE '%abc%') matching.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code ngram.size} — n-gram size (default 3)</li>
 *   <li>{@code ngram.expected_entries} — expected distinct n-grams (default 50000)</li>
 *   <li>{@code ngram.fpp} — false positive probability (default 0.01)</li>
 * </ul>
 */
public final class NGramBloomStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_NGRAM_SIZE = "ngram.size";
    private static final String PROP_EXPECTED_ENTRIES = "ngram.expected_entries";
    private static final String PROP_FPP = "ngram.fpp";
    private static final int DEFAULT_NGRAM_SIZE = 3;
    private static final int DEFAULT_EXPECTED_ENTRIES = 50_000;
    private static final double DEFAULT_FPP = 0.01;

    @Override
    public IndexType supportedType() {
        return IndexType.NGRAM_BLOOM_FILTER;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        int ngramSize = intProp(properties, PROP_NGRAM_SIZE, DEFAULT_NGRAM_SIZE);
        int expectedEntries = intProp(properties, PROP_EXPECTED_ENTRIES, DEFAULT_EXPECTED_ENTRIES);
        double fpp = doubleProp(properties, PROP_FPP, DEFAULT_FPP);

        SplitBlockBloomFilter.Builder builder = SplitBlockBloomFilter.builder(expectedEntries, fpp);
        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (!row.isEmpty() && row.get(0) != null) {
                List<String> ngrams = NGramTokenizer.tokenize(row.get(0).toString(), ngramSize);
                for (String ngram : ngrams) {
                    builder.put(ngram);
                }
            }
        }
        SplitBlockBloomFilter filter = builder.build();
        byte[] serialized = filter.toByteArray();
        return new NGramBloomIndexData(serialized, 8, (long) filter.numBlocks() * 256, ngramSize);
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof NGramBloomIndexData ngramData)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (!(predicate instanceof LikePattern like)) {
            return IndexEvalResult.UNKNOWN;
        }
        return evaluateLikeSubstring(ngramData, like);
    }

    private static IndexEvalResult evaluateLikeSubstring(NGramBloomIndexData ngramData, LikePattern like) {
        String substring = extractSubstring(like.pattern());
        if (substring == null || substring.length() < ngramData.ngramSize()) {
            return IndexEvalResult.UNKNOWN;
        }
        SplitBlockBloomFilter filter = SplitBlockBloomFilter.readFrom(ngramData.filterData());
        List<String> ngrams = NGramTokenizer.tokenize(substring, ngramData.ngramSize());
        return allNgramsPresent(filter, ngrams) ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
    }

    private static boolean allNgramsPresent(SplitBlockBloomFilter filter, List<String> ngrams) {
        for (String ngram : ngrams) {
            if (!filter.mightContain(ngram)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extracts the literal substring from a LIKE pattern.
     * Handles patterns like '%abc%', '%abc', 'abc%'.
     */
    private static String extractSubstring(String pattern) {
        String stripped = pattern;
        if (stripped.startsWith("%")) stripped = stripped.substring(1);
        if (stripped.endsWith("%")) stripped = stripped.substring(0, stripped.length() - 1);
        // If still contains wildcards, cannot use n-gram index
        if (stripped.contains("%") || stripped.contains("_")) {
            return null;
        }
        return stripped.isEmpty() ? null : stripped;
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
