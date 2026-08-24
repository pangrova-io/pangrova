package org.pangrova.indexing.engine.prefix;

import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.IndexStrategyProvider;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.data.text.PrefixEntry;
import org.pangrova.spi.index.data.text.PrefixIndexData;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.LikePattern;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Prefix index strategy for prefix-based string matching (LIKE 'abc%').
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code prefix.length} — prefix truncation length (default 8)</li>
 * </ul>
 */
public final class PrefixStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_PREFIX_LENGTH = "prefix.length";
    private static final int DEFAULT_PREFIX_LENGTH = 8;

    @Override
    public IndexType supportedType() {
        return IndexType.PREFIX;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        int prefixLength = intProp(properties, PROP_PREFIX_LENGTH, DEFAULT_PREFIX_LENGTH);
        TreeSet<PrefixEntry> sorted = new TreeSet<>(Comparator.comparing(PrefixEntry::prefix));
        long rowOffset = 0;

        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (!row.isEmpty() && row.get(0) != null) {
                String value = row.get(0).toString();
                String prefix = value.length() > prefixLength
                        ? value.substring(0, prefixLength) : value;
                sorted.add(new PrefixEntry(prefix, rowOffset));
            }
            rowOffset++;
        }
        return new PrefixIndexData(new ArrayList<>(sorted), prefixLength);
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof PrefixIndexData prefixData)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (!(predicate instanceof LikePattern like)) {
            return IndexEvalResult.UNKNOWN;
        }
        return evaluateLikePrefix(prefixData, like);
    }

    private static IndexEvalResult evaluateLikePrefix(PrefixIndexData prefixData, LikePattern like) {
        String pattern = like.pattern();
        if (!pattern.endsWith("%") || pattern.indexOf('%') != pattern.length() - 1) {
            return IndexEvalResult.UNKNOWN;
        }
        String searchPrefix = pattern.substring(0, pattern.length() - 1);
        int prefixLen = prefixData.prefixLength();
        String truncated = searchPrefix.length() > prefixLen
                ? searchPrefix.substring(0, prefixLen) : searchPrefix;
        return binarySearchPrefix(prefixData.entries(), truncated)
                ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
    }

    private static boolean binarySearchPrefix(List<PrefixEntry> entries, String truncated) {
        int lo = 0;
        int hi = entries.size() - 1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            String entryPrefix = entries.get(mid).prefix();
            if (entryPrefix.startsWith(truncated)) {
                return true;
            }
            if (entryPrefix.compareTo(truncated) < 0) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return false;
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }
}
