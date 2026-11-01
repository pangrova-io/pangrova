package org.pangrova.indexing.engine.sparse.provider;

import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.IndexStrategyProvider;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.IndexData;
import org.pangrova.spi.indexing.data.spatial.SparseEntry;
import org.pangrova.spi.indexing.data.spatial.SparseIndexData;
import org.pangrova.spi.connector.index.predicate.IndexPredicate;
import org.pangrova.spi.connector.index.predicate.RangeQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Sparse index strategy: samples every N-th row for range query narrowing.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code sparse.stride} — sampling interval (default 1000)</li>
 * </ul>
 */
public final class SparseStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_STRIDE = "sparse.stride";
    private static final int DEFAULT_STRIDE = 1000;

    @Override
    public IndexType supportedType() {
        return IndexType.SPARSE;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        int stride = intProp(properties, PROP_STRIDE, DEFAULT_STRIDE);
        List<SparseEntry> entries = new ArrayList<>();
        long rowOffset = 0;

        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (rowOffset % stride == 0 && !row.isEmpty() && row.get(0) != null) {
                entries.add(new SparseEntry(row.get(0), rowOffset));
            }
            rowOffset++;
        }
        return new SparseIndexData(entries, stride);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof SparseIndexData sparse)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (!(predicate instanceof RangeQuery range)) {
            return IndexEvalResult.UNKNOWN;
        }
        List<SparseEntry> entries = sparse.entries();
        if (entries.isEmpty()) {
            return IndexEvalResult.UNKNOWN;
        }
        return evaluateSparseRange(entries, range);
    }

    private static IndexEvalResult evaluateSparseRange(List<SparseEntry> entries, RangeQuery range) {
        int startIdx = resolveStartIndex(entries, range);
        int endIdx = resolveEndIndex(entries, range);
        return scanEntriesForRange(entries, range, startIdx, endIdx);
    }

    private static int resolveStartIndex(List<SparseEntry> entries, RangeQuery range) {
        if (range.lower() == null) {
            return 0;
        }
        int idx = lowerBound(entries, range.lower());
        return idx > 0 ? idx - 1 : idx;
    }

    private static int resolveEndIndex(List<SparseEntry> entries, RangeQuery range) {
        if (range.upper() == null) {
            return entries.size() - 1;
        }
        int idx = upperBound(entries, range.upper());
        return idx < entries.size() - 1 ? idx + 1 : idx;
    }

    private static IndexEvalResult scanEntriesForRange(List<SparseEntry> entries, RangeQuery range,
                                                       int startIdx, int endIdx) {
        for (int i = startIdx; i <= endIdx && i < entries.size(); i++) {
            if (entryInRange(entries.get(i), range)) {
                return IndexEvalResult.KEEP;
            }
            if (gapOverlapsRange(entries, range, i, endIdx)) {
                return IndexEvalResult.KEEP;
            }
        }
        return IndexEvalResult.SKIP;
    }

    private static boolean gapOverlapsRange(List<SparseEntry> entries, RangeQuery range,
                                            int i, int endIdx) {
        return i < entries.size() - 1 && i + 1 <= endIdx
                && rangeOverlapsInterval(range, entries.get(i).key(), entries.get(i + 1).key());
    }

    /**
     * Returns the index of the first entry whose key >= target.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int lowerBound(List<SparseEntry> entries, Comparable<?> target) {
        int lo = 0;
        int hi = entries.size();
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            int cmp = ((Comparable) entries.get(mid).key()).compareTo(target);
            if (cmp < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Returns the index of the last entry whose key <= target.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int upperBound(List<SparseEntry> entries, Comparable<?> target) {
        int lo = 0;
        int hi = entries.size() - 1;
        int result = -1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            int cmp = ((Comparable) entries.get(mid).key()).compareTo(target);
            if (cmp <= 0) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result >= 0 ? result : 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean entryInRange(SparseEntry entry, RangeQuery range) {
        return !isEntryBelowLower(entry, range) && !isEntryAboveUpper(entry, range);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isEntryBelowLower(SparseEntry entry, RangeQuery range) {
        if (range.lower() == null) {
            return false;
        }
        int cmp = ((Comparable) entry.key()).compareTo(range.lower());
        return range.lowerInclusive() ? cmp < 0 : cmp <= 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isEntryAboveUpper(SparseEntry entry, RangeQuery range) {
        if (range.upper() == null) {
            return false;
        }
        int cmp = ((Comparable) entry.key()).compareTo(range.upper());
        return range.upperInclusive() ? cmp > 0 : cmp >= 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean rangeOverlapsInterval(
            RangeQuery range, Comparable<?> intervalLo, Comparable<?> intervalHi) {
        return !isIntervalAboveUpper(range, intervalLo)
                && !isIntervalBelowLower(range, intervalHi);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isIntervalAboveUpper(RangeQuery range, Comparable<?> intervalLo) {
        if (range.upper() == null) {
            return false;
        }
        int cmp = ((Comparable) intervalLo).compareTo(range.upper());
        return range.upperInclusive() ? cmp > 0 : cmp >= 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isIntervalBelowLower(RangeQuery range, Comparable<?> intervalHi) {
        if (range.lower() == null) {
            return false;
        }
        int cmp = ((Comparable) intervalHi).compareTo(range.lower());
        return range.lowerInclusive() ? cmp < 0 : cmp <= 0;
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }
}
