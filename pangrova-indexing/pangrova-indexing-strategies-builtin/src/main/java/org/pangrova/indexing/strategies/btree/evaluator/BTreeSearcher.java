package org.pangrova.indexing.strategies.btree.evaluator;

import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.indexing.api.data.BTreeIndexData;
import org.pangrova.indexing.api.data.BTreePage;
import org.pangrova.indexing.api.data.CompositeIndexKey;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.index.predicate.IsNotNull;
import org.pangrova.connector.api.index.predicate.IsNull;
import org.pangrova.connector.api.index.predicate.RangeQuery;

import java.util.List;

/**
 * Stateless search operations on BTree index data.
 */
public final class BTreeSearcher {

    private BTreeSearcher() {}

    /**
     * Evaluates a predicate against BTree index data without touching row values.
     *
     * @param btree     BTree index data to search
     * @param predicate predicate to evaluate
     * @return KEEP, SKIP, or UNKNOWN for the indexed column
     */
    public static IndexEvalResult evaluate(BTreeIndexData btree, IndexPredicate predicate) {
        // The payload has no null-row/arity coverage metadata. An empty key set
        // cannot prove a predicate on omitted (possibly partial-null) rows absent.
        if (btree.leafPages().isEmpty()) return IndexEvalResult.UNKNOWN;
        Comparable<?> prototype = btree.leafPages().get(0).minKey();
        if (predicate instanceof EqualTo eq) {
            if (!comparableKeys(prototype, eq.value())) return IndexEvalResult.UNKNOWN;
            return evaluateEqual(btree, eq.value());
        }
        if (predicate instanceof RangeQuery range) {
            if ((range.lower() != null && !comparableKeys(prototype, range.lower()))
                    || (range.upper() != null && !comparableKeys(prototype, range.upper()))) {
                return IndexEvalResult.UNKNOWN;
            }
            return evaluateRange(btree, range);
        }
        if (predicate instanceof InList inList) {
            if (inList.values().stream().anyMatch(value -> !comparableKeys(prototype, value))) {
                return IndexEvalResult.UNKNOWN;
            }
            return evaluateInList(btree, inList.values());
        }
        return evaluateNullPredicate(btree, predicate);
    }

    /** Type compatibility only; callers still own column binding and coverage. */
    public static boolean comparableKeys(Comparable<?> left, Comparable<?> right) {
        if (left == null || right == null || left.getClass() != right.getClass()) return false;
        return !(left instanceof CompositeIndexKey key) || key.hasSameShape((CompositeIndexKey) right);
    }

    private static IndexEvalResult evaluateNullPredicate(BTreeIndexData btree, IndexPredicate predicate) {
        if (predicate instanceof IsNull) {
            return IndexEvalResult.UNKNOWN;
        }
        if (predicate instanceof IsNotNull) {
            return btree.leafPages().isEmpty() ? IndexEvalResult.SKIP : IndexEvalResult.KEEP;
        }
        return IndexEvalResult.UNKNOWN;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static IndexEvalResult evaluateEqual(BTreeIndexData btree, Comparable<?> value) {
        List<BTreePage> pages = btree.leafPages();
        int pageIndex = findPageByBinarySearch(pages, value);
        if (pageIndex < 0) {
            return IndexEvalResult.SKIP;
        }
        // Check the found page and its neighbors (value may straddle page boundary)
        for (int i = Math.max(0, pageIndex - 1); i <= Math.min(pages.size() - 1, pageIndex + 1); i++) {
            BTreePage page = pages.get(i);
            int cmpMin = ((Comparable) value).compareTo(page.minKey());
            int cmpMax = ((Comparable) value).compareTo(page.maxKey());
            if (cmpMin >= 0 && cmpMax <= 0) {
                if (binarySearchKeys(page.keys(), value)) {
                    return IndexEvalResult.KEEP;
                }
            }
        }
        return IndexEvalResult.SKIP;
    }

    /**
     * Binary search for the leaf page whose range contains the target value.
     * Returns the index of the best candidate page, or -1 if the value is
     * outside all page ranges.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int findPageByBinarySearch(List<BTreePage> pages, Comparable<?> value) {
        if (pages.isEmpty()) {
            return -1;
        }
        int lo = 0;
        int hi = pages.size() - 1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            BTreePage page = pages.get(mid);
            int cmpMin = ((Comparable) value).compareTo(page.minKey());
            int cmpMax = ((Comparable) value).compareTo(page.maxKey());
            if (cmpMin >= 0 && cmpMax <= 0) {
                return mid; // value within this page's range
            }
            if (cmpMin < 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return -1;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static IndexEvalResult evaluateRange(BTreeIndexData btree, RangeQuery range) {
        List<BTreePage> pages = btree.leafPages();
        if (pages.isEmpty()) {
            return IndexEvalResult.SKIP;
        }
        int startIdx = resolveRangeStart(pages, range);
        if (startIdx < 0) {
            return IndexEvalResult.SKIP;
        }
        return scanPagesForRange(pages, range, startIdx);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int resolveRangeStart(List<BTreePage> pages, RangeQuery range) {
        if (range.lower() == null) {
            return 0;
        }
        return findFirstPageOverlappingLower(pages, range.lower(), range.lowerInclusive());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static IndexEvalResult scanPagesForRange(List<BTreePage> pages, RangeQuery range, int startIdx) {
        for (int i = startIdx; i < pages.size(); i++) {
            BTreePage page = pages.get(i);
            if (isPageBeyondUpperBound(page, range)) {
                break;
            }
            if (pageOverlapsRange(page, range)) {
                return IndexEvalResult.KEEP;
            }
        }
        return IndexEvalResult.SKIP;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isPageBeyondUpperBound(BTreePage page, RangeQuery range) {
        if (range.upper() == null) {
            return false;
        }
        int cmp = ((Comparable) page.minKey()).compareTo(range.upper());
        return range.upperInclusive() ? cmp > 0 : cmp >= 0;
    }

    /**
     * Binary search for the first leaf page whose maxKey is >= lower bound.
     * Returns -1 if no page qualifies.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int findFirstPageOverlappingLower(
            List<BTreePage> pages, Comparable<?> lower, boolean inclusive) {
        int lo = 0;
        int hi = pages.size() - 1;
        int result = -1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            int cmp = ((Comparable) pages.get(mid).maxKey()).compareTo(lower);
            if (inclusive ? cmp >= 0 : cmp > 0) {
                result = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return result;
    }

    private static IndexEvalResult evaluateInList(BTreeIndexData btree, List<Comparable<?>> values) {
        for (Comparable<?> val : values) {
            if (evaluateEqual(btree, val) == IndexEvalResult.KEEP) {
                return IndexEvalResult.KEEP;
            }
        }
        return IndexEvalResult.SKIP;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean pageOverlapsRange(BTreePage page, RangeQuery range) {
        return !isExcludedByUpperBound(page, range)
                && !isExcludedByLowerBound(page, range);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isExcludedByUpperBound(BTreePage page, RangeQuery range) {
        if (range.upper() == null) {
            return false;
        }
        int cmp = ((Comparable) page.minKey()).compareTo(range.upper());
        return range.upperInclusive() ? cmp > 0 : cmp >= 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean isExcludedByLowerBound(BTreePage page, RangeQuery range) {
        if (range.lower() == null) {
            return false;
        }
        int cmp = ((Comparable) page.maxKey()).compareTo(range.lower());
        return range.lowerInclusive() ? cmp < 0 : cmp <= 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean binarySearchKeys(List<Comparable<?>> keys, Comparable<?> target) {
        int lo = 0;
        int hi = keys.size() - 1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            int cmp = ((Comparable) keys.get(mid)).compareTo(target);
            if (cmp == 0) return true;
            if (cmp < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return false;
    }
}
