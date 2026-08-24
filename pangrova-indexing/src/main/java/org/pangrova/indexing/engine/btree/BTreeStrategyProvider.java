package org.pangrova.indexing.engine.btree;

import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.IndexStrategyProvider;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.BTreeIndexData;
import org.pangrova.spi.index.data.BTreePage;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.predicate.IndexPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * BTree index strategy: sorted keys in leaf pages for range and equality queries.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code btree.page_size} — max keys per page (default 4096)</li>
 *   <li>{@code btree.store_positions} — store row positions (default false)</li>
 * </ul>
 */
public final class BTreeStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_PAGE_SIZE = "btree.page_size";
    private static final String PROP_STORE_POSITIONS = "btree.store_positions";
    private static final int DEFAULT_PAGE_SIZE = 4096;

    @Override
    public IndexType supportedType() {
        return IndexType.BTREE;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        int pageSize = intProp(properties, PROP_PAGE_SIZE, DEFAULT_PAGE_SIZE);
        boolean storePositions = boolProp(properties, PROP_STORE_POSITIONS, false);

        List<Comparable<?>> allKeys = new ArrayList<>();
        List<Integer> allRowIndices = new ArrayList<>();
        int rowIndex = 0;
        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            Comparable<?> key = extractKey(row);
            if (key != null) {
                allKeys.add(key);
                allRowIndices.add(rowIndex);
            }
            rowIndex++;
        }
        sortParallel(allKeys, allRowIndices);
        List<BTreePage> pages = buildPages(allKeys, allRowIndices, pageSize, storePositions);
        return new BTreeIndexData(pages, storePositions);
    }

    /**
     * Extracts a key from a row. Single-column rows produce a plain Comparable;
     * multi-column rows produce a CompositeKey with lexicographic ordering.
     * Returns null if any component is null (nulls not indexed in BTree).
     */
    private static Comparable<?> extractKey(List<Comparable<?>> row) {
        if (row.isEmpty()) {
            return null;
        }
        if (row.size() == 1) {
            return row.get(0); // null check: null means skip
        }
        // Multi-column: all components must be non-null
        for (Comparable<?> component : row) {
            if (component == null) {
                return null;
            }
        }
        return new CompositeKey(row);
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof BTreeIndexData btree)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (btree.leafPages().isEmpty()) {
            return IndexEvalResult.SKIP;
        }
        return BTreeSearcher.evaluate(btree, predicate);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void sortParallel(List<Comparable<?>> keys, List<Integer> indices) {
        int n = keys.size();
        if (n <= 1) return;
        Comparable<?>[] k = keys.toArray(Comparable[]::new);
        Integer[] order = new Integer[n];
        Arrays.setAll(order, i -> i);
        Arrays.parallelSort(order, (a, b) -> ((Comparable) k[a]).compareTo(k[b]));
        Comparable<?>[] sortedKeys = new Comparable[n];
        int[] sortedIndices = new int[n];
        for (int i = 0; i < n; i++) {
            sortedKeys[i] = k[order[i]];
            sortedIndices[i] = indices.get(order[i]);
        }
        for (int i = 0; i < n; i++) {
            keys.set(i, sortedKeys[i]);
            indices.set(i, sortedIndices[i]);
        }
    }

    private static List<BTreePage> buildPages(
            List<Comparable<?>> sortedKeys, List<Integer> sortedIndices,
            int pageSize, boolean storePositions) {
        List<BTreePage> pages = new ArrayList<>();
        int n = sortedKeys.size();
        for (int start = 0; start < n; start += pageSize) {
            int end = Math.min(start + pageSize, n);
            List<Comparable<?>> pageKeys = List.copyOf(sortedKeys.subList(start, end));
            Comparable<?> minKey = pageKeys.get(0);
            Comparable<?> maxKey = pageKeys.get(pageKeys.size() - 1);
            List<List<Integer>> rowPositions = null;
            if (storePositions) {
                rowPositions = new ArrayList<>();
                for (int i = start; i < end; i++) {
                    rowPositions.add(List.of(sortedIndices.get(i)));
                }
            }
            pages.add(new BTreePage(pageKeys, rowPositions, minKey, maxKey));
        }
        return pages;
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }

    private static boolean boolProp(Map<String, String> props, String key, boolean defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Boolean.parseBoolean(val);
    }
}
