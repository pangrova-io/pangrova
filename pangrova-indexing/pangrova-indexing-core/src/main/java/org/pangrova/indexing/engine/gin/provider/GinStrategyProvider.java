package org.pangrova.indexing.engine.gin.provider;

import org.pangrova.indexing.engine.gin.serializer.ElementExtractor;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.IndexStrategyProvider;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.text.GinIndexData;
import org.pangrova.spi.indexing.data.IndexData;
import org.pangrova.spi.connector.index.predicate.ContainmentMode;
import org.pangrova.spi.connector.index.predicate.ContainsElement;
import org.pangrova.spi.connector.index.predicate.IndexPredicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * GIN (Generalized Inverted) index strategy for composite value containment queries.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code gin.store_positions} — store posting lists (default false)</li>
 * </ul>
 */
public final class GinStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_STORE_POSITIONS = "gin.store_positions";

    @Override
    public IndexType supportedType() {
        return IndexType.GIN;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        boolean storePositions = boolProp(properties, PROP_STORE_POSITIONS, false);
        Map<String, List<Integer>> elementPostings = new HashMap<>();
        int rowIndex = 0;

        while (values.hasNext()) {
            indexGinRow(values.next(), rowIndex, storePositions, elementPostings);
            rowIndex++;
        }

        if (!storePositions) {
            elementPostings = toElementSet(elementPostings);
        }
        return new GinIndexData(elementPostings, storePositions);
    }

    private static void indexGinRow(List<Comparable<?>> row, int rowIndex,
                                    boolean storePositions,
                                    Map<String, List<Integer>> elementPostings) {
        if (row.isEmpty() || row.get(0) == null) {
            return;
        }
        List<String> elements = ElementExtractor.extract(row.get(0).toString());
        for (String element : elements) {
            List<Integer> positions = elementPostings.computeIfAbsent(element, k -> new ArrayList<>());
            if (storePositions) {
                positions.add(rowIndex);
            }
        }
    }

    private static Map<String, List<Integer>> toElementSet(Map<String, List<Integer>> postings) {
        Map<String, List<Integer>> elementSet = new HashMap<>(postings.size());
        for (String element : postings.keySet()) {
            elementSet.put(element, List.of());
        }
        return elementSet;
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof GinIndexData gin)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (!(predicate instanceof ContainsElement contains)) {
            return IndexEvalResult.UNKNOWN;
        }
        return evaluateContainment(gin.elementPostings(), contains);
    }

    private static IndexEvalResult evaluateContainment(Map<String, List<Integer>> postings,
                                                       ContainsElement contains) {
        if (contains.mode() == ContainmentMode.CONTAINS_ANY) {
            return containsAny(postings, contains.elements())
                    ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
        }
        return containsAll(postings, contains.elements())
                ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
    }

    private static boolean containsAny(Map<String, List<Integer>> postings, List<Comparable<?>> elements) {
        for (Comparable<?> elem : elements) {
            if (postings.containsKey(elem.toString())) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAll(Map<String, List<Integer>> postings, List<Comparable<?>> elements) {
        for (Comparable<?> elem : elements) {
            if (!postings.containsKey(elem.toString())) {
                return false;
            }
        }
        return true;
    }

    private static boolean boolProp(Map<String, String> props, String key, boolean defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Boolean.parseBoolean(val);
    }
}
