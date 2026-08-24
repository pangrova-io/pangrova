package org.pangrova.spi.index.data.text;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GIN (Generalized Inverted) index data: element to posting list mapping
 * for composite types (arrays, JSON, multi-valued columns).
 *
 * <p>When {@code storePositions} is false (default), elementPostings
 * contains element → empty list (element set only) for file-level skip.
 * When true, full posting lists (row positions) are stored.</p>
 *
 * @param elementPostings mapping from element value to row position list
 * @param storePositions  whether row positions are stored
 */
@Experimental
public record GinIndexData(
        Map<String, List<Integer>> elementPostings,
        boolean storePositions) implements IndexData {

    public GinIndexData {
        elementPostings = deepCopyPostings(elementPostings);
    }

    /**
     * Convenience constructor with storePositions defaulting to false.
     */
    public GinIndexData(Map<String, List<Integer>> elementPostings) {
        this(elementPostings, false);
    }

    @Override
    public IndexType indexType() {
        return IndexType.GIN;
    }

    @Override
    public long estimatedSizeInBytes() {
        if (!storePositions) {
            return elementPostings.keySet().stream()
                    .mapToLong(k -> k.length() * 2L + 32L).sum();
        }
        return elementPostings.entrySet().stream()
                .mapToLong(e -> e.getKey().length() * 2L + 32L
                        + e.getValue().size() * 4L)
                .sum();
    }

    private static Map<String, List<Integer>> deepCopyPostings(
            Map<String, List<Integer>> original) {
        if (original == null || original.isEmpty()) {
            return Map.of();
        }
        var copy = new LinkedHashMap<String, List<Integer>>(original.size());
        for (var entry : original.entrySet()) {
            copy.put(entry.getKey(),
                    entry.getValue() == null ? List.of() : List.copyOf(entry.getValue()));
        }
        return Map.copyOf(copy);
    }
}
