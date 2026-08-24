package org.pangrova.spi.index.data.text;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Inverted index data: term to posting list mapping.
 *
 * <p>When {@code storePositions} is false (default), the postings map
 * contains term → empty list (term set only) for file-level skip.
 * When true, full posting lists (row positions) are stored.</p>
 *
 * @param postings       mapping from term to row position list
 * @param storePositions whether row positions are stored
 */
@Experimental
public record InvertedIndexData(
        Map<String, List<Integer>> postings,
        boolean storePositions) implements IndexData {

    public InvertedIndexData {
        postings = deepCopyPostings(postings);
    }

    /**
     * Convenience constructor with storePositions defaulting to false.
     */
    public InvertedIndexData(Map<String, List<Integer>> postings) {
        this(postings, false);
    }

    @Override
    public IndexType indexType() {
        return IndexType.INVERTED;
    }

    @Override
    public long estimatedSizeInBytes() {
        if (!storePositions) {
            return postings.keySet().stream()
                    .mapToLong(k -> k.length() * 2L + 32L).sum();
        }
        return postings.entrySet().stream()
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
