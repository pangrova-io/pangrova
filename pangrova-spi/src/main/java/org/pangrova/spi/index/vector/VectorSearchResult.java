package org.pangrova.spi.index.vector;

import java.util.List;

/**
 * Result of an ANN (Approximate Nearest Neighbor) search on a single data file.
 *
 * <p>Contains the data file path and a list of scored neighbor rows, sorted
 * by distance ascending (lower distance = more similar). The {@code neighbors}
 * list is defensively copied via {@link List#copyOf(java.util.Collection)}.</p>
 *
 * @param dataFilePath path of the data file that was searched
 * @param neighbors    scored neighbor rows (defensively copied, immutable)
 * @param indexFound   true if a vector index was found and used; false if no index exists
 */
public record VectorSearchResult(String dataFilePath, List<ScoredRow> neighbors, boolean indexFound) {

    public VectorSearchResult(String dataFilePath, List<ScoredRow> neighbors) {
        this(dataFilePath, neighbors, true);
    }

    public VectorSearchResult {
        neighbors = List.copyOf(neighbors);
    }

    /**
     * A single neighbor row with its distance score.
     *
     * <p>Implements {@link Comparable} using {@link Double#compare} on
     * {@code distance}, providing a total ordering consistent with
     * {@code equals} for merge-heap correctness.</p>
     *
     * @param rowOffset byte offset of the row in the data file
     * @param distance  distance from the query vector (lower = more similar)
     */
    public record ScoredRow(long rowOffset, double distance) implements Comparable<ScoredRow> {

        @Override
        public int compareTo(ScoredRow other) {
            return Double.compare(this.distance, other.distance);
        }
    }
}
