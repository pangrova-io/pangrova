package org.pangrova.spi.index.pruning;

import java.util.List;

/**
 * Result of LIMIT-aware pruning: identifies fully-matching files whose
 * combined row count satisfies the query's LIMIT k requirement.
 *
 * <p>When {@code canPrune} is true, the query engine may scan only
 * {@code fullyMatchingFiles} instead of all predicate-matching files.</p>
 */
public record LimitPruningResult(
        List<String> fullyMatchingFiles,
        long fullyMatchingRowCount,
        long limitK,
        boolean canPrune) {

    public LimitPruningResult {
        fullyMatchingFiles = List.copyOf(fullyMatchingFiles);
    }

    /** No LIMIT pruning possible — scan all matching files. */
    public static final LimitPruningResult NOT_APPLICABLE =
            new LimitPruningResult(List.of(), 0, 0, false);

    /**
     * Creates a result indicating LIMIT pruning is possible.
     */
    public static LimitPruningResult of(
            List<String> fullyMatchingFiles, long fullyMatchingRowCount, long limitK) {
        boolean canPrune = fullyMatchingRowCount >= limitK && limitK > 0;
        return new LimitPruningResult(fullyMatchingFiles, fullyMatchingRowCount, limitK, canPrune);
    }
}
