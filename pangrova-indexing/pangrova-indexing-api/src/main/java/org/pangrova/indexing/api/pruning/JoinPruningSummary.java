package org.pangrova.indexing.api.pruning;

/**
 * Build-side value summary for JOIN pruning, reported by workers after
 * hash join build phase completes.
 *
 * @param joinColumn    column used in the JOIN condition
 * @param buildSideMin  minimum value on the build side
 * @param buildSideMax  maximum value on the build side
 * @param buildSideNdv  approximate number of distinct values on the build side
 */
public record JoinPruningSummary(
        String joinColumn,
        Comparable<?> buildSideMin,
        Comparable<?> buildSideMax,
        long buildSideNdv) {}
