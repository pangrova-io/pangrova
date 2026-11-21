package org.pangrova.indexing.api.pruning;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Runtime boundary for Top-K pruning, reported by workers as the top-k heap fills.
 *
 * @param orderByColumn  column used in ORDER BY clause
 * @param boundaryValue  current boundary value from the top-k heap
 * @param descending     true if ORDER BY DESC
 * @param heapSize       current number of rows in the top-k heap
 */
@Experimental
public record TopKBoundary(
        String orderByColumn,
        Comparable<?> boundaryValue,
        boolean descending,
        long heapSize) {}
