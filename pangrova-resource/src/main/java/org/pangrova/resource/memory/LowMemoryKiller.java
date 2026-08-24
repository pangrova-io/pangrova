package org.pangrova.resource.memory;

import java.util.List;
import java.util.Optional;

/**
 * Strategy interface for selecting a query to terminate when node
 * memory is critically low.
 * <p>
 * Implementations decide which query to kill based on memory usage,
 * priority, or other heuristics.
 */
public interface LowMemoryKiller {

    /**
     * Selects a query pool to terminate in order to reclaim memory.
     *
     * @param candidates active query pools on the node, ordered by
     *                   descending used bytes
     * @return the query pool to kill, or empty if no candidate is suitable
     */
    Optional<QueryMemoryPool> chooseQueryToKill(List<QueryMemoryPool> candidates);
}
