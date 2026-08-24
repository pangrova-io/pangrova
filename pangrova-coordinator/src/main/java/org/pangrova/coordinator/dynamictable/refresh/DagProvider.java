package org.pangrova.coordinator.dynamictable.refresh;

import java.util.Set;

/**
 * Provides DAG topology for Dynamic Table cascade refresh.
 */
@FunctionalInterface
public interface DagProvider {

    /**
     * Return the set of direct downstream Dynamic Table names
     * that depend on the given upstream.
     *
     * @param dtName upstream Dynamic Table name
     * @return set of direct downstream DT names (never null)
     */
    Set<String> getDirectDownstreams(String dtName);
}
