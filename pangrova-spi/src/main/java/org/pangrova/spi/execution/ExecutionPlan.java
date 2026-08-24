package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Internal;

import java.util.List;
import java.util.Map;

/**
 * Represents the distributed execution plan for a query.
 *
 * <p>An execution plan is a directed acyclic graph of {@link PlanFragment}s.
 * Each fragment can be executed independently on a worker node, with data
 * exchanged between fragments via shuffle operations. The plan also carries
 * split assignments that map plan nodes to the data splits they should
 * process.</p>
 */
@Internal
public interface ExecutionPlan {

    /**
     * Returns the root fragment of the execution plan.
     *
     * <p>The root fragment produces the final query results.</p>
     *
     * @return the root plan fragment, never null
     */
    PlanFragment getRootFragment();

    /**
     * Returns all fragments in the execution plan.
     *
     * <p>The returned list includes the root fragment and all intermediate
     * fragments, in no particular order.</p>
     *
     * @return an unmodifiable list of all plan fragments, never null
     */
    List<PlanFragment> getAllFragments();

    /**
     * Returns the mapping from plan node identifiers to their assigned data splits.
     *
     * @return an unmodifiable map of split assignments, never null
     */
    Map<PlanNodeId, Split> getSplitAssignments();
}
