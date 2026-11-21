package org.pangrova.protocol.execution;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/**
 * Represents a fragment of a distributed execution plan.
 *
 * <p>A plan fragment is a portion of the overall query plan that can be
 * executed independently on a single node. Fragments are connected by
 * exchange operators that shuffle data between nodes.</p>
 */
@Internal
public interface PlanFragment {

    /**
     * Returns the unique identifier for this fragment's root plan node.
     *
     * @return the plan node id, never null
     */
    PlanNodeId getId();

    /**
     * Returns the identifiers of child fragments that feed data into this fragment.
     *
     * @return an unmodifiable list of child fragment plan node ids, never null
     */
    List<PlanNodeId> getChildFragmentIds();
}
