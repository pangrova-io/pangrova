package org.pangrova.protocol.execution;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Immutable identifier for a node within an execution plan.
 *
 * @param id the unique plan node identifier string, must not be null
 */
public record PlanNodeId(String id) {

    /**
     * Creates a new plan node identifier.
     *
     * @param id the unique identifier string
     * @throws NullPointerException if id is null
     */
    public PlanNodeId {
        checkNotNull(id, "id must not be null");
    }

    @Override
    public String toString() {
        return id;
    }
}
