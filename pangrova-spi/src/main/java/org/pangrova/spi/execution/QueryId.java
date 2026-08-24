package org.pangrova.spi.execution;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable identifier for a query within the Pangrova engine.
 *
 * @param id the unique query identifier string, must not be null
 */
public record QueryId(String id) {

    /**
     * Creates a new query identifier.
     *
     * @param id the unique identifier string
     * @throws NullPointerException if id is null
     */
    public QueryId {
        checkNotNull(id, "id must not be null");
    }

    @Override
    public String toString() {
        return id;
    }
}
