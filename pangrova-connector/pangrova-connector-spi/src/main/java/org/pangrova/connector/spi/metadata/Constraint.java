package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;

import org.pangrova.connector.spi.metadata.table.ColumnHandle;

/**
 * Represents pushed-down predicate constraints for connector-level filtering.
 *
 * <p>Constraints are derived from query predicates and allow connectors to
 * filter data at the source (partition pruning, file skipping, etc.).</p>
 */
@Experimental
public interface Constraint {

    /**
     * Returns per-column domain constraints.
     *
     * @return column handle to domain mapping, empty if no constraints
     */
    Map<ColumnHandle, Domain> getDomains();

    /** Empty constraint — no filtering. */
    Constraint EMPTY = Map::of;
}
