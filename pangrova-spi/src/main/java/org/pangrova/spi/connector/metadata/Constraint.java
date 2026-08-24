package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;

import org.pangrova.spi.connector.metadata.table.ColumnHandle;

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
