package org.pangrova.spi.connector.metadata.table;

import org.pangrova.spi.annotation.Experimental;

/**
 * Marker interface for connector-specific column handle.
 *
 * <p>Each connector implementation provides its own class implementing this interface
 * to represent a reference to a column in the underlying data source.</p>
 */
@Experimental
public interface ColumnHandle {

    /**
     * Returns the column name represented by this handle.
     *
     * <p>Used by the engine for predicate pushdown, constraint extraction,
     * and split filtering without resorting to reflection.</p>
     *
     * @return the column name, never null
     */
    String columnName();
}
