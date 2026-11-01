package org.pangrova.streaming.maintenance;

import java.util.Collection;

/**
 * Provides the set of table names that are candidates for maintenance operations.
 *
 * <p>Implementations may query a metadata catalog, read from a configuration file,
 * or derive the list from active streaming pipelines.</p>
 */
@FunctionalInterface
public interface MaintenanceTableSource {

    /**
     * Returns the current set of table names eligible for maintenance.
     *
     * @return collection of fully-qualified table names
     */
    Collection<String> listManagedTables();
}
