package org.pangrova.metadata.api.cache;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Callback fired by an invalidation consumer after applying a TABLE
 * invalidation entry from the cross-coordinator {@code invalidation_log}.
 *
 * <p>F-META-03: The poller drops the unified {@link MetadataCache} L1 entry directly,
 * but downstream caches (e.g. {@code DefaultMetadataService.resolvedTableCache}) need
 * a typed (catalog, schema, table) signal to invalidate their entries too. This SAM
 * is the wiring point so the cross-node poller reaches every cache layer in lockstep
 * with the unified cache, eliminating divergent TTLs across coordinators.</p>
 */
@FunctionalInterface
@Experimental
public interface TableInvalidationListener {

    /**
     * Invoked after the unified cache has dropped its L1 entry for this table.
     * Implementations should invalidate any downstream caches keyed by the same
     * table reference.
     */
    void onTableInvalidated(String catalog, String schema, String table);
}
