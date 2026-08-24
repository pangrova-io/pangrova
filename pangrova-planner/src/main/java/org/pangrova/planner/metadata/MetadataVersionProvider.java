package org.pangrova.planner.metadata;

/**
 * Provides the current metadata change sequence for a catalog.
 *
 * <p>Used by {@link PangrovaPlanner} to pin metadata versions before compilation
 * and verify they haven't changed after compilation, ensuring cross-catalog
 * query consistency.</p>
 */
@FunctionalInterface
public interface MetadataVersionProvider {

    /**
     * Returns the latest change sequence number for the given catalog.
     * Each metadata mutation (sync, DDL) increments this sequence.
     *
     * @param catalogName the catalog to query
     * @return the current change sequence, or 0 if the catalog has no tracked changes
     */
    long getLatestChangeSequence(String catalogName);
}
