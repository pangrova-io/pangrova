package org.pangrova.planner.api.metadata;

import org.pangrova.common.api.annotation.Internal;

/**
 * Provides the current metadata change sequence for a catalog.
 *
 * <p>The compiler can compare observations before and after optimization to reject
 * a concurrently changed catalog. This is change detection, not a transaction or
 * a common snapshot across external catalogs. The current Server does not install
 * this optional provider; connector read bindings remain independently authoritative.</p>
 *
 * <p>Implementations must be safe for concurrent query compilation. A read failure
 * must propagate as an exception, never as zero or a cached successful observation.
 * The caller borrows the provider; this interface transfers no resource ownership.</p>
 */
@FunctionalInterface
@Internal
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
