package org.pangrova.spi.connector.clustering;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Clustering state for a table.
 */
@Experimental
public enum ClusteringState {
    /** No clustering order defined. */
    NONE,
    /** Clustering order defined but not yet applied. */
    PENDING,
    /** Recluster in progress. */
    RECLUSTERING,
    /** Clustering applied and up-to-date. */
    CLUSTERED,
    /** Data has changed since last recluster; ratio degraded. */
    STALE
}
