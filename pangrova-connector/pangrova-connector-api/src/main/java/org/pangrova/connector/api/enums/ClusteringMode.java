package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Clustering mode: how multi-column values are mapped to sort keys.
 */
@Experimental
public enum ClusteringMode {
    /** Z-Order bit-interleaving: interleave bits from multiple columns. */
    INTERLEAVE,
    /** Linear dictionary-order concatenation of column values. */
    LINEAR
}
