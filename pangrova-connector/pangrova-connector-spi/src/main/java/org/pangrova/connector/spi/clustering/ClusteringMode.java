package org.pangrova.connector.spi.clustering;

/**
 * Clustering mode: how multi-column values are mapped to sort keys.
 */
public enum ClusteringMode {
    /** Z-Order bit-interleaving: interleave bits from multiple columns. */
    INTERLEAVE,
    /** Linear dictionary-order concatenation of column values. */
    LINEAR
}
