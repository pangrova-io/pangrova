package org.pangrova.spi.connector.clustering;

/**
 * Clustering mode: how multi-column values are mapped to sort keys.
 */
public enum ClusteringMode {
    /** Z-Order bit-interleaving: interleave bits from multiple columns. */
    INTERLEAVE,
    /** Linear dictionary-order concatenation of column values. */
    LINEAR
}
