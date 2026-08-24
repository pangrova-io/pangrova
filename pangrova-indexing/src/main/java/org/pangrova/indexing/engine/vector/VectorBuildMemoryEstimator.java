package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;

import java.util.Map;

/**
 * Estimates peak memory consumption during vector index build.
 *
 * <p>Used by {@link VectorStrategyProvider} to reject builds that would
 * exceed the configured memory budget before allocating any data structures.</p>
 */
final class VectorBuildMemoryEstimator {

    private static final String PROP_M = "hnsw.M";
    private static final String PROP_NLIST = "ivfpq.nlist";
    private static final String PROP_PQ_M = "ivfpq.m";
    private static final String PROP_NBITS = "ivfpq.nbits";
    private static final int DEFAULT_M = 16;
    private static final int DEFAULT_NLIST = 256;
    private static final int DEFAULT_PQ_M = 8;
    private static final int DEFAULT_NBITS = 8;
    /** Estimated navigation-graph adjacency bytes per vector for DiskANN. */
    private static final long DISKANN_ADJACENCY_BYTES_PER_VECTOR = 64L * 12;

    private VectorBuildMemoryEstimator() {
        // utility class
    }

    /**
     * Estimates peak memory in bytes for building a vector index.
     *
     * @param vectorCount number of vectors to index
     * @param dimension   vector dimensionality
     * @param algorithm   HNSW or IVFPQ
     * @param properties  algorithm-specific parameters
     * @return estimated peak memory in bytes
     * @throws IllegalArgumentException if algorithm is unknown
     */
    static long estimatePeakMemory(long vectorCount, int dimension,
                                   VectorAlgorithmType algorithm, Map<String, String> properties) {
        if (vectorCount < 0) {
            throw new IllegalArgumentException("vectorCount must not be negative: " + vectorCount);
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        if (vectorCount == 0) {
            return 0L;
        }
        return switch (algorithm) {
            case HNSW -> estimateHnsw(vectorCount, dimension, properties);
            case IVFPQ -> estimateIvfpq(vectorCount, dimension, properties);
            case DISK_ANN -> estimateDiskAnn(vectorCount, dimension);
        };
    }

    private static long estimateHnsw(long vectorCount, int dimension, Map<String, String> properties) {
        int m = intProp(properties, PROP_M, DEFAULT_M);
        // vectors in memory + adjacency lists (2*M neighbors per node, 4 bytes each)
        return vectorCount * dimension * 4L + vectorCount * m * 2L * 4L;
    }

    private static long estimateIvfpq(long vectorCount, int dimension, Map<String, String> properties) {
        int nlist = intProp(properties, PROP_NLIST, DEFAULT_NLIST);
        int pqM = intProp(properties, PROP_PQ_M, DEFAULT_PQ_M);
        int nbits = intProp(properties, PROP_NBITS, DEFAULT_NBITS);
        int subDim = dimension / Math.max(pqM, 1);
        // vectors in memory + centroids + codebook
        return vectorCount * dimension * 4L
                + (long) nlist * dimension * 4L
                + (long) pqM * (1L << nbits) * subDim * 4L;
    }

    private static long estimateDiskAnn(long vectorCount, int dimension) {
        int pqSubvectors = Math.max(1, dimension / 8);
        // PQ codes in memory (~1 byte per subvector per vector) + adjacency lists on SSD
        // In-memory footprint is much smaller than HNSW: only PQ navigation graph
        return vectorCount * pqSubvectors + vectorCount * 64L * DISKANN_ADJACENCY_BYTES_PER_VECTOR;
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }
}
