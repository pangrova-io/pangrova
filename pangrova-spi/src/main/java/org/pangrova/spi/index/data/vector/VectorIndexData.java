package org.pangrova.spi.index.data.vector;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.vector.DistanceMetric;

import java.util.Map;

/**
 * Vector index data container for approximate nearest neighbor (ANN) search.
 *
 * <p>Holds the serialized index payload (HNSW graph or IVFPQ codebook +
 * inverted lists) along with metadata needed for search dispatch.</p>
 *
 * <p><strong>Exception to IndexData defensive-copy contract:</strong>
 * {@code indexPayload} is <em>not</em> defensively copied on construction
 * because vector index payloads can exceed 12 MB. Callers <strong>must
 * not</strong> modify the array after passing it to the constructor.</p>
 *
 * @param algorithmType the vector algorithm used to build this index
 * @param dimension     vector dimensionality (must be &ge; 1)
 * @param metric        distance metric used during build
 * @param vectorCount   number of indexed vectors (must be &ge; 0)
 * @param indexPayload  serialized index bytes (not defensively copied)
 * @param buildParams   algorithm-specific build parameters (defensively copied)
 */
@Experimental
public record VectorIndexData(
        VectorAlgorithmType algorithmType,
        int dimension,
        DistanceMetric metric,
        int vectorCount,
        byte[] indexPayload,
        Map<String, String> buildParams
) implements IndexData {

    /**
     * Supported vector index algorithm types.
     *
     * <p>Each constant carries a stable integer ID used for persistence in the
     * serialized payload. These IDs are fixed and must never change, even if
     * the enum is reordered or new values are inserted.</p>
     */
    public enum VectorAlgorithmType {
        /** Hierarchical Navigable Small World graph. */
        HNSW(1),
        /** Inverted File with Product Quantization. */
        IVFPQ(2),
        /** DiskANN (Vamana graph, SSD-resident). */
        DISK_ANN(3);

        private static final java.util.Map<Integer, VectorAlgorithmType> BY_STABLE_ID;

        static {
            java.util.Map<Integer, VectorAlgorithmType> map = new java.util.HashMap<>();
            for (VectorAlgorithmType t : values()) {
                VectorAlgorithmType prev = map.put(t.stableId, t);
                if (prev != null) {
                    throw new ExceptionInInitializerError(
                            "Duplicate stableId " + t.stableId + " for " + prev + " and " + t);
                }
            }
            BY_STABLE_ID = java.util.Map.copyOf(map);
        }

        private final int stableId;

        VectorAlgorithmType(int stableId) {
            this.stableId = stableId;
        }

        /** Returns the stable persistence ID (independent of ordinal). */
        public int getStableId() {
            return stableId;
        }

        /**
         * Resolves a {@link VectorAlgorithmType} from its stable persistence ID.
         *
         * @throws IllegalArgumentException if no algorithm has the given ID
         */
        public static VectorAlgorithmType fromStableId(int stableId) {
            VectorAlgorithmType t = BY_STABLE_ID.get(stableId);
            if (t == null) {
                throw new IllegalArgumentException(
                        "Unknown VectorAlgorithmType stableId: " + stableId);
            }
            return t;
        }
    }

    public VectorIndexData {
        if (indexPayload == null) {
            throw new IllegalArgumentException("indexPayload must not be null");
        }
        if (dimension < 1) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (vectorCount < 0) {
            throw new IllegalArgumentException("vectorCount must be non-negative");
        }
        // indexPayload intentionally NOT copied — see class Javadoc.
        buildParams = Map.copyOf(buildParams);
    }

    @Override
    public IndexType indexType() {
        return IndexType.VECTOR;
    }

    @Override
    public long estimatedSizeInBytes() {
        return indexPayload.length + buildParams.size() * 64L;
    }
}
