package org.pangrova.spi.index.vector;

import org.pangrova.spi.annotation.Experimental;

/**
 * Quantization types for vector index storage and computation.
 *
 * <p>Quantization reduces the memory footprint and improves search throughput
 * of vector indexes by compressing high-dimensional floating-point vectors
 * into lower-precision representations. The trade-off is a small loss in
 * recall accuracy.</p>
 *
 * <p>Each constant carries a stable integer ID for persistence. These IDs
 * are fixed and must never change, even if the enum is reordered or new
 * values are inserted.</p>
 *
 * <p>Industry references:
 * <ul>
 *   <li>FP32/FP16: standard IEEE 754 full/half precision</li>
 *   <li>INT8 (scalar quantization): Milvus, Qdrant, Weaviate</li>
 *   <li>BINARY: Faiss BinaryFlat, Milvus BIN_FLAT / BIN_IVF_FLAT</li>
 *   <li>DiskANN and GPU indexes are future extensions built on these
 *       quantization primitives</li>
 * </ul>
 * </p>
 *
 * @see org.pangrova.spi.index.data.vector.VectorIndexData
 */
@Experimental(reason = "G-AI-07: quantization capability for vector indexes")
public enum QuantizationType {

    /**
     * Full-precision 32-bit IEEE 754 floating-point.
     * No quantization loss; highest memory usage (4 bytes per dimension).
     */
    FP32(1),

    /**
     * Half-precision 16-bit IEEE 754 floating-point.
     * 2x memory reduction with negligible recall loss for most workloads
     * (2 bytes per dimension).
     */
    FP16(2),

    /**
     * 8-bit integer scalar quantization.
     * 4x memory reduction; maps each dimension to [0, 255] range.
     * Good balance of compression and recall (1 byte per dimension).
     */
    INT8(3),

    /**
     * Binary quantization.
     * 32x memory reduction; each dimension is encoded as a single bit.
     * Best for high-dimensional vectors (>= 768 dims) where Hamming
     * distance approximates cosine distance. Significant recall loss
     * at lower dimensions.
     */
    BINARY(4);

    private static final java.util.Map<Integer, QuantizationType> BY_STABLE_ID;

    static {
        java.util.Map<Integer, QuantizationType> map = new java.util.HashMap<>();
        for (QuantizationType t : values()) {
            QuantizationType prev = map.put(t.stableId, t);
            if (prev != null) {
                throw new ExceptionInInitializerError(
                        "Duplicate stableId " + t.stableId + " for " + prev + " and " + t);
            }
        }
        BY_STABLE_ID = java.util.Map.copyOf(map);
    }

    private final int stableId;

    QuantizationType(int stableId) {
        this.stableId = stableId;
    }

    /**
     * Returns the stable persistence ID (independent of ordinal).
     *
     * @return the stable ID for serialization
     */
    public int getStableId() {
        return stableId;
    }

    /**
     * Resolves a {@link QuantizationType} from its stable persistence ID.
     *
     * @param stableId the persisted stable ID
     * @return the corresponding QuantizationType
     * @throws IllegalArgumentException if no QuantizationType has the given ID
     */
    public static QuantizationType fromStableId(int stableId) {
        QuantizationType t = BY_STABLE_ID.get(stableId);
        if (t == null) {
            throw new IllegalArgumentException(
                    "Unknown QuantizationType stableId: " + stableId);
        }
        return t;
    }

    /**
     * Returns the number of bytes per dimension for this quantization type.
     *
     * <p>For {@link #BINARY}, this returns 0 because a single bit per
     * dimension does not map cleanly to whole bytes. Callers should use
     * {@link #bitsPerDimension()} for precise memory estimation.</p>
     *
     * @return bytes per dimension, or 0 for sub-byte encodings
     */
    public int bytesPerDimension() {
        return switch (this) {
            case FP32 -> 4;
            case FP16 -> 2;
            case INT8 -> 1;
            case BINARY -> 0;
        };
    }

    /**
     * Returns the number of bits per dimension for this quantization type.
     *
     * @return bits per dimension
     */
    public int bitsPerDimension() {
        return switch (this) {
            case FP32 -> 32;
            case FP16 -> 16;
            case INT8 -> 8;
            case BINARY -> 1;
        };
    }
}
