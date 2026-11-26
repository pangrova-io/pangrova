package org.pangrova.indexing.api.enums;

import org.pangrova.common.api.annotation.Experimental;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the supported index types in the Pangrova indexing module.
 *
 * <p>Each type owns its physical execution family. Scalar file-pruning types
 * use {@code IndexStrategyProvider}; Native retrieval types use the durable
 * artifact build plane. Family membership does not advertise algorithm support.
 *
 * <p>Each enum constant carries a stable integer ID used for persistence.
 * These IDs are fixed and must never change, even if the enum is reordered
 * or new values are inserted between existing ones.
 */
@Experimental
public enum IndexType {

    /** Sorted B-Tree index for range and equality queries. */
    BTREE(1, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Inverted index for full-text search and token-based matching. */
    INVERTED(2, IndexPhysicalProfile.NATIVE_RETRIEVAL),

    /** Bloom filter index for probabilistic membership testing on equality predicates. */
    BLOOM_FILTER(3, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Prefix index for prefix-based string matching. */
    PREFIX(4, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** N-gram bloom filter index for substring (LIKE) pattern matching. */
    NGRAM_BLOOM_FILTER(5, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Bitmap index for low-cardinality columns using Roaring Bitmap encoding. */
    BITMAP(6, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Hash index for fast equality lookups via open-addressing hash table. */
    HASH(7, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Sparse index sampling every N-th row for range queries. */
    SPARSE(8, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** R-Tree spatial index for multi-dimensional bounding box queries. */
    R_TREE(9, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Generalized Inverted Index for composite value containment queries. */
    GIN(10, IndexPhysicalProfile.SCALAR_FILE_PRUNING),

    /** Vector index for approximate nearest neighbor (ANN) search on high-dimensional embeddings. */
    VECTOR(11, IndexPhysicalProfile.NATIVE_RETRIEVAL);

    private static final Map<Integer, IndexType> BY_STABLE_ID;

    static {
        BY_STABLE_ID = new HashMap<>();
        for (IndexType type : values()) {
            IndexType existing = BY_STABLE_ID.put(type.stableId, type);
            if (existing != null) {
                throw new ExceptionInInitializerError(
                        "Duplicate stableId " + type.stableId + " for " + existing + " and " + type);
            }
        }
    }

    private final int stableId;
    private final IndexPhysicalProfile physicalProfile;

    IndexType(int stableId, IndexPhysicalProfile physicalProfile) {
        this.stableId = stableId;
        this.physicalProfile = physicalProfile;
    }

    /** Returns the static execution family; does not assert that a build algorithm is installed. */
    public IndexPhysicalProfile physicalProfile() {
        return physicalProfile;
    }

    /** Returns whether the type belongs to the durable Native retrieval build plane. */
    public boolean requiresNativeArtifact() {
        return physicalProfile == IndexPhysicalProfile.NATIVE_RETRIEVAL;
    }

    /**
     * Returns the stable integer ID used for persistence.
     *
     * <p>Unlike {@link #ordinal()}, this value is fixed and does not change
     * when the enum is reordered or new values are added.
     *
     * @return the stable persistence ID
     */
    public int getStableId() {
        return stableId;
    }

    /**
     * Resolves an {@link IndexType} from its stable persistence ID.
     *
     * @param stableId the persisted stable ID
     * @return the corresponding IndexType
     * @throws IllegalArgumentException if no IndexType has the given ID
     */
    public static IndexType fromStableId(int stableId) {
        IndexType type = BY_STABLE_ID.get(stableId);
        if (type == null) {
            throw new IllegalArgumentException("Unknown IndexType stableId: " + stableId);
        }
        return type;
    }
}
