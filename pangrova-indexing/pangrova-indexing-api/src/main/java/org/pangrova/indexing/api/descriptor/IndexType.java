package org.pangrova.indexing.api.descriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the supported index types in the Pangrova indexing module.
 *
 * <p>Each value corresponds to a distinct indexing strategy with its own
 * build and evaluate semantics, implemented via {@code IndexStrategyProvider}.
 *
 * <p>Each enum constant carries a stable integer ID used for persistence.
 * These IDs are fixed and must never change, even if the enum is reordered
 * or new values are inserted between existing ones.
 */
public enum IndexType {

    /** Sorted B-Tree index for range and equality queries. */
    BTREE(1),

    /** Inverted index for full-text search and token-based matching. */
    INVERTED(2),

    /** Bloom filter index for probabilistic membership testing on equality predicates. */
    BLOOM_FILTER(3),

    /** Prefix index for prefix-based string matching. */
    PREFIX(4),

    /** N-gram bloom filter index for substring (LIKE) pattern matching. */
    NGRAM_BLOOM_FILTER(5),

    /** Bitmap index for low-cardinality columns using Roaring Bitmap encoding. */
    BITMAP(6),

    /** Hash index for fast equality lookups via open-addressing hash table. */
    HASH(7),

    /** Sparse index sampling every N-th row for range queries. */
    SPARSE(8),

    /** R-Tree spatial index for multi-dimensional bounding box queries. */
    R_TREE(9),

    /** Generalized Inverted Index for composite value containment queries. */
    GIN(10),

    /** Vector index for approximate nearest neighbor (ANN) search on high-dimensional embeddings. */
    VECTOR(11);

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

    IndexType(int stableId) {
        this.stableId = stableId;
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
