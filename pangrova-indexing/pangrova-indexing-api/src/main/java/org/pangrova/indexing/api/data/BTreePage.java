package org.pangrova.indexing.api.data;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * A single page (leaf node) in a BTree index.
 *
 * <p>When {@code storePositions} is false on the parent
 * {@link BTreeIndexData}, {@code rowPositions} is {@code null}
 * and only the key set is stored for file-level skip decisions.</p>
 *
 * @param keys         sorted keys in this page
 * @param rowPositions per-key row position lists; {@code null} when
 *                     storePositions is false
 * @param minKey       minimum key in this page
 * @param maxKey       maximum key in this page
 */
@Experimental
public record BTreePage(
        List<Comparable<?>> keys,
        List<List<Integer>> rowPositions,
        Comparable<?> minKey,
        Comparable<?> maxKey) {

    private static final long ESTIMATED_KEY_BYTES = 24L;

    public BTreePage {
        keys = keys == null ? List.of() : List.copyOf(keys);
        rowPositions = rowPositions == null ? null : List.copyOf(rowPositions.stream()
                .map(list -> list == null ? List.<Integer>of() : List.copyOf(list))
                .toList());
    }

    /**
     * Estimated size in bytes for cache eviction calculations.
     */
    public long sizeInBytes() {
        long size = keys.size() * ESTIMATED_KEY_BYTES;
        if (rowPositions != null) {
            size += rowPositions.stream()
                    .mapToLong(list -> list.size() * 4L + 16L)
                    .sum();
        }
        return size;
    }
}
