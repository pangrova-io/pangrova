package org.pangrova.indexing.api.data.bitmap;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.IndexData;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bitmap index data: one bitmap (serialized byte array) per distinct value.
 * Optimized for low-cardinality columns.
 *
 * <p>When {@code partial} is true, the bitmap does not cover all distinct
 * values (cardinality exceeded during build). Evaluation for unknown
 * values must return UNKNOWN, never SKIP.</p>
 *
 * @param bitmaps mapping from distinct value to serialized bitmap bytes
 * @param partial true if the bitmap is incomplete (cardinality exceeded)
 */
@Experimental
public record BitmapIndexData(
        Map<Comparable<?>, byte[]> bitmaps,
        boolean partial) implements IndexData {

    public BitmapIndexData {
        bitmaps = Map.copyOf(deepCopyBitmaps(bitmaps));
    }

    /** Returns an independent snapshot of all encoded bitmaps. */
    @Override
    public Map<Comparable<?>, byte[]> bitmaps() {
        return deepCopyBitmaps(bitmaps);
    }

    /** Tests key coverage without copying bitmap payloads. */
    public boolean containsKey(Comparable<?> key) {
        return bitmaps.containsKey(key);
    }

    @Override
    public IndexType indexType() {
        return IndexType.BITMAP;
    }

    @Override
    public long estimatedSizeInBytes() {
        return bitmaps.values().stream().mapToLong(b -> b.length).sum();
    }

    private static Map<Comparable<?>, byte[]> deepCopyBitmaps(
            Map<Comparable<?>, byte[]> original) {
        if (original == null || original.isEmpty()) {
            return Map.of();
        }
        var copy = new LinkedHashMap<Comparable<?>, byte[]>(original.size());
        for (var entry : original.entrySet()) {
            copy.put(entry.getKey(),
                    entry.getValue() == null ? new byte[0] : entry.getValue().clone());
        }
        return Map.copyOf(copy);
    }
}
