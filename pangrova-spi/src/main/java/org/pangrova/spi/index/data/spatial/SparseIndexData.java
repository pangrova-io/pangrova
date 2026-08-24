package org.pangrova.spi.index.data.spatial;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;

import java.util.List;

/**
 * Sparse index data: one entry per N rows (stride) for ordered columns.
 * Stores (key, rowOffset) pairs at regular intervals for range narrowing.
 *
 * @param entries sorted sparse entries (key + row offset)
 * @param stride  number of rows between consecutive entries
 */
@Experimental
public record SparseIndexData(
        List<SparseEntry> entries,
        int stride) implements IndexData {

    private static final long ESTIMATED_ENTRY_BYTES = 24L;

    public SparseIndexData {
        entries = entries == null ? List.of() : List.copyOf(entries);
    }

    @Override
    public IndexType indexType() {
        return IndexType.SPARSE;
    }

    @Override
    public long estimatedSizeInBytes() {
        return entries.size() * ESTIMATED_ENTRY_BYTES;
    }
}
