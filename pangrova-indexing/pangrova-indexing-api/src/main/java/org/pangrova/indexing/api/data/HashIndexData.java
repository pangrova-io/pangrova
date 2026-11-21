package org.pangrova.indexing.api.data;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexType;

/**
 * Hash index data: open-addressing hash table for O(1) equality lookups.
 *
 * @param table      the raw hash table byte array (slot statuses + fingerprints)
 * @param entryCount number of distinct values inserted
 * @param capacity   number of slots in the hash table
 */
@Experimental
public record HashIndexData(
        byte[] table,
        int entryCount,
        int capacity) implements IndexData {

    public HashIndexData {
        if (table == null) {
            throw new IllegalArgumentException("table must not be null");
        }
        table = table.clone();
    }

    @Override
    public byte[] table() {
        return table.clone();
    }

    @Override
    public IndexType indexType() {
        return IndexType.HASH;
    }

    @Override
    public long estimatedSizeInBytes() {
        return table.length;
    }
}
