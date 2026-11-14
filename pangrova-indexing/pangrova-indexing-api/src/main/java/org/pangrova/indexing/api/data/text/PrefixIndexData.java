package org.pangrova.indexing.api.data.text;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.IndexData;

import java.util.List;

/**
 * Prefix index data: sparse sorted prefix entries for prefix-based
 * string matching (e.g., LIKE 'abc%').
 *
 * @param entries      sorted prefix entries
 * @param prefixLength the configured prefix truncation length
 */
@Experimental
public record PrefixIndexData(
        List<PrefixEntry> entries,
        int prefixLength) implements IndexData {

    public PrefixIndexData {
        entries = entries == null ? List.of() : List.copyOf(entries);
    }

    @Override
    public IndexType indexType() {
        return IndexType.PREFIX;
    }

    @Override
    public long estimatedSizeInBytes() {
        return entries.size() * (long) (prefixLength * 2 + 8);
    }
}
