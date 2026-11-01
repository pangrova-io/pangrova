package org.pangrova.spi.indexing.data;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.indexing.descriptor.IndexType;

import java.util.List;

/**
 * BTree index data: sorted keys organized in leaf pages.
 *
 * <p>When {@code storePositions} is false (default), pages contain
 * key sets only (no rowPositions) for file-level skip decisions.
 * When true, full rowPositions are stored for row-level filtering.</p>
 *
 * @param leafPages      sorted leaf pages of the BTree
 * @param storePositions whether row positions are stored in pages
 */
@Experimental
public record BTreeIndexData(
        List<BTreePage> leafPages,
        boolean storePositions) implements IndexData {

    public BTreeIndexData {
        leafPages = leafPages == null ? List.of() : List.copyOf(leafPages);
    }

    /**
     * Convenience constructor with storePositions defaulting to false.
     */
    public BTreeIndexData(List<BTreePage> leafPages) {
        this(leafPages, false);
    }

    @Override
    public IndexType indexType() {
        return IndexType.BTREE;
    }

    @Override
    public long estimatedSizeInBytes() {
        return leafPages.stream().mapToLong(BTreePage::sizeInBytes).sum();
    }
}
