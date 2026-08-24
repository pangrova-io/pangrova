package org.pangrova.spi.index.data.spatial;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;

import java.util.List;

/**
 * R-Tree spatial index data: minimum bounding rectangles organized
 * in a tree structure for multi-dimensional geometric queries.
 *
 * @param nodes     flattened R-Tree nodes (breadth-first order)
 * @param leafCount number of leaf nodes in the tree
 */
@Experimental
public record RTreeIndexData(
        List<RTreeNode> nodes,
        int leafCount) implements IndexData {

    private static final long ESTIMATED_NODE_BYTES = 48L;

    public RTreeIndexData {
        nodes = nodes == null ? List.of() : List.copyOf(nodes);
    }

    @Override
    public IndexType indexType() {
        return IndexType.R_TREE;
    }

    @Override
    public long estimatedSizeInBytes() {
        return nodes.size() * ESTIMATED_NODE_BYTES;
    }
}
