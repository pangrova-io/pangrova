package org.pangrova.indexing.api.data.spatial;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * A node in an R-Tree spatial index: bounding box, child references,
 * and a flag indicating whether this is a leaf node.
 *
 * @param boundingBox  the minimum bounding rectangle of this node
 * @param childIndices indices of child nodes in the flattened node list
 * @param leaf         true if this is a leaf node
 */
@Experimental
public record RTreeNode(
        BoundingBox boundingBox,
        List<Integer> childIndices,
        boolean leaf) {

    public RTreeNode {
        childIndices = childIndices == null ? List.of() : List.copyOf(childIndices);
    }
}
