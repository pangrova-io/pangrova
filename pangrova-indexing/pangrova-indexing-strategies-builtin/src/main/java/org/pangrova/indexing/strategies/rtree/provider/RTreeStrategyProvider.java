package org.pangrova.indexing.strategies.rtree.provider;

import org.pangrova.indexing.api.codec.TypeSerializer;
import org.pangrova.indexing.strategies.rtree.serializer.RTreeSerializer;

import org.pangrova.indexing.strategies.rtree.serializer.RTreeBuilder;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.indexing.api.data.spatial.BoundingBox;
import org.pangrova.indexing.api.data.spatial.RTreeIndexData;
import org.pangrova.indexing.api.data.spatial.RTreeNode;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.connector.api.enums.SpatialOp;
import org.pangrova.connector.api.index.predicate.SpatialQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * R-Tree spatial index strategy using STR packing for build
 * and MBR traversal for evaluation.
 */
public final class RTreeStrategyProvider implements IndexStrategyProvider {

    @Override
    public TypeSerializer<?> payloadSerializer() {
        return new RTreeSerializer();
    }

    @Override
    public IndexType supportedType() {
        return IndexType.R_TREE;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        List<double[]> points = new ArrayList<>();
        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (row.size() >= 2 && row.get(0) != null && row.get(1) != null) {
                double x = ((Number) row.get(0)).doubleValue();
                double y = ((Number) row.get(1)).doubleValue();
                points.add(new double[]{x, y});
            }
        }
        RTreeBuilder.RTreeResult result = RTreeBuilder.build(points);
        return new RTreeIndexData(result.nodes(), result.leafCount());
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof RTreeIndexData rtree)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (!(predicate instanceof SpatialQuery spatial)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (rtree.nodes().isEmpty()) {
            return IndexEvalResult.SKIP;
        }

        BoundingBox queryBox = new BoundingBox(
                new double[]{spatial.minX(), spatial.minY()},
                new double[]{spatial.maxX(), spatial.maxY()});

        // Iterative stack-based traversal from root (index 0)
        return traverseIterative(rtree.nodes(), 0, queryBox, spatial.op())
                ? IndexEvalResult.KEEP : IndexEvalResult.SKIP;
    }

    /**
     * Iteratively traverses the R-Tree from the root using an explicit,
     * auto-growing node-index stack. Produces identical results to recursive
     * traversal.
     *
     * <p>Algorithm: explicit DFS. For each node popped, check MBR match; if leaf
     * and matching, return true; otherwise (internal node whose MBR intersects)
     * push children in reverse order to preserve left-to-right traversal. The
     * stack grows on demand so deep trees are never silently truncated — a fixed
     * cap would drop unvisited subtrees and produce false-negative SKIP results
     * (missing rows) for spatial columns large enough to exceed the cap.</p>
     */
    private static boolean traverseIterative(
            List<RTreeNode> nodes, int rootIndex,
            BoundingBox queryBox, SpatialOp op) {
        int nodeCount = nodes.size();
        int[] stack = new int[16];
        int top = 0;
        stack[top++] = rootIndex;

        while (top > 0) {
            int nodeIndex = stack[--top];
            if (!isInRange(nodeIndex, nodeCount)) {
                continue;
            }
            RTreeNode node = nodes.get(nodeIndex);
            if (node.leaf()) {
                if (matchesMbr(node.boundingBox(), queryBox, op)) {
                    return true;
                }
                continue;
            }
            if (!node.boundingBox().mbrIntersects(queryBox)) {
                continue;
            }
            List<Integer> children = node.childIndices();
            if (top + children.size() > stack.length) {
                stack = java.util.Arrays.copyOf(stack,
                        Math.max(stack.length * 2, top + children.size()));
            }
            for (int i = children.size() - 1; i >= 0; i--) {
                stack[top++] = children.get(i);
            }
        }
        return false;
    }

    private static boolean isInRange(int index, int count) {
        return index >= 0 && index < count;
    }

    private static boolean matchesMbr(BoundingBox nodeBbox, BoundingBox queryBox, SpatialOp op) {
        return switch (op) {
            case INTERSECTS -> nodeBbox.mbrIntersects(queryBox);
            case WITHIN -> queryBox.mbrContains(nodeBbox);
            case CONTAINS -> nodeBbox.mbrContains(queryBox);
        };
    }
}
