package org.pangrova.indexing.engine.rtree;

import org.pangrova.spi.index.data.spatial.BoundingBox;
import org.pangrova.spi.index.data.spatial.RTreeNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builds an R-Tree using the Sort-Tile-Recursive (STR) packing algorithm.
 * Produces a flattened breadth-first node list.
 */
final class RTreeBuilder {

    private static final int DEFAULT_NODE_CAPACITY = 8;

    private RTreeBuilder() {}

    /**
     * Builds an R-Tree from 2D points using STR packing.
     *
     * @param points list of (x, y) coordinate pairs
     * @return flattened R-Tree nodes in breadth-first order, and leaf count
     */
    static RTreeResult build(List<double[]> points) {
        if (points.isEmpty()) {
            return new RTreeResult(List.of(), 0);
        }

        List<RTreeNode> leafNodes = buildLeaves(points);
        if (leafNodes.size() == 1) {
            return new RTreeResult(leafNodes, 1);
        }

        List<List<RTreeNode>> levels = buildLevels(leafNodes);
        List<RTreeNode> allNodes = flattenAllLevels(levels);
        return new RTreeResult(allNodes, leafNodes.size());
    }

    private static List<List<RTreeNode>> buildLevels(List<RTreeNode> leafNodes) {
        List<List<RTreeNode>> levels = new ArrayList<>();
        levels.add(leafNodes);
        List<RTreeNode> currentLevel = leafNodes;

        while (currentLevel.size() > 1) {
            currentLevel = buildParentLevel(currentLevel);
            levels.add(currentLevel);
        }
        return levels;
    }

    private static List<RTreeNode> buildParentLevel(List<RTreeNode> currentLevel) {
        List<RTreeNode> parentLevel = new ArrayList<>();
        for (int i = 0; i < currentLevel.size(); i += DEFAULT_NODE_CAPACITY) {
            int end = Math.min(i + DEFAULT_NODE_CAPACITY, currentLevel.size());
            List<RTreeNode> children = currentLevel.subList(i, end);
            BoundingBox merged = mergeBounds(children);
            List<Integer> childIndices = new ArrayList<>();
            for (int j = i; j < end; j++) {
                childIndices.add(j);
            }
            parentLevel.add(new RTreeNode(merged, childIndices, false));
        }
        return parentLevel;
    }

    private static List<RTreeNode> flattenAllLevels(List<List<RTreeNode>> levels) {
        int totalLevels = levels.size();
        int[] levelOffset = new int[totalLevels];
        int offset = 0;
        for (int lvl = totalLevels - 1; lvl >= 0; lvl--) {
            levelOffset[lvl] = offset;
            offset += levels.get(lvl).size();
        }
        return flattenLevels(levels, levelOffset, offset);
    }

    private static List<RTreeNode> flattenLevels(List<List<RTreeNode>> levels,
                                                  int[] levelOffset, int totalSize) {
        int totalLevels = levels.size();
        List<RTreeNode> allNodes = new ArrayList<>(totalSize);
        for (int lvl = totalLevels - 1; lvl >= 0; lvl--) {
            if (lvl == 0) {
                allNodes.addAll(levels.get(lvl));
            } else {
                reindexLevel(levels.get(lvl), levelOffset[lvl - 1], allNodes);
            }
        }
        return allNodes;
    }

    private static void reindexLevel(List<RTreeNode> level, int childOffset, List<RTreeNode> out) {
        for (RTreeNode node : level) {
            List<Integer> corrected = new ArrayList<>(node.childIndices().size());
            for (int idx : node.childIndices()) {
                corrected.add(idx + childOffset);
            }
            out.add(new RTreeNode(node.boundingBox(), corrected, false));
        }
    }

    private static List<RTreeNode> buildLeaves(List<double[]> points) {
        int n = points.size();
        List<double[]> sorted = new ArrayList<>(points);
        sorted.sort(Comparator.comparingDouble(p -> p[0]));

        int leafCount = (int) Math.ceil((double) n / DEFAULT_NODE_CAPACITY);
        int numSlices = Math.max(1, (int) Math.ceil(Math.sqrt(leafCount)));
        int sliceSize = numSlices * DEFAULT_NODE_CAPACITY;

        List<RTreeNode> leaves = new ArrayList<>();
        for (int s = 0; s < n; s += sliceSize) {
            int sliceEnd = Math.min(s + sliceSize, n);
            List<double[]> slice = sorted.subList(s, sliceEnd);
            slice.sort(Comparator.comparingDouble(p -> p[1]));
            tileSlice(slice, leaves);
        }
        return leaves;
    }

    private static void tileSlice(List<double[]> slice, List<RTreeNode> leaves) {
        for (int i = 0; i < slice.size(); i += DEFAULT_NODE_CAPACITY) {
            int tileEnd = Math.min(i + DEFAULT_NODE_CAPACITY, slice.size());
            List<double[]> tile = slice.subList(i, tileEnd);
            leaves.add(new RTreeNode(computeBounds(tile), List.of(), true));
        }
    }

    private static BoundingBox computeBounds(List<double[]> tile) {
        double minX = Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        for (double[] p : tile) {
            minX = Math.min(minX, p[0]);
            minY = Math.min(minY, p[1]);
            maxX = Math.max(maxX, p[0]);
            maxY = Math.max(maxY, p[1]);
        }
        return new BoundingBox(new double[]{minX, minY}, new double[]{maxX, maxY});
    }

    private static BoundingBox mergeBounds(List<RTreeNode> nodes) {
        double minX = Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        for (RTreeNode node : nodes) {
            double[] mins = node.boundingBox().minValues();
            double[] maxs = node.boundingBox().maxValues();
            minX = Math.min(minX, mins[0]);
            minY = Math.min(minY, mins[1]);
            maxX = Math.max(maxX, maxs[0]);
            maxY = Math.max(maxY, maxs[1]);
        }
        return new BoundingBox(new double[]{minX, minY}, new double[]{maxX, maxY});
    }

    record RTreeResult(List<RTreeNode> nodes, int leafCount) {}
}
