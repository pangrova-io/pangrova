package org.pangrova.indexing.engine.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * In-memory HNSW graph structure used during build and search.
 *
 * <p>Package-private: only used by {@link HnswEngine} and {@link HnswGraphCodec}.
 * Stores vectors and per-level adjacency lists for each node.</p>
 */
final class HnswGraph {

    private final List<float[]> vectors;
    private final int dimension;
    // adjacencyLists[level] → list of per-node neighbor lists
    // adjacencyLists.get(level).get(nodeId) → neighbors of nodeId at level
    private final List<List<List<Integer>>> adjacencyLists;
    // rowOffsets[nodeId] → physical 0-based row position of the vector in the
    // source data file (nulls counted). Search results must report this offset,
    // NOT the dense nodeId, so ANN results align with the brute-force scanner
    // and downstream row fetching when the vector column contains NULLs.
    private long[] rowOffsets;
    private int entryPoint;
    private int maxLevel;

    HnswGraph(List<float[]> vectors, long[] rowOffsets, int dimension) {
        // Wrap to guarantee mutability for incremental builds that append.
        this.vectors = new ArrayList<>(vectors);
        this.rowOffsets = rowOffsets.clone();
        this.dimension = dimension;
        this.adjacencyLists = new ArrayList<>();
        this.entryPoint = -1;
        this.maxLevel = -1;
    }

    /** Deserialization constructor. */
    HnswGraph(List<float[]> vectors, long[] rowOffsets, int dimension,
              List<List<List<Integer>>> adjacencyLists, int entryPoint, int maxLevel) {
        this.vectors = new ArrayList<>(vectors);
        this.rowOffsets = rowOffsets.clone();
        this.dimension = dimension;
        this.adjacencyLists = adjacencyLists;
        this.entryPoint = entryPoint;
        this.maxLevel = maxLevel;
    }

    int nodeCount() { return vectors.size(); }
    int dimension() { return dimension; }
    int entryPoint() { return entryPoint; }
    int maxLevel() { return Math.max(maxLevel, 0); }
    float[] vector(int nodeId) { return vectors.get(nodeId); }
    List<float[]> vectors() { return vectors; }
    int levelCount() { return adjacencyLists.size(); }

    /** Physical 0-based row offset of the vector stored at {@code nodeId}. */
    long rowOffset(int nodeId) { return rowOffsets[nodeId]; }

    /** Returns the nodeId → rowOffset table for serialization. */
    long[] rowOffsets() { return rowOffsets; }

    void setEntryPoint(int nodeId) {
        this.entryPoint = nodeId;
    }

    /**
     * Appends new vectors and their physical row offsets to the graph (for
     * incremental build). The caller is responsible for invoking
     * {@link #ensureLevel} for each appended node before adding connections.
     *
     * @return the starting nodeId of the appended block (== old node count)
     */
    int appendVectors(List<float[]> newVectors, long[] newRowOffsets) {
        int start = vectors.size();
        vectors.addAll(newVectors);
        long[] merged = Arrays.copyOf(rowOffsets, start + newVectors.size());
        System.arraycopy(newRowOffsets, 0, merged, start, newRowOffsets.length);
        this.rowOffsets = merged;
        return start;
    }

    int size() {
        return vectors.size();
    }

    /** Ensures adjacency lists exist for the given node up to the given level. */
    synchronized void ensureLevel(int nodeId, int level) {
        while (adjacencyLists.size() <= level) {
            adjacencyLists.add(new ArrayList<>());
        }
        for (int l = 0; l <= level; l++) {
            List<List<Integer>> levelList = adjacencyLists.get(l);
            while (levelList.size() <= nodeId) {
                levelList.add(new ArrayList<>());
            }
        }
        if (level > maxLevel) {
            maxLevel = level;
        }
    }

    synchronized List<Integer> neighbors(int nodeId, int level) {
        if (level >= adjacencyLists.size()) {
            return Collections.emptyList();
        }
        List<List<Integer>> levelList = adjacencyLists.get(level);
        if (nodeId >= levelList.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(levelList.get(nodeId));
    }

    synchronized void addConnection(int fromId, int toId, int level) {
        ensureLevel(fromId, level);
        ensureLevel(toId, level);
        List<Integer> fromNeighbors = adjacencyLists.get(level).get(fromId);
        if (fromNeighbors.indexOf(toId) < 0) {
            fromNeighbors.add(toId);
        }
    }

    synchronized void setConnections(int nodeId, int level, List<Integer> newNeighbors) {
        ensureLevel(nodeId, level);
        List<Integer> current = adjacencyLists.get(level).get(nodeId);
        current.clear();
        current.addAll(newNeighbors);
    }

    /** Returns the adjacency lists for serialization. */
    List<List<List<Integer>>> adjacencyLists() {
        return adjacencyLists;
    }
}
