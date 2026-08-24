// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.coordinator.ai.cache;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HNSW-backed semantic cache that uses approximate nearest neighbor search
 * to find similar queries in O(log n) instead of brute-force O(n).
 *
 * <p>Implements a lightweight in-process HNSW graph tailored for the semantic
 * cache use case (small dimension embeddings, incremental insertion, moderate
 * cardinality up to ~100K entries).</p>
 *
 * <p>This replaces {@link InMemorySemanticCache} for deployments where cache
 * size exceeds ~5K entries and brute-force scan becomes a latency concern.</p>
 */
@ThreadSafe
public final class HnswBackedSemanticCache implements SemanticCache {

    private static final Logger LOG = LoggerFactory.getLogger(HnswBackedSemanticCache.class);

    private static final int DEFAULT_M = 16;
    private static final int DEFAULT_EF_CONSTRUCTION = 64;
    private static final int DEFAULT_EF_SEARCH = 32;
    private static final double ML = 1.0 / Math.log(DEFAULT_M);

    private final int maxEntries;
    private final int m;
    private final int efConstruction;
    private final int efSearch;

    private final List<SemanticCacheEntry> entries = new ArrayList<>();
    private final List<List<List<Integer>>> layers = new ArrayList<>(); // [nodeIdx][layer] -> neighbors
    private int maxLevel = -1;
    private int entryPoint = -1;
    private int validCount = 0; // tracks non-null entries for correct eviction

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public HnswBackedSemanticCache(int maxEntries) {
        this(maxEntries, DEFAULT_M, DEFAULT_EF_CONSTRUCTION, DEFAULT_EF_SEARCH);
    }

    /** Full constructor exposing HNSW tuning parameters (m, efConstruction, efSearch). */
    public HnswBackedSemanticCache(int maxEntries, int m, int efConstruction, int efSearch) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
        this.m = m;
        this.efConstruction = efConstruction;
        this.efSearch = efSearch;
    }

    @Override
    public Optional<SemanticCacheHit> findSimilar(float[] queryEmbedding, String tenantId,
                                                   String principal, double threshold) {
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            return Optional.empty();
        }
        lock.readLock().lock();
        try {
            return searchBestHit(queryEmbedding, tenantId, threshold);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Optional<SemanticCacheHit> searchBestHit(float[] queryEmbedding, String tenantId,
                                                     double threshold) {
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        // HNSW search: find top-1 nearest neighbor
        List<int[]> candidates = searchHnsw(queryEmbedding, Math.max(efSearch, 10));

        SemanticCacheEntry bestEntry = null;
        double bestSimilarity = threshold;

        for (int[] candidate : candidates) {
            int idx = candidate[0];
            SemanticCacheEntry entry = entries.get(idx);
            if (entry == null || !matchesTenant(entry, tenantId)) {
                continue;
            }
            double similarity = cosineSimilarity(queryEmbedding, entry.queryEmbedding());
            if (similarity > bestSimilarity) {
                bestSimilarity = similarity;
                bestEntry = entry;
            }
        }

        if (bestEntry != null) {
            LOG.debug("event=hnsw_cache_hit similarity={}", bestSimilarity);
            return Optional.of(new SemanticCacheHit(bestEntry, bestSimilarity));
        }
        return Optional.empty();
    }

    @Override
    public void put(SemanticCacheEntry entry) {
        if (entry == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (validCount >= maxEntries) {
                // Evict oldest non-null entry (scan forward from index 0)
                for (int i = 0; i < entries.size(); i++) {
                    if (entries.get(i) != null) {
                        entries.set(i, null);
                        validCount--;
                        break;
                    }
                }
            }
            int nodeIdx = entries.size();
            entries.add(entry);
            validCount++;
            insertIntoGraph(nodeIdx, entry.queryEmbedding());

            // Compact if null ratio exceeds 50% to prevent unbounded list growth
            if (entries.size() > maxEntries * 2 && validCount < entries.size() / 2) {
                compactEntries();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void invalidateByTable(String tableName) {
        if (tableName == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            int removed = 0;
            for (int i = 0; i < entries.size(); i++) {
                SemanticCacheEntry e = entries.get(i);
                if (e != null && e.referencedTables().contains(tableName)) {
                    entries.set(i, null); // logical deletion
                    removed++;
                }
            }
            validCount -= removed;
            if (removed > 0) {
                LOG.info("event=hnsw_cache_invalidated table={} removed={}", tableName, removed);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        lock.readLock().lock();
        try {
            return entries.stream().filter(Objects::nonNull).count();
        } finally {
            lock.readLock().unlock();
        }
    }

    // --- Compaction ---

    /**
     * Rebuilds the entries list and HNSW graph by removing null entries.
     * Called when null ratio exceeds 50% to prevent unbounded list growth.
     * Must be called under write lock.
     */
    private void compactEntries() {
        List<SemanticCacheEntry> compacted = new ArrayList<>(validCount + 16);
        for (SemanticCacheEntry e : entries) {
            if (e != null) {
                compacted.add(e);
            }
        }
        entries.clear();
        layers.clear();
        maxLevel = -1;
        entryPoint = -1;
        validCount = 0;

        for (SemanticCacheEntry e : compacted) {
            int nodeIdx = entries.size();
            entries.add(e);
            validCount++;
            insertIntoGraph(nodeIdx, e.queryEmbedding());
        }
        LOG.info("event=hnsw_cache_compacted size_before={} size_after={}",
                compacted.size() + (entries.size() - compacted.size()), validCount);
    }

    // --- HNSW graph operations ---

    private void insertIntoGraph(int nodeIdx, float[] vector) {
        int nodeLevel = randomLevel();

        // Initialize layer structure for new node
        List<List<Integer>> nodeLayers = new ArrayList<>();
        for (int l = 0; l <= nodeLevel; l++) {
            nodeLayers.add(new ArrayList<>());
        }
        while (layers.size() <= nodeIdx) {
            layers.add(null);
        }
        layers.set(nodeIdx, nodeLayers);

        if (entryPoint == -1) {
            entryPoint = nodeIdx;
            maxLevel = nodeLevel;
            return;
        }

        int ep = entryPoint;

        // Greedy descent through layers above nodeLevel
        for (int level = maxLevel; level > nodeLevel; level--) {
            ep = greedyClosest(vector, ep, level);
        }

        // Insert at each layer from nodeLevel down to 0
        for (int level = Math.min(nodeLevel, maxLevel); level >= 0; level--) {
            ep = insertAtLevel(nodeIdx, vector, ep, nodeLayers, level);
        }

        if (nodeLevel > maxLevel) {
            maxLevel = nodeLevel;
            entryPoint = nodeIdx;
        }
    }

    private int insertAtLevel(int nodeIdx, float[] vector, int ep,
                              List<List<Integer>> nodeLayers, int level) {
        List<int[]> candidates = searchLayer(vector, ep, efConstruction, level);
        List<Integer> neighbors = selectNeighbors(candidates, m);
        nodeLayers.get(level).addAll(neighbors);

        // Bidirectional connections
        for (int neighbor : neighbors) {
            connectToNeighbor(nodeIdx, neighbor, level);
        }

        if (!candidates.isEmpty()) {
            return candidates.get(0)[0];
        }
        return ep;
    }

    private void connectToNeighbor(int nodeIdx, int neighbor, int level) {
        List<List<Integer>> neighborLayers = layers.get(neighbor);
        if (neighborLayers == null || level >= neighborLayers.size()) {
            return;
        }
        List<Integer> nNeighbors = neighborLayers.get(level);
        nNeighbors.add(nodeIdx);
        if (nNeighbors.size() > m * 2) {
            // Shrink: keep closest m*2 neighbors
            shrinkNeighbors(neighbor, level, m * 2);
        }
    }

    private List<int[]> searchHnsw(float[] query, int ef) {
        int ep = entryPoint;
        for (int level = maxLevel; level > 0; level--) {
            ep = greedyClosest(query, ep, level);
        }
        return searchLayer(query, ep, ef, 0);
    }

    private int greedyClosest(float[] query, int ep, int level) {
        float bestDist = distance(query, ep);
        boolean improved = true;
        while (improved) {
            improved = false;
            List<List<Integer>> epLayers = layers.get(ep);
            if (epLayers == null || level >= epLayers.size()) break;
            for (int neighbor : epLayers.get(level)) {
                if (entries.get(neighbor) == null) continue;
                float d = distance(query, neighbor);
                if (d < bestDist) {
                    bestDist = d;
                    ep = neighbor;
                    improved = true;
                }
            }
        }
        return ep;
    }

    private List<int[]> searchLayer(float[] query, int ep, int ef, int level) {
        Set<Integer> visited = new HashSet<>();
        // Min-heap by distance (closest first)
        PriorityQueue<int[]> candidates = new PriorityQueue<>(
                Comparator.comparingDouble(a -> Float.intBitsToFloat(a[1])));
        PriorityQueue<int[]> result = new PriorityQueue<>(
                (a, b2) -> Double.compare(
                        Float.intBitsToFloat(b2[1]), Float.intBitsToFloat(a[1])));

        float epDist = distance(query, ep);
        int epDistInt = Float.floatToIntBits(epDist);
        candidates.add(new int[]{ep, epDistInt});
        result.add(new int[]{ep, epDistInt});
        visited.add(ep);

        while (!candidates.isEmpty()) {
            int[] closest = candidates.poll();
            int[] furthestResult = result.peek();
            if (furthestResult != null
                    && Float.intBitsToFloat(closest[1]) > Float.intBitsToFloat(furthestResult[1])) {
                break;
            }

            int node = closest[0];
            List<List<Integer>> nodeLayers = layers.get(node);
            if (nodeLayers == null || level >= nodeLayers.size()) continue;

            for (int neighbor : nodeLayers.get(level)) {
                considerNeighbor(query, visited, candidates, result, ef, neighbor);
            }
        }

        List<int[]> resultList = new ArrayList<>(result);
        resultList.sort(Comparator.comparingDouble(a -> Float.intBitsToFloat(a[1])));
        return resultList;
    }

    private void considerNeighbor(float[] query, Set<Integer> visited,
                                  PriorityQueue<int[]> candidates, PriorityQueue<int[]> result,
                                  int ef, int neighbor) {
        if (visited.contains(neighbor)) return;
        visited.add(neighbor);
        if (entries.get(neighbor) == null) return;

        float d = distance(query, neighbor);
        int dInt = Float.floatToIntBits(d);
        if (result.size() < ef || d < Float.intBitsToFloat(result.peek()[1])) {
            candidates.add(new int[]{neighbor, dInt});
            result.add(new int[]{neighbor, dInt});
            if (result.size() > ef) {
                result.poll();
            }
        }
    }

    private List<Integer> selectNeighbors(List<int[]> candidates, int maxNeighbors) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < Math.min(candidates.size(), maxNeighbors); i++) {
            result.add(candidates.get(i)[0]);
        }
        return result;
    }

    private void shrinkNeighbors(int node, int level, int maxSize) {
        List<List<Integer>> nodeLayers = layers.get(node);
        if (nodeLayers == null || level >= nodeLayers.size()) return;
        List<Integer> neighbors = nodeLayers.get(level);
        if (neighbors.size() <= maxSize) return;

        float[] nodeVec = entries.get(node) != null ? entries.get(node).queryEmbedding() : null;
        if (nodeVec == null) return;

        neighbors.sort(Comparator.comparingDouble(n -> distance(nodeVec, n)));
        List<Integer> trimmed = new ArrayList<>(neighbors.subList(0, maxSize));
        nodeLayers.set(level, trimmed);
    }

    private float distance(float[] query, int nodeIdx) {
        SemanticCacheEntry entry = entries.get(nodeIdx);
        if (entry == null) return Float.MAX_VALUE;
        // Use 1 - cosine as distance
        return 1.0f - (float) cosineSimilarity(query, entry.queryEmbedding());
    }

    private int randomLevel() {
        return (int) (-Math.log(ThreadLocalRandom.current().nextDouble()) * ML);
    }

    private static boolean matchesTenant(SemanticCacheEntry entry, String tenantId) {
        if (tenantId == null) {
            return entry.tenantId() == null;
        }
        return tenantId.equals(entry.tenantId());
    }

    static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            return 0.0;
        }
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        // Unrolled loop for better JIT vectorization.
        int i = 0;
        int len = a.length;
        int limit = len - (len % 4);
        for (; i < limit; i += 4) {
            dot += (double) a[i] * b[i] + (double) a[i + 1] * b[i + 1]
                 + (double) a[i + 2] * b[i + 2] + (double) a[i + 3] * b[i + 3];
            normA += (double) a[i] * a[i] + (double) a[i + 1] * a[i + 1]
                   + (double) a[i + 2] * a[i + 2] + (double) a[i + 3] * a[i + 3];
            normB += (double) b[i] * b[i] + (double) b[i + 1] * b[i + 1]
                   + (double) b[i + 2] * b[i + 2] + (double) b[i + 3] * b[i + 3];
        }
        for (; i < len; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        double denominator = Math.sqrt(normA) * Math.sqrt(normB);
        return denominator > 0 ? dot / denominator : 0.0;
    }
}
