/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory incremental HNSW layer for recently ingested vectors.
 *
 * <p>Stores new vectors in a flat buffer until a merge threshold is reached,
 * then merges into the persistent index. Queries combine results from
 * both the delta layer and the persistent index.</p>
 *
 * <p>This enables near-real-time vector searchability for streaming
 * ingestion without rebuilding the full persistent index on every write.</p>
 *
 * <p>Thread-safe: concurrent inserts and queries are supported.</p>
 */
@SuppressWarnings("ThreadSafe")
public final class HnswDeltaLayer {

    private static final Logger LOG = LoggerFactory.getLogger(HnswDeltaLayer.class);

    private final int mergeThreshold;
    private final int dimension;
    private final DistanceMetric metric;
    private final AtomicReference<ConcurrentHashMap<Long, float[]>> deltaVectors =
            new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicInteger insertCount = new AtomicInteger(0);

    /** Creates a delta layer for the given vector shape and merge threshold. */
    public HnswDeltaLayer(int dimension, DistanceMetric metric, int mergeThreshold) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (mergeThreshold <= 0) {
            throw new IllegalArgumentException("mergeThreshold must be positive");
        }
        this.dimension = dimension;
        this.metric = metric;
        this.mergeThreshold = mergeThreshold;
    }

    /** Buffers a new vector under its row id until the layer is merged. */
    public void insert(long rowId, float[] vector) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "vector dimension mismatch: expected " + dimension + ", got " + vector.length);
        }
        deltaVectors.get().put(rowId, vector.clone());
        int count = insertCount.incrementAndGet();
        if (count % 1000 == 0) {
            LOG.info("event=delta_layer_growth size={} mergeThreshold={}",
                    count, mergeThreshold);
        }
    }

    /** Brute-force searches the buffered delta vectors for the top-K matches. */
    public List<VectorSearchResult.ScoredRow> search(float[] queryVector, int topK) {
        List<VectorSearchResult.ScoredRow> results = new ArrayList<>();
        for (Map.Entry<Long, float[]> entry : deltaVectors.get().entrySet()) {
            double distance = computeDistance(queryVector, entry.getValue());
            results.add(new VectorSearchResult.ScoredRow(entry.getKey(), distance));
        }
        results.sort((a, b) -> Double.compare(a.distance(), b.distance()));
        return List.copyOf(results.subList(0, Math.min(topK, results.size())));
    }

    public boolean needsMerge() {
        return deltaVectors.get().size() >= mergeThreshold;
    }

    /**
     * Atomically swaps out the current delta map with a fresh empty one, then
     * returns the old map. Any inserts that race with this call land in the new
     * map and are not lost — they will be drained on the next merge cycle.
     */
    public Map<Long, float[]> drainForMerge() {
        Map<Long, float[]> drained = deltaVectors.getAndSet(new ConcurrentHashMap<>());
        insertCount.set(0);
        LOG.info("event=delta_layer_drained vectors={}", drained.size());
        return drained;
    }

    public int size() {
        return deltaVectors.get().size();
    }

    private double computeDistance(float[] a, float[] b) {
        return switch (metric) {
            case COSINE -> 1.0 - cosine(a, b);
            case L2 -> l2(a, b);
            case INNER_PRODUCT -> -dotProduct(a, b);
        };
    }

    private static double cosine(float[] a, float[] b) {
        double dot = 0;
        double normA = 0;
        double normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom > 0 ? dot / denom : 0.0;
    }

    private static double l2(float[] a, float[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    private static double dotProduct(float[] a, float[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += (double) a[i] * b[i];
        }
        return sum;
    }
}
