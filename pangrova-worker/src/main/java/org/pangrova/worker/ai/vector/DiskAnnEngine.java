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
package org.pangrova.worker.ai.vector;

import org.pangrova.spi.index.vector.DiskAnnIndexStrategy;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.worker.jni.NativeLibraryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * JNI bridge to the C++ DiskANN implementation (VamanaGraph / DiskAnnBuilder / DiskAnnSearcher).
 *
 * <p>Provides build and search operations for disk-based approximate nearest neighbor
 * indices that store the graph on SSD, keeping only PQ-compressed codes in memory.
 * This enables 10x memory reduction compared to in-memory HNSW at the cost of
 * slightly higher query latency (SSD I/O).</p>
 *
 * <p>The native library must be loaded before any method is called. Loading happens
 * automatically via {@code NativeLibraryLoader} during worker bootstrap.</p>
 */
public final class DiskAnnEngine implements DiskAnnIndexStrategy {

    static {
        NativeLibraryLoader.load();
    }

    private static final Logger LOG = LoggerFactory.getLogger(DiskAnnEngine.class);

    /** Background executor for index warm-up (lazy-load → eager-load on registration). */
    private static final ExecutorService WARM_UP_EXECUTOR = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "diskann-warmup");
        t.setDaemon(true);
        return t;
    });

    private final String dataDirectory;
    private final ConcurrentHashMap<String, LoadedShard> loadedShards = new ConcurrentHashMap<>();

    /** Describes a loaded index shard on this worker. */
    public record LoadedShard(String tableName, String vectorColumn,
                              String shardId, int generation, long rowCount, String indexPath) {}

    /** Nanoseconds per second, for duration conversion. */
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    /** Nanoseconds per millisecond, for latency conversion. */
    private static final double NANOS_PER_MILLI = 1_000_000.0;

    public DiskAnnEngine(String dataDirectory) {
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory required");
    }

    /** No-arg constructor for ServiceLoader instantiation. Uses temp directory. */
    public DiskAnnEngine() {
        this.dataDirectory = System.getProperty("java.io.tmpdir");
    }

    @Override
    public BuildResult buildIndex(float[][] vectors, int dimension, DistanceMetric metric,
                                  BuildParams params, String outputDirectory) {
        LOG.info("event=diskann_build_start vectors={} dim={} metric={} R={} L={}",
                vectors.length, dimension, metric, params.maxDegree(), params.buildListSize());
        long startNanos = System.nanoTime();
        byte[] resultBytes = nativeBuildIndex(
                vectors, dimension, metric.name(),
                params.maxDegree(), params.buildListSize(),
                params.pqSubvectors(), outputDirectory);
        double durationSec = (System.nanoTime() - startNanos) / NANOS_PER_SECOND;
        LOG.info("event=diskann_build_complete duration_sec={} outputDir={}",
                durationSec, outputDirectory);
        return decodeBuildResult(resultBytes);
    }

    @Override
    public List<VectorSearchResult.ScoredRow> search(String indexPath, float[] queryVector,
                                                      int topK, int searchListSize) {
        byte[] resultBytes = nativeSearch(indexPath, queryVector, topK, searchListSize);
        return decodeSearchResult(resultBytes);
    }

    /**
     * Searches with a pre-filter bitset. Only rows with their bit set in the bitset
     * are considered during ANN search, enabling efficient WHERE-clause pushdown.
     *
     * @param indexPath        path to the DiskANN index
     * @param queryVector      query vector
     * @param topK             number of nearest neighbors to return
     * @param searchListSize   beam width for graph traversal
     * @param preFilterBitset  bit-packed filter (null = no filter)
     * @return scored rows sorted by distance ascending
     */
    public List<VectorSearchResult.ScoredRow> searchFiltered(String indexPath, float[] queryVector,
                                                             int topK, int searchListSize,
                                                             byte[] preFilterBitset) {
        if (preFilterBitset == null || preFilterBitset.length == 0) {
            return search(indexPath, queryVector, topK, searchListSize);
        }
        byte[] resultBytes = nativeSearchFiltered(
                indexPath, queryVector, topK, searchListSize, preFilterBitset);
        return decodeSearchResult(resultBytes);
    }

    /**
     * Loads a DiskANN index from the specified storage path, making it available for search.
     * Called when the coordinator notifies this worker that a new index is ready.
     *
     * @param storagePath  path to the index files in shared storage
     * @param tableName    table the index belongs to
     * @param vectorColumn indexed vector column
     * @param shardId      shard identifier
     * @param generation   index generation
     * @param rowCount     number of rows in this shard
     */
    public void loadIndex(String storagePath, String tableName, String vectorColumn,
                          String shardId, int generation, long rowCount) {
        LOG.info("event=diskann_load_index path={} table={} column={} shard={} gen={}",
                storagePath, tableName, vectorColumn, shardId, generation);
        loadedShards.put(shardId, new LoadedShard(
                tableName, vectorColumn, shardId, generation, rowCount, storagePath));

        // Eager warm-up: trigger native index load in background so first search
        // doesn't pay the cold-start penalty (disk I/O for graph + PQ codes).
        WARM_UP_EXECUTOR.submit(() -> {
            try {
                LOG.info("event=diskann_warmup_start shard={} path={}", shardId, storagePath);
                long start = System.nanoTime();
                nativeWarmUp(storagePath);
                double ms = (System.nanoTime() - start) / NANOS_PER_MILLI;
                LOG.info("event=diskann_warmup_complete shard={} latency_ms={}", shardId, ms);
            } catch (Exception e) {
                LOG.warn("event=diskann_warmup_failed shard={} error={}", shardId, e.getMessage());
            }
        });
    }

    /**
     * Returns all currently loaded shards for heartbeat reporting.
     */
    public List<LoadedShard> getLoadedShards() {
        return List.copyOf(loadedShards.values());
    }

    private static BuildResult decodeBuildResult(byte[] bytes) {
        if (bytes == null || bytes.length < 16) {
            return new BuildResult(0, 0, 0);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long vectorCount = buf.getLong();
        int graphSizeBytes = buf.getInt();
        int pqCodeSizeBytes = buf.getInt();
        return new BuildResult(vectorCount, graphSizeBytes, pqCodeSizeBytes);
    }

    private static List<VectorSearchResult.ScoredRow> decodeSearchResult(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            return List.of();
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        List<VectorSearchResult.ScoredRow> results = new ArrayList<>(count);
        for (int i = 0; i < count && buf.remaining() >= 16; i++) {
            long rowId = buf.getLong();
            double distance = buf.getDouble();
            results.add(new VectorSearchResult.ScoredRow(rowId, distance));
        }
        return results;
    }

    private static native byte[] nativeBuildIndex(
            float[][] vectors, int dimension, String metric,
            int maxDegree, int buildListSize, int pqSubvectors,
            String outputDirectory);

    private static native byte[] nativeSearch(
            String indexPath, float[] queryVector, int topK, int searchListSize);

    private static native byte[] nativeSearchFiltered(
            String indexPath, float[] queryVector, int topK, int searchListSize,
            byte[] preFilterBitset);

    /**
     * Triggers native index loading into the LRU cache without performing a search.
     * Used for eager warm-up on shard registration to eliminate cold-start latency.
     */
    private static native void nativeWarmUp(String indexPath);
}
