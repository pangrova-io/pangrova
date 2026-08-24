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
package org.pangrova.coordinator.ai.index.lifecycle;

import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;

/**
 * Recommends the optimal vector index algorithm based on dataset characteristics
 * (vector count, dimensionality, available memory).
 *
 * <p>Algorithm routing:
 * <ul>
 *   <li>&lt;1M vectors, fits in memory: HNSW (best recall, simplest lifecycle)</li>
 *   <li>1M–100M vectors: IVFPQ (good recall/memory tradeoff)</li>
 *   <li>&gt;100M vectors or constrained memory: DISK_ANN (SSD-resident, minimal memory)</li>
 * </ul></p>
 */
@ThreadSafe
public final class IndexRecommendationEngine {

    private static final Logger LOG = LoggerFactory.getLogger(IndexRecommendationEngine.class);

    static final long HNSW_UPPER_BOUND = 1_000_000L;
    static final long BYTES_PER_FLOAT = 4L;

    /**
     * Recommends a vector algorithm for the given dataset characteristics.
     *
     * @param vectorCount     number of vectors to index
     * @param dimension       vector dimensionality
     * @param availableMemory available memory budget in bytes
     * @return recommendation, or empty if the table has no vectors
     */
    public Optional<AlgorithmRecommendation> recommend(long vectorCount, int dimension, long availableMemory) {
        if (vectorCount <= 0 || dimension <= 0) {
            return Optional.empty();
        }

        long hnswMemory = estimateHnswMemory(vectorCount, dimension);
        long ivfpqMemory = estimateIvfpqMemory(vectorCount, dimension);

        VectorAlgorithmType recommended;
        String reason;

        if (vectorCount <= HNSW_UPPER_BOUND && hnswMemory <= availableMemory) {
            recommended = VectorAlgorithmType.HNSW;
            reason = String.format("vectorCount=%d within HNSW range and memory fits (%d/%d bytes)",
                    vectorCount, hnswMemory, availableMemory);
        } else if (ivfpqMemory <= availableMemory) {
            recommended = VectorAlgorithmType.IVFPQ;
            reason = String.format("vectorCount=%d; IVFPQ fits memory budget (%d/%d bytes)",
                    vectorCount, ivfpqMemory, availableMemory);
        } else {
            // DiskANN is ideal for large-scale or memory-constrained datasets:
            // - Graph + PQ codes reside on SSD, only medoid in memory
            // - 10x memory reduction vs HNSW with ~95% recall@10
            recommended = VectorAlgorithmType.DISK_ANN;
            reason = String.format("vectorCount=%d exceeds in-memory budget "
                    + "(HNSW=%d, IVFPQ=%d, budget=%d bytes); recommending DISK_ANN for SSD-based ANN",
                    vectorCount, hnswMemory, ivfpqMemory, availableMemory);
        }

        LOG.info("event=algorithm_recommendation vectorCount={} dimension={} "
                        + "availableMemory={} recommended={} reason={}",
                vectorCount, dimension, availableMemory, recommended, reason);

        return Optional.of(new AlgorithmRecommendation(recommended, reason,
                hnswMemory, ivfpqMemory, vectorCount, dimension));
    }

    /**
     * Evaluates whether the current algorithm for an existing index is still
     * optimal given updated dataset statistics.
     *
     * @param descriptor      existing vector index descriptor
     * @param currentCount    current vector count (may have grown since build)
     * @param dimension       vector dimensionality
     * @param availableMemory available memory budget
     * @return recommendation to change algorithm, or empty if current is still optimal
     */
    public Optional<AlgorithmRecommendation> evaluateExisting(
            IndexDescriptor descriptor, long currentCount, int dimension, long availableMemory) {
        if (descriptor.indexType() != IndexType.VECTOR) {
            return Optional.empty();
        }
        String currentAlgo = descriptor.properties().getOrDefault("algorithm", "HNSW");
        VectorAlgorithmType current;
        try {
            current = VectorAlgorithmType.valueOf(currentAlgo.toUpperCase());
        } catch (IllegalArgumentException e) {
            current = VectorAlgorithmType.HNSW;
        }

        Optional<AlgorithmRecommendation> recommendation = recommend(currentCount, dimension, availableMemory);
        if (recommendation.isEmpty()) {
            return Optional.empty();
        }

        if (recommendation.get().algorithm() == current) {
            return Optional.empty();
        }

        LOG.info("event=algorithm_change_recommended indexId={} current={} recommended={} reason={}",
                descriptor.indexId(), current, recommendation.get().algorithm(),
                recommendation.get().reason());

        return recommendation;
    }

    private static long estimateHnswMemory(long vectorCount, int dimension) {
        int m = 16;
        return vectorCount * dimension * BYTES_PER_FLOAT + vectorCount * m * 2L * BYTES_PER_FLOAT;
    }

    private static long estimateIvfpqMemory(long vectorCount, int dimension) {
        int nlist = 256;
        int pqM = 8;
        int nbits = 8;
        int subDim = dimension / Math.max(pqM, 1);
        return vectorCount * dimension * BYTES_PER_FLOAT
                + (long) nlist * dimension * BYTES_PER_FLOAT
                + (long) pqM * (1L << nbits) * subDim * BYTES_PER_FLOAT;
    }

    /**
     * Algorithm recommendation with supporting data.
     *
     * @param algorithm         recommended algorithm
     * @param reason            human-readable explanation
     * @param estimatedHnswBytes  estimated HNSW memory requirement
     * @param estimatedIvfpqBytes estimated IVFPQ memory requirement
     * @param vectorCount       dataset vector count
     * @param dimension         vector dimensionality
     */
    public record AlgorithmRecommendation(
            VectorAlgorithmType algorithm,
            String reason,
            long estimatedHnswBytes,
            long estimatedIvfpqBytes,
            long vectorCount,
            int dimension) {

        public AlgorithmRecommendation {
            if (algorithm == null) {
                throw new IllegalArgumentException("algorithm required");
            }
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("reason required");
            }
        }
    }
}
