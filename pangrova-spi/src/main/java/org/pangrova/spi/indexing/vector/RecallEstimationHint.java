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
package org.pangrova.spi.indexing.vector;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Provides recall rate estimation for vector search queries.
 *
 * <p>Used by the query planner to dynamically adjust topK amplification
 * when estimated recall is below the acceptable threshold. This avoids
 * the need for cross-module dependencies on worker-side estimators.</p>
 *
 * <p>The estimation uses empirical formulas based on algorithm parameters
 * and index statistics, without requiring a live index connection.</p>
 */
@Experimental(reason = "Recall estimation for adaptive topK amplification")
public interface RecallEstimationHint {

    double DEFAULT_RECALL = 0.9;
    double HNSW_MAX_RECALL = 0.99;
    double HNSW_BASE_RECALL = 0.95;
    double HNSW_TOP_K_FACTOR = 0.01;
    double IVF_PQ_MIN_RECALL = 0.7;
    double IVF_PQ_BASE_RECALL = 0.85;
    double IVF_PQ_DIMENSION_FACTOR = 0.02;
    double DISK_ANN_MAX_RECALL = 0.98;
    double DISK_ANN_BASE_RECALL = 0.90;
    double DISK_ANN_TOP_K_FACTOR = 0.005;

    /**
     * Estimates the recall rate for a given vector search configuration.
     *
     * @param topK       requested number of nearest neighbors
     * @param dimension  vector dimensionality
     * @param algorithm  canonical index algorithm name ("HNSW", "IVF_PQ", or "DISK_ANN")
     * @param vectorCount approximate number of vectors in the index
     * @return estimated recall rate between 0.0 and 1.0
     */
    double estimateRecall(int topK, int dimension, String algorithm, long vectorCount);

    /**
     * Default estimation based on empirical formulas.
     * - HNSW: ~0.95 for typical parameters
     * - IVF_PQ: ~0.85 for typical parameters, degrades with high dimension
     * - DISK_ANN: ~0.90 for typical parameters
     */
    static RecallEstimationHint defaultEstimator() {
        return (topK, dimension, algorithm, vectorCount) -> {
            if (algorithm == null) {
                return DEFAULT_RECALL;
            }
            return switch (algorithm.toUpperCase()) {
                case "HNSW" -> Math.min(
                        HNSW_MAX_RECALL, HNSW_BASE_RECALL + HNSW_TOP_K_FACTOR * Math.log10(topK));
                case "IVF_PQ" -> Math.max(
                        IVF_PQ_MIN_RECALL,
                        IVF_PQ_BASE_RECALL - IVF_PQ_DIMENSION_FACTOR * (dimension / 128.0));
                case "DISK_ANN" -> Math.min(
                        DISK_ANN_MAX_RECALL,
                        DISK_ANN_BASE_RECALL + DISK_ANN_TOP_K_FACTOR * Math.log10(topK));
                default -> DEFAULT_RECALL;
            };
        };
    }
}
