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
package org.pangrova.indexing.api.vector;

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

}
