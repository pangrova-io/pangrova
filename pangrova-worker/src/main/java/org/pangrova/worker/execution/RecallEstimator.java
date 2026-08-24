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
package org.pangrova.worker.execution;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Computes retrieval quality metrics by comparing ANN results against
 * a brute-force ground truth baseline.
 *
 * <p>Stateless utility — all methods are static and thread-safe.</p>
 */
public final class RecallEstimator {

    private RecallEstimator() {}

    /**
     * Computes Recall@K: the fraction of ground-truth top-K results
     * that appear in the ANN result set.
     *
     * @param annRowIds    row IDs returned by the ANN search
     * @param groundTruth  row IDs returned by brute-force search (the baseline)
     * @param k            the K value for Recall@K
     * @return recall in [0.0, 1.0]
     */
    public static double recallAtK(List<Long> annRowIds, List<Long> groundTruth, int k) {
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive, got: " + k);
        }
        if (groundTruth == null) {
            throw new IllegalArgumentException("groundTruth must not be null");
        }
        if (groundTruth.isEmpty()) {
            return 1.0;
        }
        int effectiveK = Math.min(k, groundTruth.size());
        Set<Long> truthSet = new HashSet<>(groundTruth.subList(0, effectiveK));
        int hits = 0;
        for (int i = 0; i < annRowIds.size(); i++) {
            if (truthSet.contains(annRowIds.get(i))) {
                hits++;
            }
        }
        return (double) hits / effectiveK;
    }

    /**
     * Computes the standard deviation of distance scores, indicating
     * how spread out the result distances are.
     *
     * @param distances the distance scores from search results
     * @return standard deviation, or 0.0 if fewer than 2 values
     */
    public static double scoreSpread(List<Double> distances) {
        if (distances.size() < 2) {
            return 0.0;
        }
        double mean = distances.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = distances.stream()
                .mapToDouble(d -> (d - mean) * (d - mean))
                .average().orElse(0.0);
        return Math.sqrt(variance);
    }
}
