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
package org.pangrova.coordinator.ai.layout;

import org.pangrova.planner.core.advisor.QueryPatternRecorder;
import org.pangrova.planner.core.advisor.QueryPatternRecorder.QueryPattern;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recommends data layout optimizations (partition keys, sort orders) based
 * on observed query patterns.
 *
 * <p>Analyzes column usage frequency across recorded queries and recommends
 * the most impactful layout changes. Requires a minimum sample size before
 * producing recommendations to avoid overfitting to early workload.</p>
 */
public final class DataLayoutAdvisor {

    private static final Logger LOG = LoggerFactory.getLogger(DataLayoutAdvisor.class);
    static final int MIN_SAMPLE_SIZE = 50;
    /** Write-ratio above which layout recommendations are withheld. */
    private static final double HIGH_WRITE_RATIO_THRESHOLD = 0.3;
    /** Minimum sample size required before a recommendation is applied. */
    private static final int MIN_CONFIDENT_SAMPLE_SIZE = 200;

    private final QueryPatternRecorder recorder;

    /** Creates an advisor drawing statistics from the given query pattern recorder. */
    public DataLayoutAdvisor(QueryPatternRecorder recorder) {
        if (recorder == null) {
            throw new IllegalArgumentException("recorder required");
        }
        this.recorder = recorder;
    }

    /**
     * Produces a layout recommendation for the given table.
     *
     * @param tableName fully qualified table name
     * @return recommendation if enough patterns exist, empty otherwise
     */
    public Optional<LayoutRecommendation> recommend(String tableName) {
        List<QueryPattern> patterns = recorder.getPatternsForTable(tableName);
        if (patterns.size() < MIN_SAMPLE_SIZE) {
            LOG.debug("event=layout_advisor_skip table={} patterns={} minRequired={}",
                    tableName, patterns.size(), MIN_SAMPLE_SIZE);
            return Optional.empty();
        }

        Map<String, Integer> filterFrequency = new HashMap<>();
        Map<String, Integer> sortFrequency = new HashMap<>();
        long writeCount = 0;

        for (QueryPattern p : patterns) {
            if (p.isWrite()) {
                writeCount++;
            }
            for (String col : p.filterColumns()) {
                filterFrequency.merge(col, 1, Integer::sum);
            }
            for (String col : p.sortColumns()) {
                sortFrequency.merge(col, 1, Integer::sum);
            }
        }

        String recommendedPartitionKey = topColumn(filterFrequency);
        String recommendedSortKey = topColumn(sortFrequency);
        double writeRatio = (double) writeCount / patterns.size();

        var recommendation = new LayoutRecommendation(
                tableName,
                recommendedPartitionKey,
                recommendedSortKey,
                filterFrequency,
                writeRatio,
                patterns.size());

        LOG.info("event=layout_recommendation table={} partitionKey={} sortKey={} "
                        + "writeRatio={} patternCount={}",
                tableName, recommendedPartitionKey, recommendedSortKey,
                writeRatio, patterns.size());

        return Optional.of(recommendation);
    }

    private static String topColumn(Map<String, Integer> frequency) {
        return frequency.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    /**
     * Evaluates and returns a recommendation if it meets the confidence threshold.
     *
     * <p>Auto-apply conditions:
     * <ul>
     *   <li>writeRatio < 0.3 (read-heavy workload)</li>
     *   <li>sampleSize > 200 (sufficient data)</li>
     *   <li>top column frequency > 60% of all queries</li>
     * </ul>
     *
     * @param tableName     fully qualified table name
     * @param minConfidence minimum confidence threshold (0.0 to 1.0)
     * @return recommendation if confidence met, empty otherwise
     */
    public Optional<LayoutRecommendation> applyIfConfident(String tableName, double minConfidence) {
        Optional<LayoutRecommendation> rec = recommend(tableName);
        if (rec.isEmpty()) {
            return Optional.empty();
        }
        LayoutRecommendation r = rec.get();

        // Confidence guards
        if (r.writeRatio() >= HIGH_WRITE_RATIO_THRESHOLD) {
            LOG.debug("event=layout_advisor_skip_high_write_ratio table={} writeRatio={}",
                    tableName, r.writeRatio());
            return Optional.empty();
        }
        if (r.sampleSize() <= MIN_CONFIDENT_SAMPLE_SIZE) {
            LOG.debug("event=layout_advisor_skip_low_sample table={} sampleSize={}",
                    tableName, r.sampleSize());
            return Optional.empty();
        }

        // Check top column dominance
        String topCol = r.recommendedPartitionKey();
        if (topCol != null && r.filterColumnFrequency().containsKey(topCol)) {
            double dominance = (double) r.filterColumnFrequency().get(topCol) / r.sampleSize();
            if (dominance < minConfidence) {
                LOG.debug("event=layout_advisor_skip_low_confidence table={} dominance={} threshold={}",
                        tableName, dominance, minConfidence);
                return Optional.empty();
            }
        }

        LOG.info("event=layout_advisor_auto_apply table={} partitionKey={} sortKey={} confidence_met=true",
                tableName, r.recommendedPartitionKey(), r.recommendedSortKey());
        return rec;
    }

    /**
     * A data layout optimization recommendation.
     *
     * @param tableName             target table
     * @param recommendedPartitionKey most frequently filtered column (best partition key)
     * @param recommendedSortKey    most frequently sorted column (best sort key)
     * @param filterColumnFrequency frequency map of filter column usage
     * @param writeRatio            fraction of queries that are writes (0.0 to 1.0)
     * @param sampleSize            number of query patterns analyzed
     */
    public record LayoutRecommendation(
            String tableName,
            String recommendedPartitionKey,
            String recommendedSortKey,
            Map<String, Integer> filterColumnFrequency,
            double writeRatio,
            int sampleSize) {

        public LayoutRecommendation {
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("tableName required");
            }
            filterColumnFrequency = filterColumnFrequency != null
                    ? Map.copyOf(filterColumnFrequency) : Map.of();
        }
    }
}
