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

import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorIndexLoader;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.spi.observability.DoubleHistogram;
import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.MetricAttributes;
import org.pangrova.spi.observability.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Samples a configurable fraction of vector ANN search calls to estimate
 * retrieval quality by comparing ANN results against a brute-force baseline.
 *
 * <p>For each sampled call, runs a brute-force scan via the provided
 * {@link VectorIndexLoader} (which must honor {@code mode=brute_force} in
 * searchParams), then computes Recall@K using {@link RecallEstimator} and
 * records the results as Prometheus metrics.</p>
 *
 * <p>Thread-safe: all metric instruments are concurrency-safe and sampling
 * decisions use {@link ThreadLocalRandom}.</p>
 */
@ThreadSafe
public final class RetrievalQualitySampler {

    private static final Logger LOG = LoggerFactory.getLogger(RetrievalQualitySampler.class);

    static final double DEFAULT_SAMPLING_RATE = 0.01;
    static final double RECALL_DEGRADATION_THRESHOLD = 0.9;
    private static final int BRUTE_FORCE_TOPK_MULTIPLIER = 10;
    private static final int BRUTE_FORCE_TOPK_CAP = 10_000;
    private static final Map<String, String> BRUTE_FORCE_PARAMS = Map.of("mode", "brute_force");

    private static final List<Double> RECALL_BUCKETS = List.of(
            0.0, 0.5, 0.7, 0.8, 0.85, 0.9, 0.95, 0.99, 1.0);
    private static final List<Double> SPREAD_BUCKETS = List.of(
            0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0);

    private final VectorIndexLoader bruteForceLoader;
    private final double samplingRate;
    private final DoubleHistogram recallHistogram;
    private final DoubleHistogram scoreSpreadHistogram;
    private final LongCounter sampleCount;
    private final LongCounter degradationCount;

    /** Creates a sampler recording retrieval quality metrics at the given sampling rate. */
    public RetrievalQualitySampler(VectorIndexLoader bruteForceLoader,
                                    MetricsProvider metricsProvider,
                                    double samplingRate) {
        this.bruteForceLoader = Objects.requireNonNull(bruteForceLoader, "bruteForceLoader must not be null");
        Objects.requireNonNull(metricsProvider, "metricsProvider must not be null");
        if (samplingRate < 0.0 || samplingRate > 1.0) {
            throw new IllegalArgumentException("samplingRate must be in [0, 1]: " + samplingRate);
        }
        this.samplingRate = samplingRate;
        this.recallHistogram = metricsProvider.createHistogram(
                "pangrova.vector.recall",
                "Recall@K distribution for ANN search quality sampling",
                "1", RECALL_BUCKETS);
        this.scoreSpreadHistogram = metricsProvider.createHistogram(
                "pangrova.vector.score_spread",
                "Standard deviation of ANN search distance scores",
                "1", SPREAD_BUCKETS);
        this.sampleCount = metricsProvider.createCounter(
                "pangrova.vector.sample.total",
                "Total ANN quality sample evaluations", "samples");
        this.degradationCount = metricsProvider.createCounter(
                "pangrova.vector.degradation.total",
                "ANN recall samples below the degradation threshold", "events");
    }

    public RetrievalQualitySampler(VectorIndexLoader bruteForceLoader,
                                    MetricsProvider metricsProvider) {
        this(bruteForceLoader, metricsProvider, DEFAULT_SAMPLING_RATE);
    }

    /**
     * Evaluates ANN quality for the given result if this call is selected for sampling.
     *
     * <p>This method is safe to call on every search — it short-circuits immediately
     * for the (1 - samplingRate) fraction of non-sampled calls.</p>
     *
     * @param dataFilePath path of the searched data file
     * @param vectorColumn name of the vector column
     * @param queryVector  the query embedding
     * @param topK         the K value requested for Recall@K
     * @param metric       distance metric used for the search
     * @param annResult    the ANN result already returned to the caller
     */
    public void sampleIfEligible(String dataFilePath,
                                  String vectorColumn,
                                  float[] queryVector,
                                  int topK,
                                  DistanceMetric metric,
                                  VectorSearchResult annResult) {
        if (ThreadLocalRandom.current().nextDouble() >= samplingRate) {
            return;
        }
        try {
            runSample(dataFilePath, vectorColumn, queryVector, topK, metric, annResult);
        } catch (Exception e) {
            LOG.warn("event=recall_sample_failed file={} column={} reason={}",
                    dataFilePath, vectorColumn, e.getMessage());
        }
    }

    private void runSample(String dataFilePath, String vectorColumn,
                           float[] queryVector, int topK, DistanceMetric metric,
                           VectorSearchResult annResult) {
        MetricAttributes attrs = MetricAttributes.of("column", vectorColumn);
        sampleCount.add(1, attrs);

        List<Double> distances = annResult.neighbors().stream()
                .map(VectorSearchResult.ScoredRow::distance)
                .toList();
        if (!distances.isEmpty()) {
            scoreSpreadHistogram.record(RecallEstimator.scoreSpread(distances), attrs);
        }

        if (annResult.neighbors().isEmpty()) {
            return;
        }

        int bruteForceTopK = Math.min(topK * BRUTE_FORCE_TOPK_MULTIPLIER, BRUTE_FORCE_TOPK_CAP);
        VectorSearchRequest bruteForceRequest =
                new VectorSearchRequest(vectorColumn, queryVector, bruteForceTopK, metric, null);
        VectorSearchResult groundTruth = bruteForceLoader.searchFile(
                dataFilePath, vectorColumn, bruteForceRequest, BRUTE_FORCE_PARAMS);
        if (groundTruth.neighbors().isEmpty()) {
            return;
        }

        List<Long> annOffsets = annResult.neighbors().stream()
                .map(VectorSearchResult.ScoredRow::rowOffset)
                .toList();
        List<Long> truthOffsets = groundTruth.neighbors().stream()
                .map(VectorSearchResult.ScoredRow::rowOffset)
                .toList();

        double recall = RecallEstimator.recallAtK(annOffsets, truthOffsets, topK);
        recallHistogram.record(recall, attrs);
        LOG.debug("event=recall_sampled file={} column={} topK={} recall={}",
                dataFilePath, vectorColumn, topK, recall);

        if (recall < RECALL_DEGRADATION_THRESHOLD) {
            degradationCount.add(1, attrs);
            LOG.warn("event=recall_degraded file={} column={} topK={} recall={} threshold={}",
                    dataFilePath, vectorColumn, topK, recall, RECALL_DEGRADATION_THRESHOLD);
        }
    }
}
