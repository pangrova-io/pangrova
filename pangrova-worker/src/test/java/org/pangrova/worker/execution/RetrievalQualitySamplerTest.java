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
import org.pangrova.spi.observability.LongUpDownCounter;
import org.pangrova.spi.observability.MetricAttributes;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class RetrievalQualitySamplerTest {

    @Test
    void perfectRecallDoesNotIncrementDegradation() {
        TrackingProvider tracking = new TrackingProvider();
        // Ground truth returns same rows as ANN → Recall@3 = 1.0
        VectorSearchResult groundTruth = new VectorSearchResult("f.parquet",
                List.of(row(1L, 0.1), row(2L, 0.2), row(3L, 0.3)));
        VectorSearchResult annResult = new VectorSearchResult("f.parquet",
                List.of(row(1L, 0.1), row(2L, 0.2), row(3L, 0.3)));

        RetrievalQualitySampler sampler = new RetrievalQualitySampler(
                new FixedLoader(groundTruth), tracking, 1.0);
        sampler.sampleIfEligible("f.parquet", "vec", new float[]{0.1f}, 3,
                DistanceMetric.COSINE, annResult);

        assertEquals(1, tracking.total("pangrova.vector.sample.total"), "One sample taken");
        assertEquals(0, tracking.total("pangrova.vector.degradation.total"), "No degradation");
    }

    @Test
    void belowRecallThresholdIncreasesDegradationCount() {
        TrackingProvider tracking = new TrackingProvider();
        // ANN result has zero overlap with ground truth → Recall@3 = 0.0
        VectorSearchResult groundTruth = new VectorSearchResult("f.parquet",
                List.of(row(1L, 0.1), row(2L, 0.2), row(3L, 0.3)));
        VectorSearchResult annResult = new VectorSearchResult("f.parquet",
                List.of(row(10L, 0.5), row(20L, 0.6), row(30L, 0.7)));

        RetrievalQualitySampler sampler = new RetrievalQualitySampler(
                new FixedLoader(groundTruth), tracking, 1.0);
        sampler.sampleIfEligible("f.parquet", "vec", new float[]{0.1f}, 3,
                DistanceMetric.COSINE, annResult);

        assertEquals(1, tracking.total("pangrova.vector.degradation.total"),
                "Zero recall should trigger degradation");
    }

    @Test
    void zeroSamplingRateNeverSamples() {
        TrackingProvider tracking = new TrackingProvider();
        VectorSearchResult result = new VectorSearchResult("f.parquet",
                List.of(row(1L, 0.1)));

        RetrievalQualitySampler sampler = new RetrievalQualitySampler(
                new FixedLoader(result), tracking, 0.0);

        for (int i = 0; i < 100; i++) {
            sampler.sampleIfEligible("f.parquet", "vec", new float[]{0.1f}, 1,
                    DistanceMetric.COSINE, result);
        }
        assertEquals(0, tracking.total("pangrova.vector.sample.total"),
                "Zero sampling rate should never trigger a sample");
    }

    @Test
    void emptyAnnResultSkipsBruteForceAndSampleIsStillCounted() {
        TrackingProvider tracking = new TrackingProvider();
        VectorSearchResult empty = new VectorSearchResult("f.parquet", List.of());
        RetrievalQualitySampler sampler = new RetrievalQualitySampler(
                new FixedLoader(empty), tracking, 1.0);

        // Empty ANN result: sampleCount incremented but bruteForce loader not needed
        sampler.sampleIfEligible("f.parquet", "vec", new float[]{0.5f}, 5,
                DistanceMetric.COSINE, empty);

        assertEquals(1, tracking.total("pangrova.vector.sample.total"),
                "sampleCount incremented even for empty ANN result");
        assertEquals(0, tracking.total("pangrova.vector.degradation.total"),
                "No degradation for empty result");
    }

    @Test
    void invalidSamplingRateThrows() {
        TrackingProvider tracking = new TrackingProvider();
        VectorSearchResult r = new VectorSearchResult("f.parquet", List.of());
        assertThrows(IllegalArgumentException.class,
                () -> new RetrievalQualitySampler(new FixedLoader(r), tracking, -0.1));
        assertThrows(IllegalArgumentException.class,
                () -> new RetrievalQualitySampler(new FixedLoader(r), tracking, 1.1));
    }

    @Test
    void nullLoaderThrows() {
        TrackingProvider tracking = new TrackingProvider();
        assertThrows(NullPointerException.class,
                () -> new RetrievalQualitySampler(null, tracking, 0.5));
    }

    @Test
    void defaultSamplingRateIsOnePct() {
        assertEquals(0.01, RetrievalQualitySampler.DEFAULT_SAMPLING_RATE, 1e-9);
    }

    // ---- helpers ----

    private static VectorSearchResult.ScoredRow row(long id, double dist) {
        return new VectorSearchResult.ScoredRow(id, dist);
    }

    private static final class FixedLoader implements VectorIndexLoader {
        private final VectorSearchResult result;
        FixedLoader(VectorSearchResult result) { this.result = result; }

        @Override
        public VectorSearchResult searchFile(String dataFilePath, String vectorColumn,
                                              VectorSearchRequest request,
                                              Map<String, String> searchParams) {
            return result;
        }
    }

    private static final class TrackingProvider implements MetricsProvider {
        private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

        long total(String name) {
            AtomicLong v = counters.get(name);
            return v == null ? 0L : v.get();
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            AtomicLong v = counters.computeIfAbsent(name, k -> new AtomicLong());
            return new LongCounter() {
                @Override public void add(long value) { v.addAndGet(value); }
                @Override public void add(long value, MetricAttributes attrs) { v.addAndGet(value); }
            };
        }

        @Override
        public LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
            AtomicLong v = counters.computeIfAbsent(name, k -> new AtomicLong());
            return v::addAndGet;
        }

        @Override
        public DoubleHistogram createHistogram(String name, String description,
                                               String unit, List<Double> boundaries) {
            return value -> {};
        }

        @Override
        public ObservableDoubleGauge createGauge(String name, String description,
                                                  String unit, Supplier<Double> supplier) {
            return new ObservableDoubleGauge() {};
        }
    }
}
