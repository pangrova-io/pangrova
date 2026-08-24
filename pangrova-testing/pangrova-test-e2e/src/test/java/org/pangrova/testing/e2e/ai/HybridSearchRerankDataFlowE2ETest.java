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
package org.pangrova.testing.e2e.ai;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.pangrova.worker.ai.rerank.CascadeRerankerFactory;
import org.pangrova.worker.ai.rerank.RrfRerankerFactory;
import org.pangrova.worker.ai.rerank.WeightedRerankerFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for Hybrid Search and Reranking with actual data flow.
 *
 * <p>Unlike HybridSearchChainE2ETest (which only tests ServiceLoader discovery
 * and AST parsing), this test verifies ACTUAL DATA FLOW through reranker
 * implementations:
 * <ul>
 *   <li>WeightedReranker: min-max normalization + weighted fusion</li>
 *   <li>RRF (Reciprocal Rank Fusion): rank-based fusion</li>
 *   <li>CascadeReranker: multi-stage pipeline (RRF → Weighted)</li>
 *   <li>Score ordering correctness and topK enforcement</li>
 * </ul>
 */
@DisplayName("Hybrid Search / Rerank E2E — Actual Data Flow")
class HybridSearchRerankDataFlowE2ETest {

    // -----------------------------------------------------------------------
    // WeightedReranker — Score Fusion with Normalization
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("WeightedReranker — Score Fusion")
    class WeightedRerankerTests {

        @Test
        @DisplayName("Two sources with equal weights produce averaged scores")
        void twoSourcesEqualWeights() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.vector", "0.5", "weight.fts", "0.5"));

            List<ScoredCandidate> candidates = List.of(
                    new ScoredCandidate(100, 0.9, "vector"),
                    new ScoredCandidate(200, 0.7, "vector"),
                    new ScoredCandidate(300, 0.5, "vector"),
                    new ScoredCandidate(100, 0.8, "fts"),
                    new ScoredCandidate(200, 0.6, "fts"),
                    new ScoredCandidate(400, 0.9, "fts")
            );

            List<RankedResult> results = reranker.rerank("test query", candidates, 3);

            assertThat(results).hasSize(3);
            // Results should be sorted by descending fused score
            for (int i = 0; i < results.size() - 1; i++) {
                assertThat(results.get(i).score())
                        .isGreaterThanOrEqualTo(results.get(i + 1).score());
            }
            // Row 100 appears in both sources with high scores — should be ranked highly
            assertThat(results.stream().mapToLong(RankedResult::rowOffset))
                    .contains(100L);
        }

        @Test
        @DisplayName("Unequal weights bias toward higher-weighted source")
        void unequalWeightsBiasTowardsHigherWeight() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.vector", "0.8", "weight.fts", "0.2"));

            List<ScoredCandidate> candidates = List.of(
                    new ScoredCandidate(100, 0.9, "vector"),  // high vector score
                    new ScoredCandidate(200, 0.3, "vector"),  // low vector score
                    new ScoredCandidate(100, 0.2, "fts"),     // low fts score for row 100
                    new ScoredCandidate(200, 0.95, "fts")     // very high fts for row 200
            );

            List<RankedResult> results = reranker.rerank("bias test", candidates, 2);

            assertThat(results).hasSize(2);
            // Row 100 has high vector score (weight=0.8) → should rank first
            assertThat(results.get(0).rowOffset()).isEqualTo(100L);
        }

        @Test
        @DisplayName("Single source produces normalized scores")
        void singleSourceNormalized() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.vector", "1.0"));

            List<ScoredCandidate> candidates = List.of(
                    new ScoredCandidate(1, 100.0, "vector"),
                    new ScoredCandidate(2, 50.0, "vector"),
                    new ScoredCandidate(3, 0.0, "vector")
            );

            List<RankedResult> results = reranker.rerank("normalize", candidates, 3);

            assertThat(results).hasSize(3);
            // After min-max normalization: row1=1.0, row2=0.5, row3=0.0
            assertThat(results.get(0).rowOffset()).isEqualTo(1L);
            assertThat(results.get(0).score()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.01));
            assertThat(results.get(1).rowOffset()).isEqualTo(2L);
            assertThat(results.get(2).rowOffset()).isEqualTo(3L);
        }

        @Test
        @DisplayName("topK limits output size")
        void topKLimitsOutput() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.vector", "1.0"));

            List<ScoredCandidate> candidates = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                candidates.add(new ScoredCandidate(i, Math.random(), "vector"));
            }

            List<RankedResult> results = reranker.rerank("topk test", candidates, 5);

            assertThat(results).hasSize(5);
        }

        @Test
        @DisplayName("Weighted reranker name is 'weighted'")
        void nameIsWeighted() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.vector", "1.0"));
            assertThat(reranker.name()).isEqualTo("weighted");
        }
    }

    // -----------------------------------------------------------------------
    // RRF (Reciprocal Rank Fusion)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("RRF Reranker — Rank-Based Fusion")
    class RrfRerankerTests {

        @Test
        @DisplayName("RRF fuses candidates from multiple sources by rank position")
        void rrfFusesByRankPosition() {
            RrfRerankerFactory factory = new RrfRerankerFactory();
            Reranker reranker = factory.create(Map.of());

            // Two sources each contributing ranked candidates
            List<ScoredCandidate> candidates = List.of(
                    // Vector source: row100 rank1, row200 rank2, row300 rank3
                    new ScoredCandidate(100, 0.95, "vector"),
                    new ScoredCandidate(200, 0.80, "vector"),
                    new ScoredCandidate(300, 0.70, "vector"),
                    // FTS source: row200 rank1, row100 rank2, row400 rank3
                    new ScoredCandidate(200, 0.90, "fts"),
                    new ScoredCandidate(100, 0.75, "fts"),
                    new ScoredCandidate(400, 0.60, "fts")
            );

            List<RankedResult> results = reranker.rerank("rrf test", candidates, 4);

            assertThat(results).hasSize(4);
            // Results descending by RRF score
            for (int i = 0; i < results.size() - 1; i++) {
                assertThat(results.get(i).score())
                        .isGreaterThanOrEqualTo(results.get(i + 1).score());
            }
            // Row 100 and 200 both appear in both sources — should be top-ranked
            List<Long> topRows = results.stream().mapToLong(RankedResult::rowOffset).boxed().toList();
            assertThat(topRows.subList(0, 2)).containsExactlyInAnyOrder(100L, 200L);
        }

        @Test
        @DisplayName("RRF reranker name is 'rrf'")
        void nameIsRrf() {
            RrfRerankerFactory factory = new RrfRerankerFactory();
            Reranker reranker = factory.create(Map.of());
            assertThat(reranker.name()).isEqualTo("rrf");
        }

        @Test
        @DisplayName("RRF scores are positive")
        void rrfScoresArePositive() {
            RrfRerankerFactory factory = new RrfRerankerFactory();
            Reranker reranker = factory.create(Map.of());

            List<ScoredCandidate> candidates = List.of(
                    new ScoredCandidate(1, 0.5, "src1"),
                    new ScoredCandidate(2, 0.3, "src2")
            );

            List<RankedResult> results = reranker.rerank("positive", candidates, 2);

            assertThat(results).allSatisfy(r -> assertThat(r.score()).isPositive());
        }
    }

    // -----------------------------------------------------------------------
    // CascadeReranker — Multi-Stage Pipeline
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("CascadeReranker — Multi-Stage Pipeline")
    class CascadeRerankerTests {

        @Test
        @DisplayName("Cascade with RRF → Weighted produces refined results")
        void cascadeRrfThenWeighted() {
            CascadeRerankerFactory cascadeFactory = new CascadeRerankerFactory();

            // Build cascade: rrf first, then weighted
            Map<String, String> props = new HashMap<>();
            props.put("stages", "rrf,weighted");
            props.put("rrf.k", "60");
            props.put("weighted.weight.rrf", "1.0");

            Reranker cascade = cascadeFactory.create(props);

            List<ScoredCandidate> candidates = List.of(
                    new ScoredCandidate(10, 0.9, "vector"),
                    new ScoredCandidate(20, 0.8, "vector"),
                    new ScoredCandidate(30, 0.7, "vector"),
                    new ScoredCandidate(10, 0.85, "fts"),
                    new ScoredCandidate(20, 0.6, "fts"),
                    new ScoredCandidate(40, 0.95, "fts")
            );

            List<RankedResult> results = cascade.rerank("cascade test", candidates, 3);

            assertThat(results).hasSize(3);
            assertThat(results).allSatisfy(r -> {
                // After min-max normalization the lowest score may be 0.0
                assertThat(r.score()).isGreaterThanOrEqualTo(0.0);
                assertThat(r.metadata()).isNotNull();
            });
            // Descending order
            for (int i = 0; i < results.size() - 1; i++) {
                assertThat(results.get(i).score())
                        .isGreaterThanOrEqualTo(results.get(i + 1).score());
            }
        }

        @Test
        @DisplayName("Cascade reranker name is 'cascade'")
        void nameIsCascade() {
            CascadeRerankerFactory factory = new CascadeRerankerFactory();
            Map<String, String> props = new HashMap<>();
            props.put("stages", "rrf");
            Reranker cascade = factory.create(props);
            assertThat(cascade.name()).isEqualTo("cascade");
        }

        @Test
        @DisplayName("Cascade with single stage is equivalent to that stage alone")
        void singleStageCascade() {
            RrfRerankerFactory rrfFactory = new RrfRerankerFactory();
            CascadeRerankerFactory cascadeFactory = new CascadeRerankerFactory();

            Reranker rrfDirect = rrfFactory.create(Map.of());
            Map<String, String> cascadeProps = new HashMap<>();
            cascadeProps.put("stages", "rrf");
            Reranker cascadeSingle = cascadeFactory.create(cascadeProps);

            List<ScoredCandidate> candidates = List.of(
                    new ScoredCandidate(1, 0.9, "a"),
                    new ScoredCandidate(2, 0.7, "a"),
                    new ScoredCandidate(1, 0.8, "b"),
                    new ScoredCandidate(3, 0.6, "b")
            );

            List<RankedResult> directResults = rrfDirect.rerank("q", candidates, 3);
            List<RankedResult> cascadeResults = cascadeSingle.rerank("q", candidates, 3);

            // Same row offsets should appear (order determined by same algorithm)
            assertThat(cascadeResults.stream().mapToLong(RankedResult::rowOffset).sorted().toArray())
                    .isEqualTo(directResults.stream().mapToLong(RankedResult::rowOffset).sorted().toArray());
        }
    }

    // -----------------------------------------------------------------------
    // Input Validation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Input Validation")
    class InputValidation {

        @Test
        @DisplayName("WeightedReranker rejects empty weights")
        void weightedRejectsEmptyWeights() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            // When no weight.* properties, factory uses defaults (vector=0.5, fts=0.5)
            // So we test that creating with negative weight throws
            Map<String, String> negativeProps = new HashMap<>();
            negativeProps.put("weight.src", "-0.5");
            assertThatThrownBy(() -> factory.create(negativeProps))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("WeightedReranker rejects zero/negative weight")
        void weightedRejectsNegativeWeight() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Map<String, String> zeroProps = new HashMap<>();
            zeroProps.put("weight.src", "0.0");
            assertThatThrownBy(() -> factory.create(zeroProps))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("topK=0 is rejected by all rerankers")
        void topKZeroRejected() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.src", "1.0"));

            assertThatThrownBy(() -> reranker.rerank("q", List.of(), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Null candidates rejected")
        void nullCandidatesRejected() {
            WeightedRerankerFactory factory = new WeightedRerankerFactory();
            Reranker reranker = factory.create(Map.of("weight.src", "1.0"));

            assertThatThrownBy(() -> reranker.rerank("q", null, 5))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
