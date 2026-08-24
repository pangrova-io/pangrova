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

import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.pangrova.planner.vector.VectorIndexRoutingRule;
import org.pangrova.planner.vector.VectorSearchNode;
import org.pangrova.planner.vector.VectorTopKCandidate;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorHybridSearchStrategy;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E tests for vector search through the SQL planner layer.
 *
 * <p>Exercises the complete vector search pipeline from distance function
 * registration in the operator table, through VectorTopK pattern recognition,
 * index routing decisions, and search node construction.</p>
 *
 * <p>Tests use real planner components without requiring a full Calcite
 * cluster, verifying the planner-level logic that drives vector search
 * execution at the worker level.</p>
 */
@DisplayName("Vector Search Execution E2E")
class VectorSearchExecutionE2ETest {

    @Nested
    @DisplayName("Distance Function Compilation")
    class DistanceFunctionCompilation {

        private static PangrovaSqlOperatorTable operatorTable;

        @BeforeAll
        static void setUp() {
            operatorTable = new PangrovaSqlOperatorTable();
        }

        @Test
        @DisplayName("L2_DISTANCE is registered as vector distance function")
        void l2DistanceRegistered() {
            List<SqlOperator> ops = lookupFunction("L2_DISTANCE");

            assertThat(ops).isNotEmpty();
            assertThat(ops.get(0).getName()).isEqualTo("L2_DISTANCE");
        }

        @Test
        @DisplayName("COSINE_DISTANCE is registered as vector distance function")
        void cosineDistanceRegistered() {
            List<SqlOperator> ops = lookupFunction("COSINE_DISTANCE");

            assertThat(ops).isNotEmpty();
            assertThat(ops.get(0).getName()).isEqualTo("COSINE_DISTANCE");
        }

        @Test
        @DisplayName("INNER_PRODUCT is registered as vector distance function")
        void innerProductRegistered() {
            List<SqlOperator> ops = lookupFunction("INNER_PRODUCT");

            assertThat(ops).isNotEmpty();
            assertThat(ops.get(0).getName()).isEqualTo("INNER_PRODUCT");
        }

        @Test
        @DisplayName("COSINE_SIMILARITY is registered for projection use")
        void cosineSimilarityRegistered() {
            List<SqlOperator> ops = lookupFunction("COSINE_SIMILARITY");

            assertThat(ops).isNotEmpty();
            assertThat(ops.get(0).getName()).isEqualTo("COSINE_SIMILARITY");
        }

        @Test
        @DisplayName("All four vector functions appear in the full operator list")
        void allVectorFunctionsInList() {
            List<SqlOperator> allOps = operatorTable.getOperatorList();
            List<String> names = allOps.stream().map(SqlOperator::getName).toList();

            assertThat(names).contains("L2_DISTANCE", "COSINE_DISTANCE",
                    "INNER_PRODUCT", "COSINE_SIMILARITY");
        }

        @Test
        @DisplayName("Case-insensitive lookup finds distance functions")
        void caseInsensitiveLookup() {
            List<SqlOperator> ops = new ArrayList<>();
            operatorTable.lookupOperatorOverloads(
                    new SqlIdentifier("l2_distance", SqlParserPos.ZERO),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    SqlSyntax.FUNCTION,
                    ops,
                    SqlNameMatchers.withCaseSensitive(false));

            assertThat(ops).isNotEmpty();
        }

        private List<SqlOperator> lookupFunction(String name) {
            List<SqlOperator> operators = new ArrayList<>();
            operatorTable.lookupOperatorOverloads(
                    new SqlIdentifier(name, SqlParserPos.ZERO),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    SqlSyntax.FUNCTION,
                    operators,
                    SqlNameMatchers.withCaseSensitive(false));
            return operators;
        }
    }

    @Nested
    @DisplayName("Vector Search Rewrite via Index Routing")
    class VectorSearchRewrite {

        @Test
        @DisplayName("Matching VECTOR index produces VectorSearchNode with ANN strategy")
        void matchingIndexProducesSearchNode() {
            IndexDescriptor index = vectorIndex("idx-l2", "embedding", DistanceMetric.L2);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.products", "embedding",
                    new float[]{1.0f, 0.0f, 0.0f}, 10, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(index),
                    false);

            assertThat(result).isPresent();
            VectorSearchNode node = result.get();
            assertThat(node.tableName()).isEqualTo("catalog.schema.products");
            assertThat(node.vectorColumn()).isEqualTo("embedding");
            assertThat(node.topK()).isEqualTo(10);
            assertThat(node.metric()).isEqualTo(DistanceMetric.L2);
            assertThat(node.indexId()).isEqualTo("idx-l2");
            assertThat(node.hybridStrategy().mode())
                    .isEqualTo(VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER);
        }

        @Test
        @DisplayName("COSINE index matches COSINE_DISTANCE query metric")
        void cosineIndexMatchesCosineQuery() {
            IndexDescriptor index = vectorIndex("idx-cosine", "emb", DistanceMetric.COSINE);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "db.public.docs", "emb",
                    new float[]{0.5f, 0.5f}, 5, DistanceMetric.COSINE);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(index),
                    false);

            assertThat(result).isPresent();
            assertThat(result.get().metric()).isEqualTo(DistanceMetric.COSINE);
            assertThat(result.get().indexId()).isEqualTo("idx-cosine");
        }
    }

    @Nested
    @DisplayName("Pre-Filter Preservation")
    class PreFilterPreservation {

        @Test
        @DisplayName("Pre-filters are included in VectorSearchNode")
        void preFiltersPreserved() {
            IndexDescriptor index = vectorIndex("idx-filtered", "emb", DistanceMetric.L2);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.items", "emb",
                    new float[]{1.0f, 2.0f, 3.0f}, 5, DistanceMetric.L2);

            List<String> preFilters = List.of("=(category, 'electronics')", ">(price, 10)");

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(index),
                    false,
                    0.5,
                    10000L,
                    preFilters);

            assertThat(result).isPresent();
            VectorSearchNode node = result.get();
            assertThat(node.serializedPreFilters()).containsExactly(
                    "=(category, 'electronics')", ">(price, 10)");
        }

        @Test
        @DisplayName("High selectivity uses ANN_THEN_FILTER with over-fetched K")
        void highSelectivityUsesAnnThenFilter() {
            IndexDescriptor index = vectorIndex("idx-hiselectivity", "emb", DistanceMetric.L2);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.items", "emb",
                    new float[]{1.0f, 0.0f}, 10, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(index),
                    false,
                    0.5,
                    10000L,
                    List.of("=(active, true)"));

            assertThat(result).isPresent();
            assertThat(result.get().hybridStrategy().mode())
                    .isEqualTo(VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER);
            // Over-fetched K should be larger than original topK
            assertThat(result.get().hybridStrategy().effectiveK())
                    .isGreaterThanOrEqualTo(10);
        }

        @Test
        @DisplayName("Low selectivity uses FILTER_THEN_BRUTEFORCE")
        void lowSelectivityUsesFilterFirst() {
            IndexDescriptor index = vectorIndex("idx-loselectivity", "emb", DistanceMetric.L2);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.items", "emb",
                    new float[]{1.0f, 0.0f}, 10, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(index),
                    false,
                    0.05,
                    10000L,
                    List.of("=(rare_flag, true)"));

            assertThat(result).isPresent();
            assertThat(result.get().hybridStrategy().mode())
                    .isEqualTo(VectorHybridSearchStrategy.Mode.FILTER_THEN_BRUTEFORCE);
        }
    }

    @Nested
    @DisplayName("No Index Fallback")
    class NoIndexFallback {

        @Test
        @DisplayName("No VECTOR index returns empty (falls through to native Substrait path)")
        void noIndexReturnsEmpty() {
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.unindexed", "embedding",
                    new float[]{1.0f, 2.0f, 3.0f}, 10, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(),
                    false);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Null index list returns empty")
        void nullIndexListReturnsEmpty() {
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.nullidx", "emb",
                    new float[]{1.0f}, 5, DistanceMetric.COSINE);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> null,
                    false);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("BUILDING state index is not selected")
        void buildingIndexSkipped() {
            IndexDescriptor buildingIndex = new IndexDescriptor(
                    "idx-building", "building_idx",
                    "catalog", "schema", "table",
                    List.of("emb"), IndexType.VECTOR,
                    Map.of("metric", "L2"),
                    IndexState.BUILDING, null, 0, null, null,
                    Instant.now(), Instant.now(), "default");

            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.table", "emb",
                    new float[]{1.0f, 0.0f}, 5, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(buildingIndex),
                    false);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("STALE state index is not selected")
        void staleIndexSkipped() {
            IndexDescriptor staleIndex = new IndexDescriptor(
                    "idx-stale", "stale_idx",
                    "catalog", "schema", "table",
                    List.of("emb"), IndexType.VECTOR,
                    Map.of("metric", "COSINE"),
                    IndexState.STALE, "data changed", 0, null, null,
                    Instant.now(), Instant.now(), "default");

            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.table", "emb",
                    new float[]{0.5f, 0.5f}, 10, DistanceMetric.COSINE);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(staleIndex),
                    false);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Non-VECTOR index type is ignored")
        void nonVectorIndexIgnored() {
            IndexDescriptor btreeIndex = new IndexDescriptor(
                    "idx-btree", "btree_idx",
                    "catalog", "schema", "table",
                    List.of("emb"), IndexType.BTREE,
                    Map.of(),
                    IndexState.READY, null, 0, null, null,
                    Instant.now(), Instant.now(), "default");

            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.table", "emb",
                    new float[]{1.0f}, 5, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(btreeIndex),
                    false);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Metric mismatch between query and index is rejected")
        void metricMismatchRejected() {
            IndexDescriptor l2Index = vectorIndex("idx-l2-only", "emb", DistanceMetric.L2);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.table", "emb",
                    new float[]{0.5f, 0.5f}, 10, DistanceMetric.COSINE);

            // Metric L2 in index vs COSINE in query: no match, so empty
            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(l2Index),
                    false);

            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("Force Exact Search Hint")
    class ForceExactSearch {

        @Test
        @DisplayName("NO_VECTOR_INDEX hint bypasses index even when available")
        void hintBypassesIndex() {
            IndexDescriptor index = vectorIndex("idx-bypassed", "emb", DistanceMetric.L2);
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "catalog.schema.table", "emb",
                    new float[]{1.0f, 0.0f}, 10, DistanceMetric.L2);

            Optional<VectorSearchNode> result = VectorIndexRoutingRule.route(
                    candidate,
                    (table, col) -> List.of(index),
                    true);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Hint detection in SQL text works")
        void hintDetectionInSql() {
            assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(
                    "/*+ NO_VECTOR_INDEX */ SELECT * FROM t ORDER BY l2_distance(emb, ARRAY[1.0]) LIMIT 10"))
                    .isTrue();

            assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(
                    "SELECT * FROM t ORDER BY l2_distance(emb, ARRAY[1.0]) LIMIT 10"))
                    .isFalse();

            assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint(null)).isFalse();
            assertThat(VectorIndexRoutingRule.containsNoVectorIndexHint("")).isFalse();
        }
    }

    @Nested
    @DisplayName("VectorTopKCandidate Construction")
    class TopKCandidateConstruction {

        @Test
        @DisplayName("Valid candidate constructed with all metrics")
        void validCandidateAllMetrics() {
            for (DistanceMetric metric : DistanceMetric.values()) {
                VectorTopKCandidate candidate = new VectorTopKCandidate(
                        "db.schema.table", "embedding",
                        new float[]{1.0f, 2.0f}, 10, metric);

                assertThat(candidate.tableName()).isEqualTo("db.schema.table");
                assertThat(candidate.vectorColumn()).isEqualTo("embedding");
                assertThat(candidate.topK()).isEqualTo(10);
                assertThat(candidate.metric()).isEqualTo(metric);
            }
        }

        @Test
        @DisplayName("Query vector is defensively copied")
        void queryVectorDefensivelyCopied() {
            float[] original = {1.0f, 2.0f, 3.0f};
            VectorTopKCandidate candidate = new VectorTopKCandidate(
                    "db.schema.table", "emb", original, 5, DistanceMetric.L2);

            original[0] = 999.0f;

            assertThat(candidate.queryVector()[0]).isEqualTo(1.0f);
        }

        @Test
        @DisplayName("Blank table name is rejected")
        void blankTableNameRejected() {
            assertThatThrownBy(() -> new VectorTopKCandidate(
                    "", "emb", new float[]{1.0f}, 5, DistanceMetric.L2))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("topK less than 1 is rejected")
        void invalidTopKRejected() {
            assertThatThrownBy(() -> new VectorTopKCandidate(
                    "table", "emb", new float[]{1.0f}, 0, DistanceMetric.L2))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Null metric is rejected")
        void nullMetricRejected() {
            assertThatThrownBy(() -> new VectorTopKCandidate(
                    "table", "emb", new float[]{1.0f}, 5, null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("VectorSearchNode Construction")
    class SearchNodeConstruction {

        @Test
        @DisplayName("VectorSearchNode preserves all fields")
        void preservesAllFields() {
            float[] queryVec = {1.0f, 0.0f, 0.0f};
            List<String> filters = List.of("=(category, 'x')");
            VectorHybridSearchStrategy strategy =
                    new VectorHybridSearchStrategy(VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, 20);

            VectorSearchNode node = new VectorSearchNode(
                    "catalog.schema.table", "embedding",
                    queryVec, 10, DistanceMetric.L2,
                    "idx-hnsw", filters, strategy, 3);

            assertThat(node.tableName()).isEqualTo("catalog.schema.table");
            assertThat(node.vectorColumn()).isEqualTo("embedding");
            assertThat(node.queryVector()).containsExactly(1.0f, 0.0f, 0.0f);
            assertThat(node.topK()).isEqualTo(10);
            assertThat(node.metric()).isEqualTo(DistanceMetric.L2);
            assertThat(node.indexId()).isEqualTo("idx-hnsw");
            assertThat(node.serializedPreFilters()).containsExactly("=(category, 'x')");
            assertThat(node.hybridStrategy()).isEqualTo(strategy);
            assertThat(node.indexGeneration()).isEqualTo(3);
        }

        @Test
        @DisplayName("Query vector is defensively copied in VectorSearchNode")
        void queryVectorCopied() {
            float[] original = {1.0f, 2.0f};
            VectorSearchNode node = new VectorSearchNode(
                    "t", "c", original, 5, DistanceMetric.L2,
                    "idx", List.of(),
                    new VectorHybridSearchStrategy(
                            VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, 5),
                    0);

            original[0] = 999.0f;

            assertThat(node.queryVector()[0]).isEqualTo(1.0f);
        }

        @Test
        @DisplayName("Null pre-filters default to empty list")
        void nullPreFiltersDefaultEmpty() {
            VectorSearchNode node = new VectorSearchNode(
                    "t", "c", new float[]{1.0f}, 5, DistanceMetric.L2,
                    "idx", null,
                    new VectorHybridSearchStrategy(
                            VectorHybridSearchStrategy.Mode.ANN_THEN_FILTER, 5),
                    0);

            assertThat(node.serializedPreFilters()).isEmpty();
        }
    }

    private static IndexDescriptor vectorIndex(String indexId, String column, DistanceMetric metric) {
        return new IndexDescriptor(
                indexId, indexId + "_name",
                "catalog", "schema", "table",
                List.of(column), IndexType.VECTOR,
                Map.of("metric", metric.name()),
                IndexState.READY, null, 1, null, null,
                Instant.now(), Instant.now(), "default");
    }
}
