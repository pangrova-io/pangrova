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
package org.pangrova.testing.integration.e2e.index.vector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.index.VectorIndexE2ETestBase;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Functional Recall acceptance using actual stored FLOAT32 data and an independent arithmetic oracle. */
@Tag("diskann")
@DisplayName("Native ANN issued-binding, immutable-read and Recall acceptance")
class NativeAnnRecallE2ETest extends VectorIndexE2ETestBase {
    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int DIMENSION = 16;
    private static final int COUNT = 1_000;
    private static final int K = 20;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @Test
    @DisplayName("Real published DiskANN meets its declared Recall on the same unfiltered snapshot")
    void publishedAnnMustExecuteAndMeetIndependentRecall() {
        String fqn = CATALOG + "." + SCHEMA + ".t_ann_recall_" + System.nanoTime();
        String indexName = "idx_ann_recall";
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(" + DIMENSION + "))");
        // Seed controls only actual SQL input. Expected results are independently
        // recomputed from the values fetched through the real Native/Flight path.
        Random input = new Random(19_072_321L);
        for (int start = 0; start < COUNT; start += 100) {
            StringBuilder insert = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
            for (int row = start; row < start + 100; row++) {
                if (row != start) insert.append(", ");
                float[] values = new float[DIMENSION];
                for (int dimension = 0; dimension < DIMENSION; dimension++) {
                    values[dimension] = input.nextFloat() * 2 - 1;
                }
                insert.append('(').append(row + 1).append(", 'cat_").append((row + 1) % 4)
                        .append("', ").append(vectorSql(values)).append(')');
            }
            sqlRunner.execute(insert.toString());
        }
        var dataset = executeObserved("SELECT id, category, embedding FROM " + fqn);
        assertExact(dataset);
        assertThat(dataset.rows()).hasSize(COUNT);
        List<StoredVector> stored = dataset.rows().stream().map(row -> {
            var value = row.get("embedding");
            assertThat(value).isInstanceOf(ColumnValue.VectorValue.class);
            return new StoredVector(((Number) row.getRaw("id")).longValue(),
                    row.getRaw("category").toString(), ((ColumnValue.VectorValue) value).value());
        }).toList();
        assertThat(stored.stream().map(StoredVector::id)).doesNotHaveDuplicates();
        assertThat(stored.stream().map(row -> java.util.Arrays.toString(row.coordinates()))
                .distinct().count()).as("seeded Native vector rows retain distinct coordinates")
                .isGreaterThan(900);

        sqlRunner.executeDdl("CREATE INDEX " + indexName + " ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2',"
                + " 'search_list_size'='" + COUNT + "')");
        var published = awaitPublishedVectorIndex(indexName, fqn);
        var baselineSnapshots = dataset.tasks().stream().flatMap(task -> task.request().getSplitsList().stream())
                .filter(split -> split.getSourceId().equals(fqn)).map(split -> split.getSourceSnapshot())
                .collect(Collectors.toSet());
        assertThat(baselineSnapshots).containsExactly(published.manifest().sourceSnapshot());
        var rawObjects = published.outputs().stream()
                .filter(object -> object.objectRole().equals("RAW_VECTORS")).toList();
        assertThat(rawObjects).isNotEmpty();
        var coldRaw = rawObjects.stream().filter(object -> !java.nio.file.Files.exists(cachedObject(object))).toList();
        assertThat(coldRaw).as("actual cold Native read evidence; no synthetic receipt").isNotEmpty();

        // Four off-row queries prevent self-match-only acceptance. Every expected
        // top-K comes from this exact snapshot and the same L2/filter semantics.
        for (int query = 0; query < 4; query++) {
            float[] point = midpoint(stored.get(query * 97).coordinates(), stored.get(query * 137 + 1).coordinates());
            var evidence = executeObserved(searchSql(fqn, point, ""));
            double requiredRecall = assertAnn(published, evidence);
            assertThat(evidence.tasks().stream().flatMap(task -> task.request().getOperatorBindingsList().stream())
                    .filter(binding -> binding.getCapability().getCapabilityId().equals(
                            org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers.DISKANN_RETRIEVAL_ID)))
                    .allSatisfy(binding -> assertThat(binding.getExecutionPolicy().getApproximation().getRecallK())
                            .isEqualTo(K));
            assertDistanceAndRecall(stored, point, evidence.rows(), requiredRecall);
        }
        assertThat(coldRaw).allSatisfy(object -> assertThat(java.nio.file.Files.isRegularFile(cachedObject(object)))
                .as("real query materialized the previously absent immutable object").isTrue());

        float[] filteredPoint = midpoint(stored.get(4).coordinates(), stored.get(8).coordinates());
        var filtered = executeObserved(searchSql(fqn, filteredPoint, " WHERE category = 'cat_0'"));
        assertFilteredAnn(published, filtered);
        var filteredSnapshots = filtered.tasks().stream().flatMap(task -> task.request().getSplitsList().stream())
                .filter(split -> split.getSourceId().equals(fqn)).map(split -> split.getSourceSnapshot())
                .collect(Collectors.toSet());
        assertThat(filteredSnapshots).containsExactly(published.manifest().sourceSnapshot());
        assertDistanceAndRecall(stored.stream().filter(row -> row.category().equals("cat_0")).toList(),
                filteredPoint, filtered.rows(), 1.0);
        sqlRunner.executeDdl("DROP INDEX " + indexName + " ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }

    @Test
    @DisplayName("Filtered Native ANN preserves NULL/CASE eligibility and physical rows through real Flight")
    void filteredAnnMustUseActualEligibilityAndPhysicalRows() {
        String fqn = CATALOG + "." + SCHEMA + ".t_filtered_ann_" + System.nanoTime();
        String indexName = "idx_filtered_ann";
        sqlRunner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, category VARCHAR, embedding VECTOR(" + DIMENSION + "))");
        Random input = new Random(42_301_991L);
        for (int start = 0; start < 200; start += 100) {
            StringBuilder insert = new StringBuilder("INSERT INTO ").append(fqn).append(" VALUES ");
            for (int row = start; row < start + 100; row++) {
                if (row != start) insert.append(", ");
                float[] values = new float[DIMENSION];
                for (int dimension = 0; dimension < DIMENSION; dimension++) {
                    values[dimension] = input.nextFloat() * 2 - 1;
                }
                String category = row % 7 == 0 ? "NULL" : "'cat_" + (row % 4) + "'";
                insert.append('(').append(row + 1).append(", ").append(category)
                        .append(", ").append(vectorSql(values)).append(')');
            }
            sqlRunner.execute(insert.toString());
        }
        var dataset = executeObserved("SELECT id, category, embedding FROM " + fqn);
        assertExact(dataset);
        assertThat(dataset.rows()).hasSize(200);
        List<StoredVector> stored = dataset.rows().stream().map(row -> {
            var value = row.get("embedding");
            assertThat(value).isInstanceOf(ColumnValue.VectorValue.class);
            Object category = row.getRaw("category");
            return new StoredVector(((Number) row.getRaw("id")).longValue(),
                    category == null ? null : category.toString(), ((ColumnValue.VectorValue) value).value());
        }).toList();
        assertThat(stored.stream().map(StoredVector::id)).doesNotHaveDuplicates();
        // Full declared graph work is intentional for this small correctness cohort;
        // it is not a throughput/latency result or an optimal production setting.
        sqlRunner.executeDdl("CREATE INDEX " + indexName + " ON " + fqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2',"
                + " 'search_list_size'='200')");
        var published = awaitPublishedVectorIndex(indexName, fqn);
        assertDatasetSnapshot(dataset, published);
        long publishedSourceObjects = published.outputs().stream()
                .filter(output -> output.objectRole().equals("RAW_VECTORS"))
                .map(output -> output.sourceObjectId()).distinct().count();
        assertThat(publishedSourceObjects).isPositive();
        float[] point = midpoint(stored.get(11).coordinates(), stored.get(79).coordinates());
        List<EligibilityCase> cases = List.of(
                new EligibilityCase("category IS NULL", row -> row.category() == null),
                new EligibilityCase("category IS NOT NULL", row -> row.category() != null),
                new EligibilityCase("category = 'cat_0'", row -> "cat_0".equals(row.category())),
                new EligibilityCase("category = 'absent'", row -> false),
                new EligibilityCase("id <= 3", row -> row.id() <= 3),
                new EligibilityCase("category = 'cat_1' AND id < 130",
                        row -> "cat_1".equals(row.category()) && row.id() < 130),
                new EligibilityCase("category IS NULL OR category = 'cat_2'",
                        row -> row.category() == null || "cat_2".equals(row.category())),
                new EligibilityCase("CASE WHEN category IS NULL THEN TRUE"
                        + " WHEN category = 'cat_3' THEN id >= 50 ELSE FALSE END",
                        row -> row.category() == null || ("cat_3".equals(row.category()) && row.id() >= 50)));
        int annCases = 0;
        for (EligibilityCase eligibility : cases) {
            var evidence = executeObserved(searchSql(fqn, point, " WHERE " + eligibility.sql()));
            long taskSourceObjects = evidence.tasks().stream()
                    .flatMap(task -> task.request().getSplitsList().stream())
                    .filter(split -> split.getSourceId().equals(fqn))
                    .map(org.pangrova.protocol.v2.DataSplit::getDataObjectId).distinct().count();
            assertThat(taskSourceObjects).isLessThanOrEqualTo(publishedSourceObjects);
            var eligible = stored.stream().filter(eligibility.accepts()).toList();
            // Calcite's OR normalization is outside the current indexed
            // filter chain; CASE remains an indexed Native predicate.
            boolean exactPredicate = eligibility.sql().contains(" OR ");
            if (taskSourceObjects < publishedSourceObjects || exactPredicate) {
                assertThat(evidence.tasks().stream()
                        .flatMap(task -> task.request().getOperatorBindingsList().stream())
                        .filter(binding -> binding.getCapability().getCapabilityId().equals(
                                org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers.FILTERED_DISKANN_RETRIEVAL_ID))
                        .toList()).as("unsupported indexed scope must keep the exact scan").isEmpty();
                assertEligiblePopulation(eligible, point, evidence.rows(), 1.0);
                continue;
            }
            double required = assertFilteredAnn(published, evidence);
            annCases++;
            assertDatasetSnapshot(evidence, published);
            assertEligiblePopulation(eligible, point, evidence.rows(), required);
        }
        assertThat(annCases).as("full source coverage exercises the filtered Native ANN owner")
                .isGreaterThan(0);
        sqlRunner.executeDdl("DROP INDEX " + indexName + " ON " + fqn);
        sqlRunner.executeDdl("DROP TABLE " + fqn);
    }

    @Test
    @DisplayName("Filtered Native ANN preserves duplicate visible values as separate physical rows")
    void filteredAnnMustPreserveDuplicatePhysicalRows() {
        float[] point = new float[DIMENSION];
        // Duplicate visible values must remain two physical rows, even when the
        // client deliberately does not select any user-defined identity column.
        String duplicateFqn = CATALOG + "." + SCHEMA + ".t_filtered_duplicate_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + duplicateFqn
                + " (payload VARCHAR, category VARCHAR, embedding VECTOR(" + DIMENSION + "))");
        float[] left = point.clone();
        float[] right = point.clone();
        float[] other = point.clone();
        float[] fourth = point.clone();
        left[0] = 1;
        right[0] = -1;
        other[0] = 2;
        fourth[0] = 3;
        sqlRunner.execute("INSERT INTO " + duplicateFqn + " VALUES ('same', 'keep', "
                + vectorSql(left) + "), ('same', 'keep', " + vectorSql(right)
                + "), ('other', NULL, " + vectorSql(other) + "), ('fourth', NULL, " + vectorSql(fourth) + ")");
        sqlRunner.executeDdl("CREATE INDEX idx_filtered_duplicate ON " + duplicateFqn
                + "(embedding) USING VECTOR WITH ('algorithm'='DISK_ANN', 'metric'='L2',"
                + " 'max_degree'='3', 'search_list_size'='4', 'pq_bits_per_code'='2')");
        var duplicates = awaitPublishedVectorIndex("idx_filtered_duplicate", duplicateFqn);
        var duplicateRows = executeObserved("SELECT payload, l2_distance(embedding, " + vectorSql(point)
                + ") AS dist FROM " + duplicateFqn + " WHERE category = 'keep'"
                + " ORDER BY l2_distance(embedding, " + vectorSql(point) + ") ASC LIMIT 2");
        assertFilteredAnn(duplicates, duplicateRows);
        assertThat(duplicateRows.rows()).hasSize(2).allSatisfy(row -> {
            assertThat(row.getRaw("payload")).isEqualTo("same");
            assertThat(((Number) row.getRaw("dist")).doubleValue()).isEqualTo(1.0);
        });
        sqlRunner.executeDdl("DROP INDEX idx_filtered_duplicate ON " + duplicateFqn);
        sqlRunner.executeDdl("DROP TABLE " + duplicateFqn);
    }

    private static void assertEligiblePopulation(List<StoredVector> eligible, float[] point,
                                                List<TypedRow> actual, double required) {
        List<ExactDistance> expected = eligible.stream()
                .map(row -> new ExactDistance(row.id(), exactSquaredL2(row.coordinates(), point)))
                .sorted(Comparator.comparing(ExactDistance::squared).thenComparingLong(ExactDistance::id)).toList();
        int count = Math.min(K, expected.size());
        assertThat(actual).hasSize(count);
        if (count == 0) return;
        if (expected.size() > count) {
            assertThat(expected.get(count - 1).squared().compareTo(expected.get(count).squared())).isNegative();
        }
        Set<Long> top = expected.subList(0, count).stream().map(ExactDistance::id).collect(Collectors.toSet());
        Map<Long, BigDecimal> distances = expected.stream().collect(Collectors.toMap(
                ExactDistance::id, ExactDistance::squared));
        List<Long> ids = actual.stream().map(row -> ((Number) row.getRaw("id")).longValue()).toList();
        assertThat(ids).doesNotHaveDuplicates().allSatisfy(id -> assertThat(distances).containsKey(id));
        long hits = ids.stream().filter(top::contains).count();
        assertThat((double) hits / count).as("actual filtered same-snapshot Recall").isGreaterThanOrEqualTo(required);
        double previous = -1;
        for (TypedRow row : actual) {
            long id = ((Number) row.getRaw("id")).longValue();
            double exact = distances.get(id).sqrt(new MathContext(80)).doubleValue();
            double value = ((Number) row.getRaw("dist")).doubleValue();
            assertThat(Double.isFinite(value)).isTrue();
            assertThat(value).isCloseTo(exact, within(Math.max(1e-6, exact * 1e-5)));
            assertThat(value).isGreaterThanOrEqualTo(previous);
            previous = value;
        }
    }

    private record EligibilityCase(String sql, java.util.function.Predicate<StoredVector> accepts) {}

    private static String searchSql(String fqn, float[] point, String filter) {
        String vector = vectorSql(point);
        return "SELECT id, l2_distance(embedding, " + vector + ") AS dist FROM " + fqn + filter
                + " ORDER BY l2_distance(embedding, " + vector + ") ASC LIMIT " + K;
    }

    private static float[] midpoint(float[] left, float[] right) {
        assertThat(left).hasSize(DIMENSION);
        assertThat(right).hasSize(DIMENSION);
        float[] result = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++) result[i] = (left[i] + right[i]) / 2;
        return result;
    }

    private static String vectorSql(float[] values) {
        List<String> coordinates = new ArrayList<>(values.length);
        for (float value : values) {
            assertThat(Float.isFinite(value)).isTrue();
            coordinates.add(Float.toString(value));
        }
        return "ARRAY[" + String.join(",", coordinates) + "]";
    }

    private static void assertDistanceAndRecall(List<StoredVector> stored, float[] point,
                                               List<TypedRow> actual, double requiredRecall) {
        List<ExactDistance> expected = stored.stream()
                .map(row -> new ExactDistance(row.id(), exactSquaredL2(row.coordinates(), point)))
                .sorted(Comparator.comparing(ExactDistance::squared).thenComparingLong(ExactDistance::id))
                .toList();
        assertThat(expected).hasSizeGreaterThan(K);
        // Unique boundary makes the independent top-K set unambiguous.
        assertThat(expected.get(K - 1).squared().compareTo(expected.get(K).squared())).isNegative();
        Set<Long> topK = expected.subList(0, K).stream().map(ExactDistance::id).collect(Collectors.toSet());
        Map<Long, BigDecimal> distances = expected.stream().collect(Collectors.toMap(
                ExactDistance::id, ExactDistance::squared));
        assertThat(actual).hasSize(K);
        List<Long> actualIds = actual.stream().map(row -> ((Number) row.getRaw("id")).longValue()).toList();
        assertThat(actualIds).doesNotHaveDuplicates().allSatisfy(id -> assertThat(distances).containsKey(id));
        int hits = (int) actualIds.stream().filter(topK::contains).count();
        assertThat((double) hits / K).as("functional Recall@K for actual issued approximation profile")
                .isGreaterThanOrEqualTo(requiredRecall);
        double previous = -1;
        for (TypedRow row : actual) {
            long id = ((Number) row.getRaw("id")).longValue();
            double exact = distances.get(id).sqrt(new MathContext(80)).doubleValue();
            double value = ((Number) row.getRaw("dist")).doubleValue();
            assertThat(Double.isFinite(value)).isTrue();
            assertThat(value).isCloseTo(exact, within(Math.max(1e-6, exact * 1e-5)));
            assertThat(value).isGreaterThanOrEqualTo(previous);
            previous = value;
        }
    }

    private static BigDecimal exactSquaredL2(float[] left, float[] right) {
        assertThat(left).hasSize(right.length);
        BigDecimal sum = BigDecimal.ZERO;
        for (int i = 0; i < left.length; i++) {
            // The constructor preserves the exact binary32 value promoted to double;
            // decimal input regeneration and the engine's SIMD distance code are not reused.
            BigDecimal delta = new BigDecimal((double) left[i]).subtract(new BigDecimal((double) right[i]));
            sum = sum.add(delta.multiply(delta));
        }
        return sum;
    }

    private record StoredVector(long id, String category, float[] coordinates) {}
    private record ExactDistance(long id, BigDecimal squared) {}
}
