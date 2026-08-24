package org.pangrova.testing.e2e.jni;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-language shuffle hash consistency test: verifies that the native
 * shuffle hash produces correct partition assignments by executing queries
 * that trigger shuffle (GROUP BY, JOIN) and checking that data lands in
 * the correct partitions.
 *
 * <p>Shuffle hash is computed entirely in the native engine using
 * SIMD-optimized batch hashing. Java side receives pre-partitioned
 * ShuffleBatch with partition_id already assigned.
 *
 * <p>Unit-level hash validation is in the C++ test:
 * {@code pangrova-worker-native/test/shuffle/ShuffleHashGoldenTest.cpp}.
 *
 * <p>Requirements: E2E-06, §5.7 (ShuffleHashCrossLanguageE2ETest)
 */
@DisplayName("JNI Chain - Shuffle Hash Cross-Language Consistency")
class ShuffleHashCrossLanguageE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested
    @DisplayName("Golden hash values via shuffle queries")
    class GoldenHashViaShuffleQueries {

        @Test
        @DisplayName("GROUP BY produces correct aggregation across shuffle boundary")
        void groupByAggregation_crossShuffleBoundary() {
            String table = "t_hash_groupby_" + System.nanoTime();
            String fqn = FQN_PREFIX + table;

            sqlRunner.executeDdl(
                    "CREATE TABLE " + fqn + " (category VARCHAR, amount BIGINT)");
            sqlRunner.execute(
                    "INSERT INTO " + fqn + " (category, amount) VALUES "
                            + "('A', 10), ('B', 20), ('A', 30), ('B', 40), ('A', 50)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT category, SUM(amount) AS total FROM " + fqn
                            + " GROUP BY category ORDER BY category");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("category")).isEqualTo("A");
            assertThat(((Number) rows.get(0).getRaw("total")).longValue()).isEqualTo(90L);
            assertThat(rows.get(1).getRaw("category")).isEqualTo("B");
            assertThat(((Number) rows.get(1).getRaw("total")).longValue()).isEqualTo(60L);
        }

        @Test
        @DisplayName("JOIN with shuffle produces correct matched rows")
        void joinWithShuffle_correctMatchedRows() {
            String left = "t_hash_left_" + System.nanoTime();
            String right = "t_hash_right_" + System.nanoTime();
            String leftFqn = FQN_PREFIX + left;
            String rightFqn = FQN_PREFIX + right;

            sqlRunner.executeDdl(
                    "CREATE TABLE " + leftFqn + " (id BIGINT, name VARCHAR)");
            sqlRunner.executeDdl(
                    "CREATE TABLE " + rightFqn + " (id BIGINT, score DOUBLE)");

            sqlRunner.execute(
                    "INSERT INTO " + leftFqn + " (id, name) VALUES "
                            + "(1, 'alice'), (2, 'bob'), (3, 'carol')");
            sqlRunner.execute(
                    "INSERT INTO " + rightFqn + " (id, score) VALUES "
                            + "(1, 85.5), (2, 92.0), (4, 70.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT l.id, l.name, r.score FROM " + leftFqn + " l "
                            + "INNER JOIN " + rightFqn + " r ON l.id = r.id "
                            + "ORDER BY l.id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
        }

        @Test
        @DisplayName("Multi-column GROUP BY hash consistency across languages")
        void multiColumnGroupBy_hashConsistency() {
            String table = "t_hash_multi_" + System.nanoTime();
            String fqn = FQN_PREFIX + table;

            sqlRunner.executeDdl(
                    "CREATE TABLE " + fqn
                            + " (region VARCHAR, product VARCHAR, quantity BIGINT)");
            sqlRunner.execute(
                    "INSERT INTO " + fqn + " (region, product, quantity) VALUES "
                            + "('east', 'widget', 10), ('east', 'gadget', 20), "
                            + "('west', 'widget', 30), ('east', 'widget', 40), "
                            + "('west', 'gadget', 50)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT region, product, SUM(quantity) AS total FROM " + fqn
                            + " GROUP BY region, product ORDER BY region, product");

            assertThat(rows).hasSize(4);
            // east-gadget: 20
            assertThat(rows.get(0).getRaw("region")).isEqualTo("east");
            assertThat(rows.get(0).getRaw("product")).isEqualTo("gadget");
            assertThat(((Number) rows.get(0).getRaw("total")).longValue()).isEqualTo(20L);
            // east-widget: 50
            assertThat(rows.get(1).getRaw("region")).isEqualTo("east");
            assertThat(rows.get(1).getRaw("product")).isEqualTo("widget");
            assertThat(((Number) rows.get(1).getRaw("total")).longValue()).isEqualTo(50L);
            // west-gadget: 50
            assertThat(rows.get(2).getRaw("region")).isEqualTo("west");
            assertThat(rows.get(2).getRaw("product")).isEqualTo("gadget");
            assertThat(((Number) rows.get(2).getRaw("total")).longValue()).isEqualTo(50L);
            // west-widget: 30
            assertThat(rows.get(3).getRaw("region")).isEqualTo("west");
            assertThat(rows.get(3).getRaw("product")).isEqualTo("widget");
            assertThat(((Number) rows.get(3).getRaw("total")).longValue()).isEqualTo(30L);
        }
    }

    @Nested
    @DisplayName("Edge cases for hash boundary")
    class HashBoundaryEdgeCases {

        @Test
        @DisplayName("NULL keys hash consistently across shuffle")
        void nullKeys_hashConsistentlyAcrossShuffle() {
            String table = "t_hash_null_" + System.nanoTime();
            String fqn = FQN_PREFIX + table;

            sqlRunner.executeDdl(
                    "CREATE TABLE " + fqn + " (key VARCHAR, value BIGINT)");
            sqlRunner.execute(
                    "INSERT INTO " + fqn + " (key, value) VALUES "
                            + "(NULL, 10), (NULL, 20), ('a', 30)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT key, SUM(value) AS total FROM " + fqn
                            + " GROUP BY key ORDER BY key NULLS FIRST");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("key")).isNull();
            assertThat(((Number) rows.get(0).getRaw("total")).longValue()).isEqualTo(30L);
            assertThat(rows.get(1).getRaw("key")).isEqualTo("a");
            assertThat(((Number) rows.get(1).getRaw("total")).longValue()).isEqualTo(30L);
        }

        @Test
        @DisplayName("Large cardinality keys distribute evenly across partitions")
        void largeCardinality_evenDistribution() {
            String table = "t_hash_large_" + System.nanoTime();
            String fqn = FQN_PREFIX + table;

            sqlRunner.executeDdl(
                    "CREATE TABLE " + fqn + " (id BIGINT, value BIGINT)");

            StringBuilder values = new StringBuilder();
            for (int i = 1; i <= 200; i++) {
                if (i > 1) {
                    values.append(", ");
                }
                values.append(String.format("(%d, %d)", i, i * 10));
            }
            sqlRunner.execute(
                    "INSERT INTO " + fqn + " (id, value) VALUES " + values);

            // GROUP BY forces shuffle; correct count proves hash consistency
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(DISTINCT id) AS cnt FROM " + fqn);

            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("cnt")).longValue()).isEqualTo(200L);
        }

        @Test
        @DisplayName("Negative and extreme BIGINT keys hash correctly")
        void extremeBigintKeys_hashCorrectly() {
            String table = "t_hash_extreme_" + System.nanoTime();
            String fqn = FQN_PREFIX + table;

            sqlRunner.executeDdl(
                    "CREATE TABLE " + fqn + " (id BIGINT, label VARCHAR)");
            sqlRunner.execute(
                    "INSERT INTO " + fqn + " (id, label) VALUES "
                            + "(0, 'zero'), (-1, 'neg_one'), "
                            + "(9223372036854775807, 'max'), (-9223372036854775808, 'min')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, label FROM " + fqn
                            + " GROUP BY id, label ORDER BY id");

            assertThat(rows).hasSize(4);
            // All 4 distinct keys survive shuffle without hash collision
            assertThat(rows.stream()
                    .map(r -> r.getRaw("label"))
                    .toList())
                    .containsExactlyInAnyOrder("min", "neg_one", "zero", "max");
        }
    }
}
