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
package org.pangrova.testing.integration.e2e.sql;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack E2E tests for advanced query features.
 *
 * <p>Validates QUALIFY, TABLESAMPLE, PIVOT, UNPIVOT, Table Functions,
 * CTAS (CREATE TABLE AS SELECT), and WITH RECURSIVE through the full pipeline.</p>
 */
@E2ETest
@DisplayName("Advanced Query Features Full-Stack E2E")
class AdvancedQueryFeaturesE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignTableName(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_adv_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // QUALIFY
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("QUALIFY Clause")
    class QualifyClause {

        @Test
        @DisplayName("QUALIFY with ROW_NUMBER filters window results")
        void qualifyRowNumber() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, dept VARCHAR, salary DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'eng', 100.0), (2, 'eng', 120.0), "
                    + "(3, 'sales', 90.0), (4, 'sales', 95.0), (5, 'eng', 110.0)");

            String sql = "SELECT id, dept, salary FROM " + fqn()
                    + " QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) <= 2";
            List<TypedRow> rows = executeQuery(sql);
            // Top-2 per dept: eng(2,5), sales(4,3) = 4 rows
            assertThat(rows).hasSize(4);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("QUALIFY with RANK for deduplication")
        void qualifyRank() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, ts BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'A', 100), (2, 'A', 200), (3, 'B', 150), (4, 'B', 300)");

            String sql = "SELECT id, category, ts FROM " + fqn()
                    + " QUALIFY RANK() OVER (PARTITION BY category ORDER BY ts DESC) = 1";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).hasSize(2);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // TABLESAMPLE
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("TABLESAMPLE")
    class TableSample {

        @Test
        @DisplayName("TABLESAMPLE BERNOULLI syntax")
        void tablesampleBernoulli() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),"
                    + "(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')");

            String sql = "SELECT * FROM " + fqn() + " TABLESAMPLE BERNOULLI(50)";
            List<TypedRow> rows = executeQuery(sql);
            // Probabilistic — just verify it returns some subset
            assertThat(rows.size()).isLessThanOrEqualTo(10);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("TABLESAMPLE SYSTEM syntax")
        void tablesampleSystem() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1),(2),(3),(4),(5)");

            String sql = "SELECT * FROM " + fqn() + " TABLESAMPLE SYSTEM(30)";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // PIVOT / UNPIVOT
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("PIVOT and UNPIVOT")
    class PivotUnpivot {

        @Test
        @DisplayName("PIVOT syntax transforms rows to columns")
        void pivotQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, quarter VARCHAR, revenue DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'Q1', 100.0), (1, 'Q2', 150.0), "
                    + "(2, 'Q1', 200.0), (2, 'Q2', 250.0)");

            String sql = "SELECT * FROM " + fqn()
                    + " PIVOT (SUM(revenue) FOR quarter IN ('Q1', 'Q2'))";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotEmpty();

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("UNPIVOT syntax transforms columns to rows")
        void unpivotQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, q1_rev DOUBLE, q2_rev DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 100.0, 150.0), (2, 200.0, 250.0)");

            String sql = "SELECT * FROM " + fqn()
                    + " UNPIVOT (revenue FOR quarter IN (q1_rev, q2_rev))";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).hasSize(4);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // CTAS (CREATE TABLE AS SELECT)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("CREATE TABLE AS SELECT (CTAS)")
    class Ctas {

        @Test
        @DisplayName("CTAS creates table from query result")
        void ctasBasic() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, score DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'Alice', 95.0), (2, 'Bob', 82.0), (3, 'Carol', 91.0)");

            String newTable = fqn() + "_ctas";
            executeDdl("CREATE TABLE " + newTable
                    + " AS SELECT id, name FROM " + fqn() + " WHERE score > 90");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + newTable);
            assertThat(rows).hasSize(2);

            executeDdl("DROP TABLE " + newTable);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CTAS with aggregation")
        void ctasWithAggregation() {
            executeDdl("CREATE TABLE " + fqn() + " (dept VARCHAR, salary DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "('eng', 100.0), ('eng', 120.0), ('sales', 90.0)");

            String newTable = fqn() + "_agg";
            executeDdl("CREATE TABLE " + newTable
                    + " AS SELECT dept, AVG(salary) AS avg_salary FROM " + fqn() + " GROUP BY dept");

            List<TypedRow> rows = executeQuery(
                    "SELECT dept, avg_salary FROM " + newTable + " ORDER BY dept");
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getString("dept")).isEqualTo("eng");
            assertThat(rows.get(0).getDouble("avg_salary")).isEqualTo(110.0);
            assertThat(rows.get(1).getString("dept")).isEqualTo("sales");
            assertThat(rows.get(1).getDouble("avg_salary")).isEqualTo(90.0);

            executeDdl("DROP TABLE " + newTable);
            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // WITH RECURSIVE (Recursive CTE)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("WITH RECURSIVE")
    class WithRecursive {

        @Test
        @DisplayName("WITH RECURSIVE generates sequence")
        void recursiveCteSequence() {
            String sql = "WITH RECURSIVE seq(n) AS ("
                    + " SELECT 1"
                    + " UNION ALL"
                    + " SELECT n + 1 FROM seq WHERE n < 5"
                    + ") SELECT n FROM seq";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).hasSize(5);
        }

        @Test
        @DisplayName("WITH RECURSIVE traverses hierarchy")
        void recursiveCteHierarchy() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, parent_id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, NULL, 'root'), (2, 1, 'child1'), "
                    + "(3, 1, 'child2'), (4, 2, 'grandchild1')");

            String sql = "WITH RECURSIVE tree(id, name, lvl) AS ("
                    + " SELECT id, name, 0 FROM " + fqn() + " WHERE parent_id IS NULL"
                    + " UNION ALL"
                    + " SELECT t.id, t.name, tree.lvl + 1"
                    + " FROM " + fqn() + " t JOIN tree ON t.parent_id = tree.id"
                    + ") SELECT * FROM tree";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).hasSize(4);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Table Functions (GENERATE_SERIES)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Table Functions")
    class TableFunctions {

        @Test
        @DisplayName("GENERATE_SERIES as table function")
        void generateSeries() {
            String sql = "SELECT * FROM GENERATE_SERIES(1, 10)";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).hasSize(10);
        }

        @Test
        @DisplayName("GENERATE_SERIES with step")
        void generateSeriesWithStep() {
            String sql = "SELECT * FROM GENERATE_SERIES(0, 20, 5)";
            List<TypedRow> rows = executeQuery(sql);
            // 0, 5, 10, 15, 20 = 5 rows
            assertThat(rows).hasSize(5);
        }
    }
}
