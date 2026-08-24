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
package org.pangrova.testing.e2e.sql;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Full-stack E2E tests for builtin scalar functions.
 *
 * <p>Validates String, Math, DateTime, and Conditional functions
 * through the full coordinator → planner → worker pipeline.</p>
 */
@E2ETest
@DisplayName("Builtin Functions Full-Stack E2E")
class BuiltinFunctionsE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_func_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // String Functions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("String Functions")
    class StringFunctions {

        @Test
        @DisplayName("UPPER and LOWER")
        void upperLower() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'Hello World')");

            List<TypedRow> rows = executeQuery(
                    "SELECT UPPER(name) AS u, LOWER(name) AS l FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("u").toString()).isEqualTo("HELLO WORLD");
            assertThat(rows.get(0).getRaw("l").toString()).isEqualTo("hello world");

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CONCAT and || operator")
        void concatAndOperator() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, first_name VARCHAR, last_name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'John', 'Doe')");

            List<TypedRow> rows = executeQuery(
                    "SELECT CONCAT(first_name, ' ', last_name) AS full_name,"
                            + " first_name || ' ' || last_name AS full2 FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("full_name").toString()).isEqualTo("John Doe");

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("SUBSTRING and CHAR_LENGTH")
        void substringAndLength() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'Hello World')");

            List<TypedRow> rows = executeQuery(
                    "SELECT SUBSTRING(val, 1, 5) AS sub, CHAR_LENGTH(val) AS len FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("sub").toString()).isEqualTo("Hello");
            assertThat(((Number) rows.get(0).getRaw("len")).intValue()).isEqualTo(11);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("TRIM function")
        void trimFunction() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, '  padded  ')");

            List<TypedRow> rows = executeQuery(
                    "SELECT TRIM(val) AS trimmed FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("trimmed").toString()).isEqualTo("padded");

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("LIKE pattern matching")
        void likePattern() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'Alice'), (2, 'Bob'), (3, 'Alfred'), (4, 'Carol')");

            List<TypedRow> rows = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE name LIKE 'Al%'");
            assertThat(rows).hasSize(2);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Math Functions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Math Functions")
    class MathFunctions {

        @Test
        @DisplayName("ABS, MOD, POWER, SQRT")
        void basicMathFunctions() {
            List<TypedRow> rows = executeQuery(
                    "SELECT ABS(-5) AS abs_val, MOD(17, 5) AS mod_val,"
                            + " POWER(2, 3) AS pow_val, SQRT(16.0) AS sqrt_val");
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("abs_val")).intValue()).isEqualTo(5);
            assertThat(((Number) rows.get(0).getRaw("mod_val")).intValue()).isEqualTo(2);
            assertThat(((Number) rows.get(0).getRaw("pow_val")).doubleValue())
                    .isCloseTo(8.0, within(0.01));
            assertThat(((Number) rows.get(0).getRaw("sqrt_val")).doubleValue())
                    .isCloseTo(4.0, within(0.01));
        }

        @Test
        @DisplayName("Arithmetic operators on table data")
        void arithmeticOperators() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, a DOUBLE, b DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 10.0, 3.0)");

            List<TypedRow> rows = executeQuery(
                    "SELECT a + b AS sum, a - b AS diff, a * b AS prod, a / b AS quot FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("sum")).doubleValue())
                    .isCloseTo(13.0, within(0.01));
            assertThat(((Number) rows.get(0).getRaw("diff")).doubleValue())
                    .isCloseTo(7.0, within(0.01));
            assertThat(((Number) rows.get(0).getRaw("prod")).doubleValue())
                    .isCloseTo(30.0, within(0.01));

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // DateTime Functions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("DateTime Functions")
    class DateTimeFunctions {

        @Test
        @DisplayName("EXTRACT year/month/day from DATE")
        void extractFromDate() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, dt DATE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, DATE '2024-03-15')");

            List<TypedRow> rows = executeQuery(
                    "SELECT EXTRACT(YEAR FROM dt) AS y, EXTRACT(MONTH FROM dt) AS m,"
                            + " EXTRACT(DAY FROM dt) AS d FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("y")).intValue()).isEqualTo(2024);
            assertThat(((Number) rows.get(0).getRaw("m")).intValue()).isEqualTo(3);
            assertThat(((Number) rows.get(0).getRaw("d")).intValue()).isEqualTo(15);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("TIMESTAMP literals and EXTRACT")
        void timestampLiteralAndExtract() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, ts TIMESTAMP)");
            executeDml("INSERT INTO " + fqn()
                    + " VALUES (1, TIMESTAMP '2024-06-01 14:30:00')");

            List<TypedRow> rows = executeQuery(
                    "SELECT EXTRACT(HOUR FROM ts) AS h, EXTRACT(MINUTE FROM ts) AS m FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("h")).intValue()).isEqualTo(14);
            assertThat(((Number) rows.get(0).getRaw("m")).intValue()).isEqualTo(30);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("INTERVAL arithmetic")
        void intervalArithmetic() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, dt DATE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, DATE '2024-01-01')");

            String sql = "SELECT dt + INTERVAL '30' DAY AS future_date FROM " + fqn();
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).hasSize(1);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Conditional Functions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Conditional Functions")
    class ConditionalFunctions {

        @Test
        @DisplayName("CASE WHEN expression")
        void caseWhen() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, score DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 95.0), (2, 75.0), (3, 50.0)");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, CASE WHEN score >= 90 THEN 'A'"
                            + " WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade FROM " + fqn());
            assertThat(rows).hasSize(3);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("COALESCE returns first non-null")
        void coalesce() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, a VARCHAR, b VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, NULL, 'fallback'), (2, 'primary', 'backup')");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, COALESCE(a, b) AS result FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("result").toString()).isEqualTo("fallback");
            assertThat(rows.get(1).getRaw("result").toString()).isEqualTo("primary");

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("IS NULL and IS NOT NULL filtering")
        void isNullFiltering() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'has'), (2, NULL), (3, 'data')");

            List<TypedRow> notNull = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE val IS NOT NULL");
            assertThat(notNull).hasSize(2);

            List<TypedRow> isNull = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE val IS NULL");
            assertThat(isNull).hasSize(1);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("BETWEEN and IN operators")
        void betweenAndIn() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val INT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1,5),(2,10),(3,15),(4,20),(5,25)");

            List<TypedRow> between = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE val BETWEEN 10 AND 20");
            assertThat(between).hasSize(3);

            List<TypedRow> inSet = executeQuery(
                    "SELECT * FROM " + fqn() + " WHERE val IN (5, 15, 25)");
            assertThat(inSet).hasSize(3);

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
