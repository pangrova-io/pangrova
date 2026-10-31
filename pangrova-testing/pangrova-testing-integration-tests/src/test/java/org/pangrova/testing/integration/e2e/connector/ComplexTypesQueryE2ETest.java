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
package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

/**
 * Full-stack E2E tests for complex types (ARRAY, MAP, STRUCT) queries.
 *
 * <p>Validates creation, insertion, querying, and EXPLODE/POSEXPLODE
 * operations on complex type columns through the full SQL pipeline.</p>
 */
@E2ETest
@DisplayName("Complex Types Query Full-Stack E2E")
class ComplexTypesQueryE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_complex_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // ARRAY Type Operations
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ARRAY Type Operations")
    class ArrayOperations {

        @Test
        @DisplayName("Create table with ARRAY column, insert and query")
        void arrayColumnInsertAndQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, tags ARRAY(VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY['java', 'sql', 'ai']),"
                    + "(2, ARRAY['python', 'ml']),"
                    + "(3, ARRAY['rust', 'systems'])");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(3);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("ARRAY element access with index")
        void arrayElementAccess() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, nums ARRAY(INT))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY[10, 20, 30]),"
                    + "(2, ARRAY[40, 50, 60])");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, nums[1] AS first_elem FROM " + fqn());
            assertThat(rows).hasSize(2);
            assertThat(rows).extracting(
                    row -> row.getRaw("id"), row -> row.getRaw("first_elem"))
                    .containsExactlyInAnyOrder(tuple(1L, 10), tuple(2L, 40));

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("EXPLODE on ARRAY column unnests elements")
        void explodeArray() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, tags ARRAY(VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY['a', 'b', 'c']),"
                    + "(2, ARRAY['d', 'e'])");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, tag FROM " + fqn()
                    + ", LATERAL EXPLODE(tags) AS t(tag)");
            // 3 + 2 = 5 rows after explode
            assertThat(rows).hasSize(5);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("POSEXPLODE on ARRAY column provides index")
        void posexplodeArray() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, items ARRAY(VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY['x', 'y', 'z'])");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, pos, item FROM " + fqn()
                    + ", LATERAL POSEXPLODE(items) AS t(pos, item)");
            assertThat(rows).hasSize(3);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("ARRAY_CONTAINS function")
        void arrayContains() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, tags ARRAY(VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ARRAY['java', 'sql']),"
                    + "(2, ARRAY['python', 'ml']),"
                    + "(3, ARRAY['java', 'ml'])");

            List<TypedRow> rows = executeQuery(
                    "SELECT id FROM " + fqn()
                    + " WHERE ARRAY_CONTAINS(tags, 'java')");
            assertThat(rows).hasSize(2);
            assertThat(rows).extracting(row -> row.getRaw("id"))
                    .containsExactlyInAnyOrder(1L, 3L);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // MAP Type Operations
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("MAP Type Operations")
    class MapOperations {

        @Test
        @DisplayName("Create table with MAP column, insert and query")
        void mapColumnInsertAndQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, props MAP(VARCHAR, VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, MAP['color', 'red', 'size', 'large']),"
                    + "(2, MAP['color', 'blue', 'weight', '10kg'])");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(2);
            assertThat(rows).extracting(
                    row -> row.getRaw("id"), row -> row.getRaw("props"))
                    .containsExactlyInAnyOrder(
                            tuple(1L, Map.of("color", "red", "size", "large")),
                            tuple(2L, Map.of("color", "blue", "weight", "10kg")));

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("MAP key access with bracket notation")
        void mapKeyAccess() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, attrs MAP(VARCHAR, INT))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, MAP['width', 100, 'height', 200])");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, attrs['width'] AS w FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("w")).isEqualTo(100);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("MAP VARCHAR keys preserve explicit trailing spaces")
        void mapKeyPreservesTrailingSpace() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, attrs MAP(VARCHAR, INT))");
            executeDml("INSERT INTO " + fqn()
                    + " VALUES (1, MAP['width ', 100, 'height', 200])");

            List<TypedRow> rows = executeQuery(
                    "SELECT attrs['width '] AS exact_key, attrs['width'] AS other_key FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("exact_key")).isEqualTo(100);
            assertThat(rows.get(0).getRaw("other_key")).isNull();

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("EXPLODE on MAP column produces key-value pairs")
        void explodeMap() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, props MAP(VARCHAR, VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, MAP['a', '1', 'b', '2', 'c', '3'])");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, k, v FROM " + fqn()
                    + ", LATERAL EXPLODE(props) AS t(k, v)");
            assertThat(rows).hasSize(3);
            assertThat(rows).extracting(
                    row -> row.getRaw("k"), row -> row.getRaw("v"))
                    .containsExactlyInAnyOrder(
                            tuple("a", "1"), tuple("b", "2"), tuple("c", "3"));

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // STRUCT Type Operations
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("STRUCT Type Operations")
    class StructOperations {

        @Test
        @DisplayName("Create table with STRUCT column, insert and query")
        void structColumnInsertAndQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, address STRUCT(city VARCHAR, zip VARCHAR))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ROW('New York', '10001')),"
                    + "(2, ROW('San Francisco', '94105'))");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(2);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("STRUCT field access with dot notation")
        void structFieldAccess() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, info STRUCT(name VARCHAR, age INT))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ROW('Alice', 30)),"
                    + "(2, ROW('Bob', 25))");

            List<TypedRow> stored = executeQuery("SELECT id, info FROM " + fqn());
            assertThat(stored).hasSize(2);
            assertThat(stored.stream().map(row -> row.getRaw("info")))
                    .as("stored rows: %s", stored)
                    .containsExactlyInAnyOrder(
                            Map.of("name", "Alice", "age", 30),
                            Map.of("name", "Bob", "age", 25));

            List<TypedRow> projected = executeQuery(
                    "SELECT id, info.name AS name, info.age AS age FROM " + fqn());
            assertThat(projected).hasSize(2);
            assertThat(projected.stream().map(row -> row.getRaw("age")))
                    .as("projected rows: %s", projected)
                    .containsExactlyInAnyOrder(30, 25);

            List<TypedRow> rows = executeQuery(
                    "SELECT id, info.name, info.age FROM " + fqn()
                    + " WHERE info.age > 27");
            assertThat(rows).hasSize(1);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Nested STRUCT (STRUCT within STRUCT)")
        void nestedStruct() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, contact STRUCT(name VARCHAR, addr STRUCT(city VARCHAR, country VARCHAR)))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, ROW('Alice', ROW('Tokyo', 'Japan')))");

            List<TypedRow> rows = executeQuery(
                    "SELECT id, contact.name, contact.addr.city FROM " + fqn());
            assertThat(rows).hasSize(1);

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
