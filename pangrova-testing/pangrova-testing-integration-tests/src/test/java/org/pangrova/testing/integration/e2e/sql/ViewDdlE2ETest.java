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

import org.pangrova.types.value.TypedRow;
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
 * Full-stack E2E tests for View DDL operations through SQL.
 *
 * <p>Validates CREATE VIEW, DROP VIEW, SHOW VIEWS, and querying from views
 * through the full coordinator → planner → worker pipeline.</p>
 */
@E2ETest
@DisplayName("View DDL Full-Stack E2E")
class ViewDdlE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_view_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Test
    void createModesAndMissingDropPreserveSqlSemantics() {
        String view = fqn() + "_mode";
        executeDdl("CREATE VIEW " + view + " AS SELECT 1 AS id");
        try {
            assertThat(executeExpectingError("CREATE VIEW " + view + " AS SELECT 2 AS id"))
                    .contains("already exists");
            executeDdl("CREATE VIEW IF NOT EXISTS " + view + " AS SELECT 3 AS id");
            assertThat(executeQuery("SELECT id FROM " + view).get(0).getLong("id")).isEqualTo(1L);
            executeDdl("CREATE OR REPLACE VIEW " + view + " AS SELECT 4 AS id");
            assertThat(executeQuery("SELECT id FROM " + view).get(0).getLong("id")).isEqualTo(4L);
        } finally {
            executeDdl("DROP VIEW " + view);
        }
        assertThat(executeExpectingError("DROP VIEW " + view)).contains("not found");
        executeDdl("DROP VIEW IF EXISTS " + view);
    }

    @Nested
    @DisplayName("CREATE VIEW")
    class CreateView {

        @Test
        @DisplayName("CREATE VIEW with simple SELECT")
        void createViewSimple() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, score DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'Alice', 95.0), (2, 'Bob', 82.0)");

            String viewName = fqn() + "_view";
            executeDdl("CREATE VIEW " + viewName + " AS SELECT id, name FROM " + fqn() + " WHERE score > 90");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + viewName);
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("name").toString()).isEqualTo("Alice");

            executeDdl("DROP VIEW " + viewName);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CREATE VIEW with aggregation")
        void createViewWithAggregation() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, dept VARCHAR, salary DOUBLE)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'eng', 100.0), (2, 'eng', 120.0), (3, 'sales', 90.0)");

            String viewName = fqn() + "_agg_view";
            executeDdl("CREATE VIEW " + viewName
                    + " AS SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM " + fqn() + " GROUP BY dept");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + viewName + " ORDER BY dept");
            assertThat(rows).hasSize(2);

            executeDdl("DROP VIEW " + viewName);
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CREATE OR REPLACE VIEW")
        void createOrReplaceView() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");

            String viewName = fqn() + "_rep_view";
            executeDdl("CREATE VIEW " + viewName + " AS SELECT * FROM " + fqn());
            assertQueryReturnsRows("SELECT * FROM " + viewName, 2);

            executeDdl("CREATE OR REPLACE VIEW " + viewName
                    + " AS SELECT * FROM " + fqn() + " WHERE id = 1");
            assertQueryReturnsRows("SELECT * FROM " + viewName, 1);

            executeDdl("DROP VIEW " + viewName);
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("DROP VIEW")
    class DropView {

        @Test
        @DisplayName("DROP VIEW removes the view")
        void dropViewRemoves() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");
            String viewName = fqn() + "_drop_view";
            executeDdl("CREATE VIEW " + viewName + " AS SELECT * FROM " + fqn());
            executeDdl("DROP VIEW " + viewName);

            String error = executeExpectingError("SELECT * FROM " + viewName);
            assertThat(error).isNotEmpty();

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("DROP VIEW IF EXISTS does not error on missing view")
        void dropViewIfExistsNoError() {
            String viewName = fqn() + "_nonexistent_view";
            executeDdl("DROP VIEW IF EXISTS " + viewName);
            // No error thrown — test passes
        }
    }

    @Nested
    @DisplayName("SHOW VIEWS")
    class ShowViews {

        @Test
        @DisplayName("SHOW VIEWS lists created views")
        void showViewsListsCreated() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            String viewName = fqn() + "_show_view";
            executeDdl("CREATE VIEW " + viewName + " AS SELECT * FROM " + fqn());

            List<TypedRow> views = executeQuery("SHOW VIEWS FROM " + CATALOG + "." + SCHEMA);
            assertThat(views).isNotEmpty();

            executeDdl("DROP VIEW " + viewName);
            executeDdl("DROP TABLE " + fqn());
        }
    }
}
