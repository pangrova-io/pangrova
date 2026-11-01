package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg View Spec E2E tests: CREATE VIEW, SELECT FROM VIEW, DROP VIEW,
 * SHOW VIEWS, and nested views.
 *
 * <p>Covers B4 acceptance criteria: CREATE/SELECT/DROP VIEW, SHOW VIEWS,
 * and nested view resolution. All operations execute through the real pipeline:
 * TestCluster HTTP → Coordinator → Iceberg ViewCatalog → local filesystem.
 *
 * <p>Requirements: B4, D6
 */
@DisplayName("Iceberg View E2E Tests")
class IcebergViewTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    private String fqn(String name) {
        return FQN_PREFIX + name;
    }

    // --- 2.10.5: CREATE VIEW and SELECT FROM VIEW ---

    @Test
    @DisplayName("Create view with filter and select from it returns filtered data")
    void createViewAndSelectFromIt() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, amount) VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");

        String viewName = "v_" + tableName;
        sqlRunner.executeDdl("CREATE VIEW " + fqn(viewName)
                + " AS SELECT id, name FROM " + fqn() + " WHERE id > 1");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(viewName) + " ORDER BY id");

        assertThat(rows).hasSize(2);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("bob");
        assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        assertThat(rows.get(1).getRaw("name")).isEqualTo("carol");
    }

    // --- 2.10.6: DROP VIEW then query errors ---

    @Test
    @DisplayName("Drop view then select from it throws error")
    void dropViewThenQueryErrors() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

        String viewName = "v_" + tableName;
        sqlRunner.executeDdl("CREATE VIEW " + fqn(viewName)
                + " AS SELECT * FROM " + fqn());

        // Verify view works before dropping
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(viewName) + " ORDER BY id");
        assertThat(rows).hasSize(2);

        // Drop the view
        sqlRunner.executeDdl("DROP VIEW " + fqn(viewName));

        // Query should fail with not found error
        QueryError error = sqlRunner.executeExpectingError(
                "SELECT * FROM " + fqn(viewName));

        assertThat(error.message()).containsIgnoringCase("not found");
    }

    // --- 2.10.7: SHOW VIEWS lists created views ---

    @Test
    @DisplayName("SHOW VIEWS lists all created views in schema")
    void showViewsListsCreatedViews() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name) VALUES (1, 'alice')");

        String view1 = "v1_" + tableName;
        String view2 = "v2_" + tableName;
        sqlRunner.executeDdl("CREATE VIEW " + fqn(view1)
                + " AS SELECT * FROM " + fqn() + " WHERE id = 1");
        sqlRunner.executeDdl("CREATE VIEW " + fqn(view2)
                + " AS SELECT name FROM " + fqn());

        List<TypedRow> rows = sqlRunner.execute(
                "SHOW VIEWS FROM " + CATALOG + "." + SCHEMA);

        // Extract view names from result
        List<String> viewNames = rows.stream()
                .map(row -> (String) row.getRaw("view_name"))
                .toList();

        assertThat(viewNames).contains(view1, view2);
    }

    // --- 2.10.8: Nested views ---

    @Test
    @DisplayName("Nested views resolve correctly through multiple layers")
    void nestedViewsWork() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, score DOUBLE)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name, score) VALUES "
                + "(1, 'alice', 50.0), (2, 'bob', 60.0), "
                + "(3, 'carol', 70.0), (4, 'dave', 80.0), (5, 'eve', 90.0)");

        // v1 filters id > 1 → rows 2,3,4,5
        String view1 = "v1_" + tableName;
        sqlRunner.executeDdl("CREATE VIEW " + fqn(view1)
                + " AS SELECT * FROM " + fqn() + " WHERE id > 1");

        // v2 selects from v1 where id < 5 → rows 2,3,4
        String view2 = "v2_" + tableName;
        sqlRunner.executeDdl("CREATE VIEW " + fqn(view2)
                + " AS SELECT * FROM " + fqn(view1) + " WHERE id < 5");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT * FROM " + fqn(view2) + " ORDER BY id");

        assertThat(rows).hasSize(3);
        assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
        assertThat(rows.get(0).getRaw("name")).isEqualTo("bob");
        assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        assertThat(rows.get(1).getRaw("name")).isEqualTo("carol");
        assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(4L);
        assertThat(rows.get(2).getRaw("name")).isEqualTo("dave");
    }
}
