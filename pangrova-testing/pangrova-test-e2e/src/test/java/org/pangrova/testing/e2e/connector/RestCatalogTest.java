package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * REST Catalog E2E tests for both Iceberg and Paimon connectors.
 *
 * <p>Validates that the REST catalog integration works end-to-end:
 * table creation, data insertion, querying, and table deletion through a
 * REST catalog server.
 *
 * <p>Iceberg and Paimon have separate REST protocols and require independently
 * configured servers. Iceberg may use Gravitino; Paimon requires a Paimon REST
 * server and its warehouse identifier. Each suite runs only with its own endpoint.
 *
 * <p>Reads exercise SQL → Planner → Substrait → Native → transport → Client.
 * Writes also exercise the catalog's declaration-fenced commit path.
 *
 * <p>Requirements: B7 (Iceberg REST Catalog), C12 (Paimon REST Catalog), D3 (REST Catalog Unified)
 */
@DisplayName("REST Catalog - E2E Tests")
@EnabledIf("hasConfiguredCatalog")
class RestCatalogTest extends SqlE2ETestBase {

    private static final String ICEBERG_URI = "PANGROVA_ICEBERG_REST_CATALOG_URI";
    private static final String PAIMON_URI = "PANGROVA_PAIMON_REST_CATALOG_URI";
    private static final String PAIMON_WAREHOUSE = "PANGROVA_PAIMON_REST_CATALOG_WAREHOUSE";

    private static final String ICE_CATALOG = "iceberg_rest";
    private static final String PAI_CATALOG = "paimon_rest";
    private static final String SCHEMA = "default";
    private static final String ICE_FQN_PREFIX = ICE_CATALOG + "." + SCHEMA + ".";
    private static final String PAI_FQN_PREFIX = PAI_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        List<ConnectorTestConfig> configs = new ArrayList<>();
        if (configured(ICEBERG_URI)) {
            configs.add(new ConnectorTestConfig(ICE_CATALOG, SCHEMA, "iceberg", Map.of(
                    "catalog-type", "rest", "uri", System.getenv(ICEBERG_URI))));
        }
        if (configured(PAIMON_URI)) {
            if (!configured(PAIMON_WAREHOUSE)) {
                throw new IllegalArgumentException(PAIMON_WAREHOUSE + " is required for Paimon REST tests");
            }
            configs.add(new ConnectorTestConfig(PAI_CATALOG, SCHEMA, "paimon", Map.of(
                    "metastore", "rest", "uri", System.getenv(PAIMON_URI),
                    "warehouse", System.getenv(PAIMON_WAREHOUSE))));
        }
        return List.copyOf(configs);
    }

    private static boolean hasConfiguredCatalog() {
        return configured(ICEBERG_URI) || configured(PAIMON_URI);
    }

    private static boolean configured(String name) {
        String value = System.getenv(name);
        return value != null && !value.isBlank();
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    // --- 2.15.3: Iceberg REST Catalog CRUD ---

    @Nested
    @DisplayName("2.15.3 - Iceberg REST Catalog CRUD")
    @EnabledIfEnvironmentVariable(named = ICEBERG_URI, matches = ".+")
    class IcebergRestCatalogCrud {

        @Test
        @DisplayName("Iceberg REST Catalog: create, read empty table, drop")
        void createReadEmptyDrop() {
            String fqn = ICE_FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn + " (id BIGINT, name VARCHAR)");
            try {
                assertThat(sqlRunner.execute("SELECT id, name FROM " + fqn)).isEmpty();
            } finally {
                sqlRunner.executeDdl("DROP TABLE " + fqn);
            }
            assertThatThrownBy(() -> sqlRunner.execute("SELECT * FROM " + fqn))
                    .isInstanceOf(RuntimeException.class);
        }

        @Test
        @DisplayName("Iceberg REST Catalog: create table, insert, query")
        void createInsertQuery() {
            String fqn = ICE_FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)");

            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, amount) VALUES"
                    + " (1, 'alice', 100.0),"
                    + " (2, 'bob', 200.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, amount FROM " + fqn + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).get("id")).isEqualTo(1L);
            assertThat(rows.get(0).get("name")).isEqualTo("alice");
            assertThat(rows.get(1).get("id")).isEqualTo(2L);
        }

        @Test
        @DisplayName("Iceberg REST Catalog: drop table")
        void dropTable() {
            String fqn = ICE_FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn + " (id BIGINT, value VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn + " (id, value) VALUES (1, 'test')");

            sqlRunner.executeDdl("DROP TABLE " + fqn);

            assertThatThrownBy(() -> sqlRunner.execute("SELECT * FROM " + fqn))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    // --- 2.15.4: Paimon REST Catalog CRUD ---

    @Nested
    @DisplayName("2.15.4 - Paimon REST Catalog CRUD")
    @EnabledIfEnvironmentVariable(named = PAIMON_URI, matches = ".+")
    class PaimonRestCatalogCrud {

        @Test
        @DisplayName("Paimon REST Catalog: create table, insert, query")
        void createInsertQuery() {
            String fqn = PAI_FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, name VARCHAR, score DOUBLE,"
                    + "  PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, score) VALUES"
                    + " (1, 'alice', 80.0),"
                    + " (2, 'bob', 60.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, score FROM " + fqn + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).get("id")).isEqualTo(1L);
            assertThat(rows.get(0).get("name")).isEqualTo("alice");
            assertThat(rows.get(1).get("id")).isEqualTo(2L);
        }

        @Test
        @DisplayName("Paimon REST Catalog: drop table")
        void dropTable() {
            String fqn = PAI_FQN_PREFIX + tableName;

            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, data VARCHAR,"
                    + "  PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + fqn + " (id, data) VALUES (1, 'test')");

            sqlRunner.executeDdl("DROP TABLE " + fqn);

            assertThatThrownBy(() -> sqlRunner.execute("SELECT * FROM " + fqn))
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
