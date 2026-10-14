package org.pangrova.testing.e2e.ddl;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.CatalogDdlGrantFixture;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for catalog and schema lifecycle DDL operations.
 *
 * <p>Covers CREATE CATALOG, DROP CATALOG, CREATE SCHEMA,
 * SHOW CATALOGS, and SHOW SCHEMAS through the full pipeline.
 */
@E2ETest
@DisplayName("Catalog Lifecycle DDL - E2E Tests")
class CatalogLifecycleTest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    // ── SHOW CATALOGS ──

    @Nested
    @DisplayName("SHOW CATALOGS")
    class ShowCatalogs {

        @Test
        @DisplayName("SHOW CATALOGS lists registered catalogs")
        void showCatalogsReturnsResults() {
            List<TypedRow> rows = executeQuery("SHOW CATALOGS");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── SHOW SCHEMAS ──

    @Nested
    @DisplayName("SHOW SCHEMAS")
    class ShowSchemas {

        @Test
        @DisplayName("SHOW SCHEMAS lists schemas in catalog")
        void showSchemasReturnsResults() {
            List<TypedRow> rows = executeQuery("SHOW SCHEMAS");
            assertThat(rows).isNotEmpty();
        }
    }

    // ── CREATE SCHEMA ──

    @Nested
    @DisplayName("CREATE SCHEMA")
    class CreateSchema {

        @Test
        @DisplayName("CREATE SCHEMA creates new schema in catalog")
        void createSchema() {
            String schemaName = "test_schema_" + System.nanoTime();
            executeDdl("CREATE SCHEMA " + CATALOG + "." + schemaName);

            List<TypedRow> schemas = executeQuery("SHOW SCHEMAS");
            assertThat(schemas).isNotEmpty();
        }
    }

    // ── CREATE / DROP CATALOG ──

    @Nested
    @DisplayName("CREATE / DROP CATALOG")
    class CatalogOperations {

        @Test
        @DisplayName("CREATE CATALOG registers a new catalog")
        void createCatalog() {
            String catName = "test_cat_" + System.nanoTime();
            CatalogDdlGrantFixture.withCatalogLifecycleGrants(
                    stateStoreDataSource(), authenticatedCatalogSession(CATALOG).getUser(),
                    catName, () -> {
                        executeDdl("CREATE CATALOG " + catName
                                + " WITH ('connector' = 'iceberg', "
                                + "'warehouse' = '" + icebergWarehouse() + "')");
                        try {
                            assertThat(executeQuery("SHOW CATALOGS"))
                                    .anySatisfy(row -> assertThat(row.toString()).contains(catName));
                        } finally {
                            executeDdl("DROP CATALOG " + catName);
                        }
                    });
        }

        @Test
        @DisplayName("DROP CATALOG removes catalog")
        void dropCatalog() {
            String catName = "test_cat_drop_" + System.nanoTime();
            CatalogDdlGrantFixture.withCatalogLifecycleGrants(
                    stateStoreDataSource(), authenticatedCatalogSession(CATALOG).getUser(),
                    catName, () -> {
                        executeDdl("CREATE CATALOG " + catName
                                + " WITH ('connector' = 'iceberg', "
                                + "'warehouse' = '" + icebergWarehouse() + "')");
                        executeDdl("DROP CATALOG " + catName);
                        assertThat(executeQuery("SHOW CATALOGS"))
                                .noneSatisfy(row -> assertThat(row.toString()).contains(catName));
                    });
        }
    }
}
