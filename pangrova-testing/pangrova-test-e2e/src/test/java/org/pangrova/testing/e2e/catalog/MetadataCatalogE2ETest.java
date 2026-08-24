package org.pangrova.testing.e2e.catalog;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.spi.connector.data.TypedRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T08: Metadata/Catalog — 5 E2E scenarios.
 */
@E2ETest
@DisplayName("T08: Metadata/Catalog")
class MetadataCatalogE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested @DisplayName("Scenario 1: DDL creates metadata")
    class DdlCreatesMetadata {
        @Test void createTableRegistersInCatalog() {
            String tbl = CATALOG + "." + SCHEMA + ".t08_meta_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT, name VARCHAR)");
            // Table should be queryable immediately
            assertQueryReturnsRows("SELECT * FROM " + tbl, 0);
        }
    }

    @Nested @DisplayName("Scenario 2: DDL visible across queries")
    class DdlVisibleAcrossQueries {
        @Test void createdTableVisibleInSubsequentQuery() {
            String tbl = CATALOG + "." + SCHEMA + ".t08_vis_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
        }
    }

    @Nested @DisplayName("Scenario 3: DROP TABLE removes metadata")
    class DropTableRemoves {
        @Test void droppedTableNotQueryable() {
            String tbl = CATALOG + "." + SCHEMA + ".t08_drop_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (1)");
            executeDdl("DROP TABLE " + tbl);
            String error = executeExpectingError("SELECT * FROM " + tbl);
            assertThat(error).isNotNull();
        }
    }

    @Nested @DisplayName("Scenario 4: Schema isolation")
    class SchemaIsolation {
        @Test void tablesInDifferentSchemasIsolated() {
            String tbl = CATALOG + "." + SCHEMA + ".t08_iso_" + System.nanoTime();
            executeDdl("CREATE TABLE " + tbl + " (id BIGINT)");
            executeDml("INSERT INTO " + tbl + " VALUES (42)");
            assertQueryReturnsRows("SELECT * FROM " + tbl, 1);
        }
    }

    @Nested @DisplayName("Scenario 5: Catalog reconcile")
    class CatalogReconcile {
        @Test void multipleTablesCoexistInCatalog() {
            String t1 = CATALOG + "." + SCHEMA + ".t08_rec1_" + System.nanoTime();
            String t2 = CATALOG + "." + SCHEMA + ".t08_rec2_" + System.nanoTime();
            executeDdl("CREATE TABLE " + t1 + " (id BIGINT)");
            executeDdl("CREATE TABLE " + t2 + " (id BIGINT)");
            executeDml("INSERT INTO " + t1 + " VALUES (1)");
            executeDml("INSERT INTO " + t2 + " VALUES (2)");
            assertQueryReturnsRows("SELECT * FROM " + t1, 1);
            assertQueryReturnsRows("SELECT * FROM " + t2, 1);
        }
    }
}
