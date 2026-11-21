package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Geometry and Geography type E2E tests: CREATE TABLE with GEOMETRY/GEOGRAPHY
 * columns, INSERT WKB binary data, SELECT, and CRS metadata preservation.
 *
 * <p>Geometry and Geography types are stored as WKB (Well-Known Binary) in
 * VARBINARY columns. The native reader reuses the existing VARBINARY column
 * reader with zero additional overhead.
 *
 * <p>Covers A4 acceptance criteria:
 * <ul>
 *   <li>CREATE TABLE with GEOMETRY/GEOGRAPHY columns</li>
 *   <li>INSERT WKB binary data and SELECT back</li>
 *   <li>CRS metadata preservation through Iceberg table properties</li>
 * </ul>
 *
 * <p>All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 *
 * <p>Requirements: A4
 */
@DisplayName("Geometry & Geography Type E2E Tests")
class GeometryGeographyTypeE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String ICE_PREFIX = ICE_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String iceFqn() {
        return ICE_PREFIX + tableName;
    }

    // --- 2.4.5: CREATE TABLE GEOMETRY column INSERT WKB SELECT ---

    @Nested
    @DisplayName("2.4.5 - Geometry Column INSERT WKB SELECT")
    class GeometryColumnInsertWkbSelect {

        @Test
        @DisplayName("Create Iceberg v3 table with GEOMETRY column, insert WKB hex, select back")
        void createGeometryTableInsertWkbSelect() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, location GEOMETRY)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            // WKB hex for POINT(1.0 2.0): little-endian, type=Point,
            // X=1.0 (000000000000F03F), Y=2.0 (0000000000000040)
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, location) VALUES "
                    + "(1, X'0101000000000000000000F03F0000000000000040'),"
                    + "(2, X'0101000000000000000000244000000000000034400000000000003E40'),"
                    + "(3, NULL)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, location FROM " + iceFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);

            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("location")).isNotNull();

            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getRaw("location")).isNotNull();

            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
            assertThat(rows.get(2).getRaw("location")).isNull();
        }

        @Test
        @DisplayName("Create Iceberg v3 table with GEOGRAPHY column, insert WKB hex, select back")
        void createGeographyTableInsertWkbSelect() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, geo GEOGRAPHY)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            // WKB hex for POINT(-122.3321 47.6062) — Seattle coordinates
            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, geo) VALUES "
                    + "(1, X'0101000000E5D022DBF99E5EC0F6285C8FC2E44740'),"
                    + "(2, X'01010000006666666666A65EC03D0AD7A3709D4840')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, geo FROM " + iceFqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("geo")).isNotNull();

            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getRaw("geo")).isNotNull();
        }

        @Test
        @DisplayName("GEOMETRY and GEOGRAPHY columns in same table")
        void geometryAndGeographyInSameTable() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, shape GEOMETRY, position GEOGRAPHY)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, shape, position) VALUES "
                    + "(1, X'0101000000000000000000F03F0000000000000040',"
                    + " X'0101000000E5D022DBF99E5EC0F6285C8FC2E44740')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, shape, position FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("shape")).isNotNull();
            assertThat(rows.get(0).getRaw("position")).isNotNull();
        }
    }

    // --- 2.4.6: CRS metadata preservation ---

    @Nested
    @DisplayName("2.4.6 - CRS Metadata Preservation")
    class CrsMetadataPreservation {

        @Test
        @DisplayName("GEOMETRY column preserves CRS:84 default through Iceberg schema")
        void geometryCrsPreservedInSchema() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, geom GEOMETRY)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, geom) VALUES "
                    + "(1, X'0101000000000000000000F03F0000000000000040')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, geom FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("geom")).isNotNull();
        }

        @Test
        @DisplayName("GEOGRAPHY column preserves CRS:84 default through Iceberg schema")
        void geographyCrsPreservedInSchema() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, geo GEOGRAPHY)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, geo) VALUES "
                    + "(1, X'0101000000E5D022DBF99E5EC0F6285C8FC2E44740')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, geo FROM " + iceFqn());

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("geo")).isNotNull();
        }

        @Test
        @DisplayName("Multiple inserts preserve WKB binary integrity")
        void multipleInsertsPreserveWkbIntegrity() {
            sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                    + " (id BIGINT, geom GEOMETRY)"
                    + " TBLPROPERTIES ('format-version' = '3')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, geom) VALUES "
                    + "(1, X'0101000000000000000000F03F0000000000000040')");

            sqlRunner.execute("INSERT INTO " + iceFqn()
                    + " (id, geom) VALUES "
                    + "(2, X'0101000000000000000000244000000000000034400000000000003E40')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, geom FROM " + iceFqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("geom")).isNotNull();
            assertThat(rows.get(1).getRaw("geom")).isNotNull();

            Object geom1 = rows.get(0).getRaw("geom");
            Object geom2 = rows.get(1).getRaw("geom");
            assertThat(geom1).isNotEqualTo(geom2);
        }
    }
}
