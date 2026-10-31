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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack E2E tests for spatial functions.
 *
 * <p>Validates ST_INTERSECTS, ST_WITHIN, ST_CONTAINS using
 * GEOMETRY/GEOGRAPHY columns and WKT literals.</p>
 */
@E2ETest
@DisplayName("Spatial Functions Full-Stack E2E")
class SpatialFunctionsE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_spatial_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("ST_INTERSECTS")
    class StIntersects {

        @Test
        @DisplayName("ST_INTERSECTS with point and polygon")
        void intersectsPointPolygon() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, geom GEOMETRY)"
                    + " TBLPROPERTIES ('format-version' = '3')");
            // WKB hex: POINT(0 0), POINT(5 5), POINT(10 10)
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, X'010100000000000000000000000000000000000000'),"
                    + "(2, X'010100000000000000000014400000000000001440'),"
                    + "(3, X'010100000000000000000024400000000000002440')");

            String sql = "SELECT id FROM " + fqn()
                    + " WHERE ST_INTERSECTS(geom, ST_GEOMFROMTEXT("
                    + "'POLYGON((0 0, 6 0, 6 6, 0 6, 0 0))'))";
            assertThat(executeQuery(sql))
                    .extracting(row -> ((Number) row.getRaw("id")).longValue())
                    .containsExactlyInAnyOrder(1L, 2L);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("ST_WITHIN")
    class StWithin {

        @Test
        @DisplayName("ST_WITHIN filters points within polygon")
        void withinPolygon() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, location GEOMETRY)"
                    + " TBLPROPERTIES ('format-version' = '3')");
            // WKB hex: POINT(1 1), POINT(3 3), POINT(8 8)
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, X'0101000000000000000000F03F000000000000F03F'),"
                    + "(2, X'010100000000000000000008400000000000000840'),"
                    + "(3, X'010100000000000000000020400000000000002040')");

            String sql = "SELECT id FROM " + fqn()
                    + " WHERE ST_WITHIN(location, ST_GEOMFROMTEXT("
                    + "'POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'))";
            assertThat(executeQuery(sql))
                    .extracting(row -> ((Number) row.getRaw("id")).longValue())
                    .containsExactlyInAnyOrder(1L, 2L);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("ST_CONTAINS")
    class StContains {

        @Test
        @DisplayName("ST_CONTAINS polygon contains point")
        void containsPoint() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region GEOMETRY, name VARCHAR)"
                    + " TBLPROPERTIES ('format-version' = '3')");
            // WKB hex: POLYGON((0 0,10 0,10 10,0 10,0 0)), POLYGON((0 0,3 0,3 3,0 3,0 0))
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, X'0103000000010000000500000000000000000000000000000000000000"
                    + "00000000000024400000000000000000"
                    + "00000000000024400000000000002440"
                    + "00000000000000000000000000002440"
                    + "00000000000000000000000000000000', 'large'),"
                    + "(2, X'0103000000010000000500000000000000000000000000000000000000"
                    + "00000000000008400000000000000000"
                    + "00000000000008400000000000000840"
                    + "00000000000000000000000000000840"
                    + "00000000000000000000000000000000', 'small')");

            String sql = "SELECT id, name FROM " + fqn()
                    + " WHERE ST_CONTAINS(region, ST_GEOMFROMTEXT('POINT(5 5)'))";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("large");

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
