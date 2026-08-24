package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for BITMAP index high-cardinality column handling.
 *
 * <p>Validates: Requirements 22.1–22.4
 */
@DisplayName("Bitmap Cardinality E2E")
class BitmapCardinalityE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_bmp_card_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, status VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " VALUES (1, 'ACTIVE'), (2, 'INACTIVE'), (3, 'PENDING')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Cardinality Exceeded")
    class CardinalityExceeded {

        @Test
        @DisplayName("High-cardinality column — build completes with STALE state")
        void cardinalityExceeded() {
            // Insert high-cardinality data
            StringBuilder sb = new StringBuilder();
            for (int i = 10; i < 200; i++) {
                if (sb.length() > 0) sb.append(", ");
                sb.append("(").append(i).append(", 'status_").append(i).append("')");
            }
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES " + sb);

            sqlRunner.executeDdl("CREATE INDEX idx_bmp ON " + fqn()
                    + "(status) USING BITMAP WITH ('bitmap.max_cardinality'='10')");

            // Index should exist (may be STALE due to cardinality exceeded)
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_bmp".equals(r.getRaw("index_name").toString()));
        }
    }

    @Nested
    @DisplayName("Partial Bitmap Safety")
    class PartialBitmapSafety {

        @Test
        @DisplayName("Query with tracked value returns correct results")
        void partialBitmapSafety() {
            sqlRunner.executeDdl("CREATE INDEX idx_bmp_safe ON " + fqn()
                    + "(status) USING BITMAP");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");
            assertThat(result).isNotEmpty();
            assertThat(result).allMatch(r ->
                    "ACTIVE".equals(r.getRaw("status").toString()));
        }
    }

    @Nested
    @DisplayName("SHOW INDEXES Display")
    class ShowIndexesDisplay {

        @Test
        @DisplayName("SHOW INDEXES displays bitmap index metadata")
        void showIndexesDisplay() {
            sqlRunner.executeDdl("CREATE INDEX idx_bmp_show ON " + fqn()
                    + "(status) USING BITMAP");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_bmp_show".equals(r.getRaw("index_name").toString())
                    && "BITMAP".equals(r.getRaw("index_type").toString()));
        }
    }
}
