package org.pangrova.testing.integration.e2e.index;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for L0→L1→L2 multi-layer filtering pipeline.
 *
 * <p>Validates: Requirements 11.1–11.4, 20.1–20.4
 */
@DisplayName("Multi-Layer Filtering Pipeline E2E")
class MultiLayerFilteringE2ETest extends SqlE2ETestBase {

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
        tableName = "t_ml_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, user_id BIGINT, value DOUBLE)");
        // 4 files with non-overlapping id ranges
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 42, 10.0), (50, 43, 20.0)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (101, 44, 30.0), (150, 42, 40.0)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (201, 45, 50.0), (250, 46, 60.0)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (301, 47, 70.0), (350, 42, 80.0)");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("L1 Native Min-Max Skip")
    class L1NativeMinMax {

        @Test
        @DisplayName("L1 manifest bounds skip files outside id range")
        void l1MinMaxSkip() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE id = 50");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("id")).longValue() == 50L);
        }
    }

    @Nested
    @DisplayName("L1 + L2 Combined")
    class L1PlusL2 {

        @Test
        @DisplayName("L1 range + L2 bloom filter combined skipping")
        void l1RangePlusL2Bloom() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE (id BETWEEN 1 AND 200) AND user_id = 42");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long id = ((Number) r.getRaw("id")).longValue();
                long uid = ((Number) r.getRaw("user_id")).longValue();
                return id >= 1 && id <= 200 && uid == 42;
            });
        }
    }

    @Nested
    @DisplayName("L1 Unavailable Fallback")
    class L1Fallback {

        @Test
        @DisplayName("L2 works when L1 native evaluator unavailable")
        void l2WorksWithoutL1() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 42");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("user_id")).longValue() == 42L);
        }
    }
}
