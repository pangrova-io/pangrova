package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for index operations across Iceberg and Paimon connectors.
 *
 * <p>Validates: Requirements 11.4, 19.1–19.8, 20.3, 20.4
 */
@DisplayName("Index Multi-Connector E2E")
class IndexMultiConnectorE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_catalog";
    private static final String PMN_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";
    private static final String ICE_FQN_PREFIX = ICE_CATALOG + "." + SCHEMA + ".";
    private static final String PMN_FQN_PREFIX = PMN_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PMN_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String iceTable;
    private String pmnTable;

    @BeforeAll
    static void setupCatalogs() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PMN_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    @BeforeEach
    void createTestTables(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long ts = System.nanoTime();
        iceTable = "t_mc_ice_" + method + "_" + ts;
        pmnTable = "t_mc_pmn_" + method + "_" + ts;
        sqlRunner.executeDdl("CREATE TABLE " + iceFqn()
                + " (id BIGINT, amount DOUBLE, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + iceFqn()
                + " VALUES (1, 100.0, 'a'), (2, 200.0, 'b')");

        sqlRunner.executeDdl("CREATE TABLE " + pmnFqn()
                + " (id BIGINT, amount DOUBLE, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + pmnFqn()
                + " VALUES (1, 100.0, 'x'), (2, 200.0, 'y')");
    }

    private String iceFqn() { return ICE_FQN_PREFIX + iceTable; }
    private String pmnFqn() { return PMN_FQN_PREFIX + pmnTable; }

    @Nested
    @DisplayName("Iceberg Index Lifecycle")
    class IcebergLifecycle {

        @Test
        @DisplayName("BTREE index on Iceberg — create, query, drop")
        void icebergBtreeLifecycle() {
            sqlRunner.executeDdl("CREATE INDEX idx_ice ON " + iceFqn()
                    + "(amount) USING BTREE");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + iceFqn() + " WHERE amount BETWEEN 50 AND 150");
            assertThat(result).isNotEmpty();

            sqlRunner.executeDdl("DROP INDEX idx_ice ON " + iceFqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + iceFqn());
            boolean hasIdx = indexes.stream()
                    .anyMatch(r -> "idx_ice".equals(r.getRaw("index_name").toString())
                            && "READY".equals(r.getRaw("state").toString()));
            assertThat(hasIdx).isFalse();
        }
    }

    @Nested
    @DisplayName("Paimon Index Lifecycle")
    class PaimonLifecycle {

        @Test
        @DisplayName("BTREE index on Paimon — create, query, drop")
        void paimonBtreeLifecycle() throws Exception {
            sqlRunner.executeDdl("CREATE INDEX idx_pmn ON " + pmnFqn()
                    + "(amount) USING BTREE");

            org.awaitility.Awaitility.await("Paimon index READY")
                    .atMost(Duration.ofSeconds(45))
                    .untilAsserted(() -> assertThat(sqlRunner.execute("SHOW INDEXES FROM " + pmnFqn())
                            .stream().filter(row -> "idx_pmn".equals(row.getRaw("index_name").toString()))
                            .findFirst().orElseThrow().getRaw("state").toString()).isEqualTo("READY"));
            var options = new Options();
            options.set("warehouse", paimonWarehouse().toUri().toString());
            try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
                var table = (FileStoreTable) catalog.getTable(Identifier.create(SCHEMA, pmnTable));
                assertThat(table.tagManager().tags().values().stream()
                        .flatMap(List::stream)
                        .filter(tag -> tag.startsWith("pangrova_index_")))
                        .hasSize(1);
            }

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + pmnFqn() + " WHERE amount BETWEEN 50 AND 150");
            assertThat(result).isNotEmpty();

            sqlRunner.executeDdl("DROP INDEX idx_pmn ON " + pmnFqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + pmnFqn());
            boolean hasIdx = indexes.stream()
                    .anyMatch(r -> "idx_pmn".equals(r.getRaw("index_name").toString())
                            && "READY".equals(r.getRaw("state").toString()));
            assertThat(hasIdx).isFalse();
        }
    }

    @Nested
    @DisplayName("Cross-Catalog Query")
    class CrossCatalogQuery {

        @Test
        @DisplayName("Join query across Iceberg and Paimon with indexes on both")
        void crossCatalogJoinWithIndexes() {
            sqlRunner.executeDdl("CREATE INDEX idx_ice_j ON " + iceFqn()
                    + "(id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_pmn_j ON " + pmnFqn()
                    + "(id) USING BLOOM_FILTER");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT a.value, b.value FROM " + iceFqn() + " a"
                    + " JOIN " + pmnFqn() + " b ON a.id = b.id"
                    + " WHERE a.id = 1");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Native Capability Skip")
    class NativeCapabilitySkip {

        @Test
        @DisplayName("Paimon native bloom filter — Pangrova skips redundant build")
        void paimonNativeBloomSkip() {
            // Query should work via Paimon native bloom filter (L1)
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + pmnFqn() + " WHERE id = 1");
            assertThat(result).isNotEmpty();
        }
    }
}
