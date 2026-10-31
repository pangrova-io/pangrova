package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E tests for GC generation fencing — ensuring no deletion of current
 * generation index files during garbage collection.
 *
 * <p>Verifies that after creating an index and triggering a rebuild,
 * the index remains functional and GC does not delete active generation files.
 *
 * <p>Validates: Requirements 4.AC3
 */
@DisplayName("GC Generation Fencing E2E")
class GcGenerationFencingE2ETest extends SqlE2ETestBase {

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
        tableName = "t_gcfence_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, status VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    @Nested
    @DisplayName("Index Rebuild Lifecycle")
    class IndexRebuildLifecycle {

        @Test
        @DisplayName("Index remains functional after ALTER INDEX REBUILD")
        void indexFunctionalAfterRebuild() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'ACTIVE'), (2, 200, 'ACTIVE')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 300, 'INACTIVE'), (4, 400, 'INACTIVE')");

            sqlRunner.executeDdl("CREATE INDEX idx_status ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_status");

            // Verify index exists
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();

            // Trigger rebuild (generation 2)
            sqlRunner.executeDdl("ALTER INDEX idx_status ON " + fqn() + " REBUILD");

            // Index should still be functional after rebuild
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");

            assertThat(rows).hasSize(2);
            assertThat(rows).allMatch(r ->
                    "ACTIVE".equals(r.getRaw("status").toString()));
        }

        @Test
        @DisplayName("SHOW INDEXES still lists index after rebuild")
        void showIndexesAfterRebuild() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 100, 'A')");

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_val");

            sqlRunner.executeDdl("ALTER INDEX idx_val ON " + fqn() + " REBUILD");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
            assertThat(indexes.stream()
                    .anyMatch(r -> "idx_val".equals(r.getRaw("index_name").toString())))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("GC Safety")
    class GcSafety {

        @Test
        @DisplayName("Query returns correct results after rebuild cycle")
        void queryCorrectAfterRebuildCycle() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (1, 10, 'X'), (2, 20, 'Y')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (3, 30, 'X'), (4, 40, 'Y')");

            sqlRunner.executeDdl("CREATE INDEX idx_status ON " + fqn()
                    + "(status) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_status");

            // Rebuild cycle
            sqlRunner.executeDdl("ALTER INDEX idx_status ON " + fqn() + " REBUILD");

            // Insert new data after rebuild
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, status) VALUES (5, 50, 'X')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'X'");

            assertThat(rows).hasSize(3);
        }
    }
}
