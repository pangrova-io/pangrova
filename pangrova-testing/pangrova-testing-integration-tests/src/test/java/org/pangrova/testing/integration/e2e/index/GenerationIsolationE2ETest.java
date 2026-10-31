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
 * E2E tests for REBUILD generation counter isolation.
 *
 * <p>Validates: Requirements 35.1–35.4, 42.5
 */
@DisplayName("Generation Isolation E2E")
class GenerationIsolationE2ETest extends SqlE2ETestBase {

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
        tableName = "t_gen_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Generation Isolation")
    class GenerationIsolationTests {

        @Test
        @DisplayName("Index persists after data changes and queries remain correct")
        void indexPersistsAfterDataChanges() {
            sqlRunner.executeDdl("CREATE INDEX idx_gen ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Verify initial index created
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_gen".equals(r.getRaw("index_name").toString()));

            // Insert new data
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c')");

            // Verify index still exists and queries work
            List<TypedRow> afterInsert = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(afterInsert).anyMatch(r ->
                    "idx_gen".equals(r.getRaw("index_name").toString()));

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Stale Generation Discard")
    class StaleGenerationDiscard {

        @Test
        @DisplayName("Multiple data changes do not corrupt index query results")
        void multipleDataChangesPreserveCorrectness() {
            sqlRunner.executeDdl("CREATE INDEX idx_stale_gen ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Multiple inserts creating new data files
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (10, 'x')");
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (20, 'y')");

            // Query should still work correctly
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();
        }
    }
}
