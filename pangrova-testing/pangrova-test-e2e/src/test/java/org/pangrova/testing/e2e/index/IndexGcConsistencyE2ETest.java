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
 * E2E tests for index GC and consistency maintenance.
 *
 * <p>Validates: Requirements 13.1–13.5, 14.1–14.5
 */
@DisplayName("Index GC and Consistency E2E")
class IndexGcConsistencyE2ETest extends SqlE2ETestBase {

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
        tableName = "t_gc_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("DROPPED Index GC")
    class DroppedIndexGc {

        @Test
        @DisplayName("DROP INDEX transitions to DROPPED state")
        void dropIndexTransitionsToDropped() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("DROP INDEX idx_bf ON " + fqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            // Either empty or all DROPPED
            assertThat(indexes).allMatch(r -> {
                String state = r.getRaw("state").toString();
                return "DROPPED".equals(state);
            });
        }
    }

    @Nested
    @DisplayName("Consistency After Data Change")
    class ConsistencyAfterDataChange {

        @Test
        @DisplayName("Query still works after data changes with index present")
        void queryWorksAfterDataChange() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(id) USING BLOOM_FILTER");

            // Insert more data
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c'), (4, 'd')");

            // Query should still return correct results
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE id = 3");
            assertThat(rows).isNotEmpty();
        }
    }
}
