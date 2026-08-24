package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for ALTER TABLE impact on indexes (schema evolution).
 *
 * <p>Validates: Requirements 28.1–28.8
 */
@DisplayName("Index Schema Evolution E2E")
class IndexSchemaEvolutionE2ETest extends SqlE2ETestBase {

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
        tableName = "t_evo_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (col_a BIGINT, col_b VARCHAR, col_c DOUBLE)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'hello', 10.0)");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("DROP COLUMN Impact")
    class DropColumnImpact {

        @Test
        @DisplayName("DROP COLUMN drops index on that column")
        void dropColumnDropsIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_a ON " + fqn()
                    + "(col_a) USING BLOOM_FILTER");
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " DROP COLUMN col_a");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasActiveIndex = indexes.stream()
                    .anyMatch(r -> "idx_a".equals(r.getRaw("index_name").toString())
                            && "READY".equals(r.getRaw("state").toString()));
            assertThat(hasActiveIndex).isFalse();
        }
    }

    @Nested
    @DisplayName("RENAME COLUMN Impact")
    class RenameColumnImpact {

        @Test
        @DisplayName("RENAME COLUMN preserves index (column reference not auto-updated)")
        void renameColumnPreservesIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_b ON " + fqn()
                    + "(col_b) USING INVERTED");
            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " RENAME COLUMN col_b TO col_b_new");

            // Index metadata does not auto-update column references on rename.
            // Verify the index still exists and is not silently dropped.
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasIndex = indexes.stream()
                    .anyMatch(r -> "idx_b".equals(r.getRaw("index_name").toString()));
            assertThat(hasIndex).isTrue();
        }
    }

    @Nested
    @DisplayName("DROP TABLE Cascade")
    class DropTableCascade {

        @Test
        @DisplayName("DROP TABLE drops all indexes on that table")
        void dropTableDropsAllIndexes() {
            sqlRunner.executeDdl("CREATE INDEX idx_a ON " + fqn()
                    + "(col_a) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_b ON " + fqn()
                    + "(col_b) USING INVERTED");
            sqlRunner.executeDdl("DROP TABLE " + fqn());

            // Table gone — SHOW INDEXES returns empty or errors.
            // The system may return empty results for a dropped table.
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasActiveIndex = indexes.stream()
                    .anyMatch(r -> "READY".equals(r.getRaw("state").toString()));
            assertThat(hasActiveIndex).isFalse();
        }
    }
}
