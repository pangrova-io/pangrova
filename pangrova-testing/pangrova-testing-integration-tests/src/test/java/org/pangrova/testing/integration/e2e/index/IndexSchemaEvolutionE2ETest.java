package org.pangrova.testing.integration.e2e.index;

import org.pangrova.types.value.TypedRow;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
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

    private void applyPendingSchemaChanges() {
        e2eIndexingBootstrap().getSchemaEvolutionHandler().recoverPending();
    }

    private IndexDescriptor indexDescriptor(String indexName) {
        return e2eIndexingBootstrap().getMetadataStore()
                .findIndexesByTable(e2eTenantId(), CATALOG, SCHEMA, tableName).stream()
                .filter(index -> indexName.equals(index.indexName()))
                .findFirst()
                .orElseThrow();
    }

    /** These cases mutate an already built index, not a concurrently submitted build. */
    private void createReadyIndex(String indexName, String columnName, String indexType) {
        sqlRunner.executeDdl("CREATE INDEX " + indexName + " ON " + fqn()
                + "(" + columnName + ") USING " + indexType);
        // CREATE INDEX is asynchronous and first commits an Iceberg snapshot pin.
        // Establish the READY precondition before the schema DDL; do not retry DDL
        // conflicts or allow a never-built BUILDING index to satisfy the final assertion.
        org.awaitility.Awaitility.await("schema-evolution index ready: " + indexName)
                .atMost(java.time.Duration.ofSeconds(30))
                .pollInterval(java.time.Duration.ofMillis(200))
                .pollDelay(java.time.Duration.ZERO)
                .until(() -> {
                    for (TypedRow row : sqlRunner.execute("SHOW INDEXES FROM " + fqn())) {
                        if (!indexName.equals(row.getRaw("index_name").toString())) continue;
                        String state = row.getRaw("state").toString();
                        if ("STALE".equals(state) || "DROPPED".equals(state)) {
                            throw new IllegalStateException(
                                    "Index setup failed: " + indexName + " state=" + state);
                        }
                        return "READY".equals(state);
                    }
                    return false;
                });
    }

    @Nested
    @DisplayName("DROP COLUMN Impact")
    class DropColumnImpact {

        @Test
        @DisplayName("DROP COLUMN drops index on that column")
        void dropColumnDropsIndex() {
            createReadyIndex("idx_a", "col_a", "BLOOM_FILTER");
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " DROP COLUMN col_a");
            applyPendingSchemaChanges();

            assertThat(indexDescriptor("idx_a").state()).isEqualTo(IndexState.DROPPED);
        }
    }

    @Nested
    @DisplayName("RENAME COLUMN Impact")
    class RenameColumnImpact {

        @Test
        @DisplayName("RENAME COLUMN rebinds the index and requires a rebuild")
        void renameColumnPreservesIndex() {
            createReadyIndex("idx_b", "col_b", "INVERTED");
            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " RENAME COLUMN col_b TO col_b_new");
            applyPendingSchemaChanges();

            IndexDescriptor rebound = indexDescriptor("idx_b");
            assertThat(rebound.state()).isEqualTo(IndexState.STALE);
            assertThat(rebound.columnNames()).containsExactly("col_b_new");
        }
    }

    @Nested
    @DisplayName("DROP TABLE Cascade")
    class DropTableCascade {

        @Test
        @DisplayName("DROP TABLE drops all indexes on that table")
        void dropTableDropsAllIndexes() {
            createReadyIndex("idx_a", "col_a", "BLOOM_FILTER");
            createReadyIndex("idx_b", "col_b", "INVERTED");
            sqlRunner.executeDdl("DROP TABLE " + fqn());
            applyPendingSchemaChanges();

            assertThat(indexDescriptor("idx_a").state()).isEqualTo(IndexState.DROPPED);
            assertThat(indexDescriptor("idx_b").state()).isEqualTo(IndexState.DROPPED);
        }
    }
}
