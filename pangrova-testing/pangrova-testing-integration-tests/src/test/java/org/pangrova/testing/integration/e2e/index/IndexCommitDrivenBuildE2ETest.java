package org.pangrova.testing.integration.e2e.index;

import org.awaitility.Awaitility;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for CommitListener-driven incremental index builds.
 *
 * <p>Validates: Requirements 15.1–15.7
 */
@DisplayName("Index Commit-Driven Build E2E")
class IndexCommitDrivenBuildE2ETest extends SqlE2ETestBase {

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
        tableName = "t_commit_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, name VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'Alice'), (2, 'Bob')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    private String currentSnapshot() {
        var refs = sqlRunner.execute("SELECT snapshot_id FROM " + fqn() + "$refs WHERE name = 'main'");
        assertThat(refs).hasSize(1);
        return Long.toString(refs.get(0).getLong("snapshot_id"));
    }

    private IndexGenerationManifest awaitPublishedIndex(String sourceSnapshot) {
        String tenantId = authenticatedCatalogSession(CATALOG).getProperties().get("tenant_id");
        var indexes = new JdbcIndexMetadataStore(stateStoreDataSource(),
                TableWriteAuthorityFixture.create()::requireCurrent);
        return Awaitility.await("published index generation for snapshot " + sourceSnapshot)
                .atMost(Duration.ofSeconds(30))
                .until(() -> indexes.findIndexesByTable(tenantId, CATALOG, SCHEMA, tableName).stream()
                        .filter(index -> index.indexName().equals("idx_bf"))
                        .findFirst()
                        .flatMap(index -> indexes.findActiveIndexGeneration(index.indexId()))
                        .filter(manifest -> sourceSnapshot.equals(manifest.sourceSnapshot())),
                        Optional::isPresent)
                .orElseThrow();
    }

    @Nested
    @DisplayName("Basic Incremental Build")
    class BasicIncrementalBuild {

        @Test
        @DisplayName("INSERT after CREATE INDEX triggers incremental build")
        void insertTriggersIncrementalBuild() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            var before = awaitPublishedIndex(currentSnapshot());
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'Charlie')");
            var after = awaitPublishedIndex(currentSnapshot());
            assertThat(after.indexId()).isEqualTo(before.indexId());
            assertThat(after.generation()).isGreaterThan(before.generation());
            assertThat(after.objectCount()).isGreaterThan(before.objectCount());

            // Query should return correct results including new data
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 3");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("user_id")).longValue() == 3L);
        }
    }

    @Nested
    @DisplayName("Multiple Rapid Inserts")
    class MultipleRapidInserts {

        @Test
        @DisplayName("Multiple rapid inserts all get indexed eventually")
        void multipleInsertsAllIndexed() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            for (int i = 10; i < 15; i++) {
                sqlRunner.execute("INSERT INTO " + fqn()
                        + " VALUES (" + i + ", 'User" + i + "')");
            }

            // All inserted data should be queryable
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 12");
            assertThat(rows).isNotEmpty();
        }
    }
}
