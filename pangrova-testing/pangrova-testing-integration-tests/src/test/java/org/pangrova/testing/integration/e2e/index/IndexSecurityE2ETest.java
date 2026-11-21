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
 * E2E smoke tests for index DDL under the authenticated, fully granted fixture.
 *
 * <p>Validates: Requirements 43.1–43.4
 */
@DisplayName("Index Security E2E")
class IndexSecurityE2ETest extends SqlE2ETestBase {

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
        tableName = "t_sec_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("CREATE INDEX with granted access")
    class CreateIndexAccessControl {

        @Test
        @DisplayName("CREATE INDEX with ALTER and SELECT permission succeeds")
        void createIndexWithPermission() {
            sqlRunner.executeDdl("CREATE INDEX idx_sec_create ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_sec_create".equals(r.getRaw("index_name").toString()));
        }
    }

    @Nested
    @DisplayName("DROP INDEX with granted access")
    class DropIndexAccessControl {

        @Test
        @DisplayName("DROP INDEX with ALTER permission succeeds")
        void dropIndexWithPermission() {
            sqlRunner.executeDdl("CREATE INDEX idx_sec_drop ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("DROP INDEX idx_sec_drop ON " + fqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasIdx = indexes.stream()
                    .anyMatch(r -> "idx_sec_drop".equals(r.getRaw("index_name").toString())
                            && "READY".equals(r.getRaw("state").toString()));
            assertThat(hasIdx).isFalse();
        }
    }

    @Nested
    @DisplayName("SHOW INDEXES Access Control")
    class ShowIndexesAccessControl {

        @Test
        @DisplayName("SHOW INDEXES with SELECT permission succeeds")
        void showIndexesWithPermission() {
            sqlRunner.executeDdl("CREATE INDEX idx_sec_show ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("ALTER INDEX REBUILD Access Control")
    class AlterIndexRebuildAccessControl {

        @Test
        @DisplayName("ALTER INDEX REBUILD on freshly created index succeeds")
        void rebuildSucceedsOnFreshIndex() {
            sqlRunner.executeDdl("CREATE INDEX idx_sec_rebuild ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            IndexReadiness.awaitReady(sqlRunner, fqn(), "idx_sec_rebuild");

            sqlRunner.executeDdl(
                    "ALTER INDEX idx_sec_rebuild ON " + fqn() + " REBUILD");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Full Permission DDL")
    class FullPermissionDdl {

        @Test
        @DisplayName("CREATE, SHOW, DROP DDL operations succeed with correct permissions")
        void allDdlWithPermissions() {
            sqlRunner.executeDdl("CREATE INDEX idx_sec_all ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            sqlRunner.executeDdl("DROP INDEX idx_sec_all ON " + fqn());
        }
    }
}
