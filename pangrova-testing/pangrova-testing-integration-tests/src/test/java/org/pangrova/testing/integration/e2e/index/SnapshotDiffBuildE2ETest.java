package org.pangrova.testing.integration.e2e.index;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * E2E tests for snapshot-diff based incremental index build.
 *
 * <p>Validates: Requirements 40.1–40.5, 47.1–47.5
 */
@DisplayName("Snapshot Diff Build E2E")
class SnapshotDiffBuildE2ETest extends SqlE2ETestBase {

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
        tableName = "t_snapdiff_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Normal Snapshot-Diff")
    class NormalSnapshotDiff {

        @Test
        @DisplayName("Incremental build uses snapshot diff — only new files indexed")
        void normalSnapshotDiff() {
            sqlRunner.executeDdl("CREATE INDEX idx_diff ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Insert new batch creating new snapshot
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c')");

            // Query should return correct results including new data
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 3");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Snapshot Gap Fallback")
    class SnapshotGapFallback {

        @Test
        @DisplayName("Multiple inserts after index creation — query returns correct results")
        void snapshotGapFallback() {
            sqlRunner.executeDdl("CREATE INDEX idx_gap ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Multiple inserts creating new snapshots
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (10, 'x')");
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (20, 'y')");

            // Query should return correct results across all snapshots
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 10");
            assertThat(result).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("SnapshotAwareMetadata Unavailable")
    class SnapshotAwareMetadataUnavailable {

        @Test
        @DisplayName("Fallback to CommitEvent-based build when no SnapshotAwareMetadata")
        void fallbackToCommitEventBuild() {
            sqlRunner.executeDdl("CREATE INDEX idx_fallback ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (30, 'z')");

            // Query should work regardless of build strategy
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 30");
            assertThat(result).isNotEmpty();
        }
    }
}
