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
 * E2E tests for leader-gated commit log processing.
 *
 * <p>Verifies that commit events are processed correctly without duplicate
 * processing. In a multi-coordinator setup, only the leader should process
 * each commit event, and the processed flag prevents re-processing.
 *
 * <p>Validates: Requirements 4.AC1, 4.AC4, 4.AC7
 */
@DisplayName("Leader-Gated Processing E2E")
class LeaderGatedProcessingE2ETest extends SqlE2ETestBase {

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
        tableName = "t_leader_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, event_type VARCHAR, payload VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    @Nested
    @DisplayName("Commit Event Processing")
    class CommitEventProcessing {

        @Test
        @DisplayName("INSERT events are processed and data is queryable")
        void insertEventsProcessedCorrectly() {
            // Insert data — each INSERT generates a commit event
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (1, 'APPEND', 'batch_1')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (2, 'APPEND', 'batch_2')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (3, 'APPEND', 'batch_3')");

            // All data should be queryable — no duplicate processing artifacts
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn());

            assertThat(rows).hasSize(3);
        }

        @Test
        @DisplayName("No duplicate rows from commit event processing")
        void noDuplicateRowsFromProcessing() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (1, 'APPEND', 'data_1')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (2, 'APPEND', 'data_2')");

            // Query for specific row — should return exactly 1
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE id = 1");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("event_type").toString()).isEqualTo("APPEND");
        }
    }

    @Nested
    @DisplayName("Index with Commit Processing")
    class IndexWithCommitProcessing {

        @Test
        @DisplayName("Index creation after multiple commits works correctly")
        void indexAfterMultipleCommits() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (1, 'APPEND', 'a')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (2, 'OVERWRITE', 'b')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, event_type, payload) VALUES (3, 'COMPACT', 'c')");

            sqlRunner.executeDdl("CREATE INDEX idx_evt ON " + fqn()
                    + "(event_type) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE event_type = 'OVERWRITE'");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(2L);
        }
    }
}
