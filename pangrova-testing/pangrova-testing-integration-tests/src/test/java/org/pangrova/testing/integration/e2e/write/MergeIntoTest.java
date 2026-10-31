package org.pangrova.testing.integration.e2e.write;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for MERGE INTO operations.
 *
 * <p>Covers MERGE with MATCHED UPDATE, MATCHED DELETE, NOT MATCHED INSERT,
 * and compound merge conditions through the full pipeline.
 */
@E2ETest
@DisplayName("MERGE INTO - E2E Tests")
class MergeIntoTest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_merge_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // ── MERGE with UPDATE ──

    @Nested
    @DisplayName("MERGE WHEN MATCHED UPDATE")
    class MergeMatchedUpdate {

        @Test
        @DisplayName("MERGE updates existing rows when matched")
        void mergeUpdatesMatched() {
            String target = fqn();
            executeDdl("CREATE TABLE " + target + " (id BIGINT, name VARCHAR, val BIGINT)");
            executeDml("INSERT INTO " + target + " VALUES (1, 'old_a', 10), (2, 'old_b', 20)");

            String source = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + source + " (id BIGINT, name VARCHAR, val BIGINT)");
            executeDml("INSERT INTO " + source + " VALUES (1, 'new_a', 100), (3, 'new_c', 300)");

            executeDml("MERGE INTO " + target + " t USING " + source + " s "
                    + "ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, val = s.val "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, val) VALUES (s.id, s.name, s.val)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + target + " ORDER BY id");
            assertThat(rows).hasSize(3);
        }
    }

    // ── MERGE with DELETE ──

    @Nested
    @DisplayName("MERGE WHEN MATCHED DELETE")
    class MergeMatchedDelete {

        @Test
        @DisplayName("MERGE deletes matched rows")
        void mergeDeletesMatched() {
            String target = fqn();
            executeDdl("CREATE TABLE " + target + " (id BIGINT, status VARCHAR)");
            executeDml("INSERT INTO " + target + " VALUES (1, 'active'), (2, 'inactive'), (3, 'active')");

            String source = FQN_PREFIX + tableName + "_del";
            executeDdl("CREATE TABLE " + source + " (id BIGINT)");
            executeDml("INSERT INTO " + source + " VALUES (2)");

            executeDml("MERGE INTO " + target + " t USING " + source + " s "
                    + "ON t.id = s.id "
                    + "WHEN MATCHED THEN DELETE");

            assertQueryReturnsRows("SELECT * FROM " + target, 2);
        }
    }

    // ── MERGE NOT MATCHED INSERT only ──

    @Nested
    @DisplayName("MERGE NOT MATCHED INSERT")
    class MergeNotMatchedInsert {

        @Test
        @DisplayName("MERGE inserts new rows for non-matching source")
        void mergeInsertsNotMatched() {
            String target = fqn();
            executeDdl("CREATE TABLE " + target + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + target + " VALUES (1, 'existing')");

            String source = FQN_PREFIX + tableName + "_new";
            executeDdl("CREATE TABLE " + source + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + source + " VALUES (2, 'added'), (3, 'added2')");

            executeDml("MERGE INTO " + target + " t USING " + source + " s "
                    + "ON t.id = s.id "
                    + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)");

            assertQueryReturnsRows("SELECT * FROM " + target, 3);
        }
    }
}
