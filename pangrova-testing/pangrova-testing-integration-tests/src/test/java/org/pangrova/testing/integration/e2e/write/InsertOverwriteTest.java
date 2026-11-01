package org.pangrova.testing.integration.e2e.write;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * E2E tests for INSERT OVERWRITE operations.
 *
 * <p>Covers INSERT OVERWRITE full table replacement and partition-level
 * overwrite semantics through the full pipeline.
 */
@E2ETest
@DisplayName("INSERT OVERWRITE - E2E Tests")
class InsertOverwriteTest extends PangrovaE2ETestBase {

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
        tableName = "t_overwrite_" + method.replaceAll("[^a-zA-Z0-9]", "_") + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // ── Full table overwrite ──

    @Nested
    @DisplayName("Full table INSERT OVERWRITE")
    class FullOverwrite {

        @Test
        @DisplayName("INSERT OVERWRITE replaces all existing data")
        void overwriteReplacesAll() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'old1'), (2, 'old2'), (3, 'old3')");
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 3);

            executeDml("INSERT OVERWRITE " + fqn() + " VALUES (10, 'new1'), (20, 'new2')");
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 2);
        }

        @Test
        @DisplayName("INSERT OVERWRITE with SELECT replaces with query result")
        void overwriteWithSelect() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 100), (2, 200), (3, 300)");

            String src = FQN_PREFIX + tableName + "_src";
            executeDdl("CREATE TABLE " + src + " (id BIGINT, val BIGINT)");
            executeDml("INSERT INTO " + src + " VALUES (10, 1000), (20, 2000)");

            executeDml("INSERT OVERWRITE " + fqn() + " SELECT * FROM " + src);
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 2);
        }

        @Test
        @DisplayName("INSERT OVERWRITE with empty result empties table")
        void overwriteWithEmpty() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'data')");

            String src = FQN_PREFIX + tableName + "_empty";
            executeDdl("CREATE TABLE " + src + " (id BIGINT, val VARCHAR)");

            executeDml("INSERT OVERWRITE " + fqn() + " SELECT * FROM " + src);
            assertQueryReturnsEmpty("SELECT * FROM " + fqn());
        }
    }

    // ── Partitioned overwrite ──

    @Nested
    @DisplayName("Partitioned INSERT OVERWRITE")
    class PartitionedOverwrite {

        private void createPartitionedTable() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'eu'), (2, 'us')");
        }

        @Test
        @DisplayName("Unscoped overwrite empties a partition absent from the result")
        void overwriteEmptiesVanishedPartition() {
            createPartitionedTable();

            executeDml("INSERT OVERWRITE " + fqn() + " VALUES (3, 'eu')");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(1);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(3L);
            assertThat(rows.get(0).getRaw("region")).isEqualTo("eu");
        }

        @Test
        @DisplayName("Static partition overwrite leaves other partitions untouched")
        void overwriteStaticPartitionKeepsOtherPartitions() {
            createPartitionedTable();

            executeDml("INSERT OVERWRITE " + fqn() + " (id) PARTITION (region = 'eu') VALUES (10)");

            List<TypedRow> rows = executeQuery("SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(rows.get(0).getRaw("region")).isEqualTo("us");
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(10L);
            assertThat(rows.get(1).getRaw("region")).isEqualTo("eu");
        }
    }
}
