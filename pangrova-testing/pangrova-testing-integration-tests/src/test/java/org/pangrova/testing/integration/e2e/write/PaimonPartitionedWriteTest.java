package org.pangrova.testing.integration.e2e.write;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for partitioned writes and partition-aware reads on Paimon tables.
 *
 * <p>Covers Requirements 110.1-110.2. All operations execute through the real
 * pipeline: HTTP → Coordinator → Worker → Native → Paimon Storage → transport.
 */
@DisplayName("Paimon Partitioned Write - E2E Tests")
class PaimonPartitionedWriteTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_part";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // --- 110.1: Partition-aware read ---

    @Nested
    @DisplayName("110.1 - Partition-aware read on Paimon partitioned table")
    class PartitionAwareRead {

        @Test
        @DisplayName("INSERT into partitioned table and read with partition filter")
        void partitionFilterReturnsCorrectData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, region VARCHAR,"
                    + " PRIMARY KEY (id, region) NOT ENFORCED)"
                    + " PARTITIONED BY (region)"
                    + " WITH ('file.format' = 'parquet')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, region) VALUES "
                    + "(1, 'alice', 'us'), (2, 'bob', 'eu'), (3, 'carol', 'us')");

            List<TypedRow> usRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'us' ORDER BY id");

            assertThat(usRows).hasSize(2);
            assertThat(usRows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(usRows.get(1).getLong("id")).isEqualTo(3L);
            assertThat(usRows.get(0).getString("region")).isEqualTo("us");
            assertThat(usRows.get(1).getString("region")).isEqualTo("us");
        }
    }

    // --- 110.2: Partition pruning ---

    @Nested
    @DisplayName("110.2 - Partition pruning on Paimon partitioned table")
    class PartitionPruning {

        @Test
        @DisplayName("Partition filter returns only targeted partition data")
        void partitionPruningReturnsSubset() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, region VARCHAR,"
                    + " PRIMARY KEY (id, region) NOT ENFORCED)"
                    + " PARTITIONED BY (region)"
                    + " WITH ('file.format' = 'parquet')");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, region) VALUES "
                    + "(1, 'alpha', 'us'), (2, 'beta', 'eu'), "
                    + "(3, 'gamma', 'ap'), (4, 'delta', 'eu')");

            List<TypedRow> euRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'eu' ORDER BY id");

            assertThat(euRows).hasSize(2);
            assertThat(euRows.get(0).getLong("id")).isEqualTo(2L);
            assertThat(euRows.get(1).getLong("id")).isEqualTo(4L);

            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(allRows).hasSize(4);
        }
    }
}
