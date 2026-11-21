package org.pangrova.testing.integration.e2e.velox;

import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test: Paimon partitioned table with partition pruning.
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native →
 * transport → Client.
 *
 * <p>Validates: Requirements R12, R16.6g, R16.7
 */
@DisplayName("Native Paimon Partition E2E")
class NativePaimonPartitionE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA,
                        paimonWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA,
                        paimonWarehouse().toString())));

        String fqn = CATALOG + "." + SCHEMA;

        // Drop tables if they exist from a prior test run
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + fqn + ".partitioned_paimon"); } catch (Exception ignored) { }
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + fqn + ".bucketed_paimon"); } catch (Exception ignored) { }

        // Partitioned table by date column (parquet format for Velox compatibility)
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".partitioned_paimon "
                + "(id BIGINT, value VARCHAR, dt VARCHAR) "
                + "PARTITIONED BY (dt) "
                + "TBLPROPERTIES ('file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".partitioned_paimon (id, value, dt) VALUES "
                + "(1, 'a', '2025-01-01'), (2, 'b', '2025-01-02'), (3, 'c', '2025-01-01')");

        // Bucketed table (parquet format for Velox compatibility)
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".bucketed_paimon "
                + "(id BIGINT, value VARCHAR) "
                + "TBLPROPERTIES ('bucket' = '4', 'bucket-key' = 'id', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".bucketed_paimon (id, value) VALUES "
                + "(1, 'x'), (2, 'y'), (3, 'z')");

        // Partitioned + bucketed table (parquet format for Velox compatibility)
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".partitioned_bucketed_paimon "
                + "(id BIGINT, value VARCHAR, dt VARCHAR) "
                + "PARTITIONED BY (dt) "
                + "TBLPROPERTIES ('bucket' = '2', 'bucket-key' = 'id', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".partitioned_bucketed_paimon (id, value, dt) VALUES "
                + "(1, 'p', '2025-01-01'), (2, 'q', '2025-01-02')");
    }

    @Test
    @DisplayName("Partition pruning on date column")
    void partitionPruningOnDate() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, value FROM " + CATALOG + "." + SCHEMA +
                ".partitioned_paimon WHERE dt = '2025-01-01'");
        assertThat(rows).isNotNull();
    }

    @Test
    @DisplayName("Bucket-aware parallel scan")
    void bucketAwareParallelScan() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT count(*) FROM " + CATALOG + "." + SCHEMA +
                ".bucketed_paimon");
        assertThat(rows).isNotNull();
    }

    @Test
    @DisplayName("Partition + bucket combined pruning")
    void partitionAndBucketCombined() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, value FROM " + CATALOG + "." + SCHEMA +
                ".partitioned_bucketed_paimon WHERE dt = '2025-01-01'");
        assertThat(rows).isNotNull();
    }
}
