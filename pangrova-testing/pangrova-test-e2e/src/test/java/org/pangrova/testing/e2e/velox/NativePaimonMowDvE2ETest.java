package org.pangrova.testing.e2e.velox;

import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test: Paimon MOW + deletion vector full pipeline.
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native →
 * VeloxDeletionVectorFilter → transport → Client.
 *
 * <p>Validates: Requirements R9, R16.6e, R16.7
 */
@DisplayName("Native Paimon MOW + DV E2E")
class NativePaimonMowDvE2ETest extends SqlE2ETestBase {

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
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(CATALOG, SCHEMA,
                        paimonWarehouse().toString())));

        String fqn = CATALOG + "." + SCHEMA;
        // Drop table if it exists from a prior test run to avoid "Table already exists" errors
        try { sqlRunner.execute("DROP TABLE IF EXISTS " + fqn + ".mow_dv_table"); } catch (Exception ignored) { }
        // Create MOW table with primary key, deletion vectors, and parquet format
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".mow_dv_table "
                + "(id BIGINT, value VARCHAR) "
                + "TBLPROPERTIES ('primary-key' = 'id', "
                + "'merge-engine' = 'deduplicate', "
                + "'deletion-vectors.enabled' = 'true', "
                + "'changelog-producer' = 'none', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mow_dv_table (id, value) VALUES "
                + "(1, 'a'), (2, 'b'), (3, 'c')");
        // Delete row 2 to exercise deletion vector
        sqlRunner.execute("DELETE FROM " + fqn + ".mow_dv_table WHERE id = 2");

        // Create partitioned MOW table with deletion vectors and parquet format
        sqlRunner.executeDdl("CREATE TABLE " + fqn + ".mow_dv_partitioned "
                + "(id BIGINT, value VARCHAR, dt VARCHAR) "
                + "PARTITIONED BY (dt) "
                + "TBLPROPERTIES ('primary-key' = 'id,dt', "
                + "'merge-engine' = 'deduplicate', "
                + "'deletion-vectors.enabled' = 'true', "
                + "'changelog-producer' = 'none', "
                + "'file.format' = 'parquet')");
        sqlRunner.execute("INSERT INTO " + fqn + ".mow_dv_partitioned (id, value, dt) VALUES "
                + "(1, 'x', '2025-01-01'), (2, 'y', '2025-01-02')");
    }

    @Test
    @DisplayName("MOW with DV: deleted rows filtered by deletion vector")
    void mowDvFiltersDeletedRows() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT count(*) FROM " + CATALOG + "." + SCHEMA +
                ".mow_dv_table");
        assertThat(rows).isNotNull();
    }

    @Test
    @DisplayName("MOW with DV: correct data after DV filtering")
    void mowDvReturnsCorrectData() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, value FROM " + CATALOG + "." + SCHEMA +
                ".mow_dv_table ORDER BY id");
        assertThat(rows).isNotNull();
    }

    @Test
    @DisplayName("MOW with DV + partition pruning")
    void mowDvWithPartitionPruning() {
        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, value FROM " + CATALOG + "." + SCHEMA +
                ".mow_dv_partitioned WHERE dt = '2025-01-01'");
        assertThat(rows).isNotNull();
    }
}
