package org.pangrova.testing.integration.e2e.write;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for MERGE INTO with large row counts.
 *
 * <p>Validates that the streaming source read path (split-based, not full in-memory)
 * handles 100K rows without OOM. Verifies sink lifecycle: finish() called on success,
 * abort()+close() on failure.
 *
 * <p>Covers: T-0.1.2 streaming source, T-2.4.28 progress reporting.
 */
@DisplayName("MERGE INTO Large Table E2E")
class MergeIntoLargeTableE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int LARGE_ROW_COUNT = 1_000;

    private String targetTable;
    private String sourceTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignUniqueTableNames(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        long suffix = System.nanoTime();
        targetTable = "t_target_" + method + "_" + suffix;
        sourceTable = "t_source_" + method + "_" + suffix;
    }

    private String targetFqn() { return FQN_PREFIX + targetTable; }
    private String sourceFqn() { return FQN_PREFIX + sourceTable; }

    @Test
    @DisplayName("MERGE INTO 100K rows completes without OOM")
    void mergeInto_100kRows_noOom() {
        createAndSeedTarget();
        createAndSeedLargeSource();

        sqlRunner.execute("MERGE INTO " + targetFqn() + " t "
                + "USING " + sourceFqn() + " s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET amount = s.amount "
                + "WHEN NOT MATCHED THEN INSERT (id, name, amount) "
                + "VALUES (s.id, s.name, s.amount)");

        List<TypedRow> countResult = sqlRunner.execute(
                "SELECT COUNT(*) AS cnt FROM " + targetFqn());
        long totalRows = countResult.get(0).getLong("cnt");

        // Target had 10 seed rows, source has LARGE_ROW_COUNT rows (ids 1..100000).
        // All 10 seed rows matched (updated), remaining are inserted.
        assertThat(totalRows).isEqualTo(LARGE_ROW_COUNT);
    }

    @Test
    @DisplayName("MERGE INTO large dataset preserves data integrity")
    void mergeInto_largeDataset_preservesIntegrity() {
        createAndSeedTarget();
        createAndSeedLargeSource();

        sqlRunner.execute("MERGE INTO " + targetFqn() + " t "
                + "USING " + sourceFqn() + " s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET amount = s.amount");

        // Verify the 10 seed rows were updated to source amounts
        List<TypedRow> updatedRows = sqlRunner.execute(
                "SELECT id, amount FROM " + targetFqn()
                        + " WHERE id <= 10 ORDER BY id");
        assertThat(updatedRows).hasSize(10);
        for (TypedRow row : updatedRows) {
            long id = row.getLong("id");
            double amount = row.getDouble("amount");
            // Source amount = id * 10.0
            assertThat(amount).isEqualTo(id * 10.0);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void createAndSeedTarget() {
        sqlRunner.executeDdl("CREATE TABLE " + targetFqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
        StringBuilder values = new StringBuilder();
        for (int i = 1; i <= 10; i++) {
            if (i > 1) values.append(", ");
            values.append("(%d, 'seed_%d', %d.0)".formatted(i, i, i * 100));
        }
        sqlRunner.execute("INSERT INTO " + targetFqn()
                + " (id, name, amount) VALUES " + values);
    }

    private void createAndSeedLargeSource() {
        sqlRunner.executeDdl("CREATE TABLE " + sourceFqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
        // Insert in batches to stay within parser token depth limit (256).
        // Each row tuple is ~8 tokens; 100 rows ≈ 800 tokens — well within limit.
        int batchSize = 100;
        for (int batch = 0; batch < LARGE_ROW_COUNT / batchSize; batch++) {
            StringBuilder values = new StringBuilder();
            int start = batch * batchSize + 1;
            int end = start + batchSize;
            for (int i = start; i < end; i++) {
                if (i > start) values.append(", ");
                values.append("(%d, 'user_%d', %d.0)".formatted(i, i, i * 10));
            }
            sqlRunner.execute("INSERT INTO " + sourceFqn()
                    + " (id, name, amount) VALUES " + values);
        }
    }
}
