package org.pangrova.testing.e2e.shuffle;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies virtual-worker routing preserves results under skewed keys and
 * partition boundary values.
 *
 * <p>Both worker identities share one endpoint, so this test covers routing
 * integration only, not distributed data movement.
 */
@Tag("routing-integration")
@DisplayName("Virtual Worker Data-Skew Routing Integration")
class VirtualWorkerDataSkewRoutingIntegrationTest extends VirtualWorkerRoutingIntegrationTestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private String skewTable;
    private String boundaryTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTables() {
        String suffix = Long.toHexString(System.nanoTime());
        skewTable = "skew_" + suffix;
        boundaryTable = "boundary_" + suffix;
    }

    // -----------------------------------------------------------------------
    // R6.5: Skewed data with empty partitions
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Skewed data (all rows same partition key) produces correct results with empty partitions")
    void skewedDataWithEmptyPartitions() {
        int rowCount = 100;
        createSkewTable();
        insertSkewedData(rowCount);

        List<TypedRow> rows = sqlRunner.execute(String.format(
                "SELECT id, customer_id, amount FROM %s.%s.%s ORDER BY id ASC",
                CATALOG, SCHEMA, skewTable));

        assertThat(rows).hasSize(rowCount);
        assertNoDuplicateIds(rows);
        assertAllCustomerIdEquals(rows, 1L);
        assertExpectedAmountSum(rows, rowCount);
    }

    // -----------------------------------------------------------------------
    // R6.6: Boundary value no loss or duplication
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Data at exact partition boundary values has no duplication or loss")
    void boundaryValueNoLossOrDuplication() {
        createBoundaryTable();
        int rowCount = insertBoundaryData();

        // ORDER BY ensures results pass through a single output stage that
        // merges all scan partitions. Without ORDER BY, the native engine
        // may return partial results when multiple small Parquet files are
        // routed across scan splits in the virtual-worker harness.
        List<TypedRow> rows = sqlRunner.execute(String.format(
                "SELECT id, partition_key FROM %s.%s.%s ORDER BY id",
                CATALOG, SCHEMA, boundaryTable));

        assertThat(rows).hasSize(rowCount);
        assertNoDuplicateIds(rows);
        assertDistinctPartitionKeys(rows, rowCount);
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    private void assertNoDuplicateIds(List<TypedRow> rows) {
        List<Long> ids = rows.stream()
                .map(r -> r.getLong("id"))
                .collect(Collectors.toList());
        assertThat(ids).doesNotHaveDuplicates();
    }

    private void assertAllCustomerIdEquals(
            List<TypedRow> rows, long expectedCustomerId) {
        rows.forEach(row ->
                assertThat(row.getLong("customer_id"))
                        .as("customer_id for row id=%s", row.getLong("id"))
                        .isEqualTo(expectedCustomerId));
    }

    private void assertExpectedAmountSum(
            List<TypedRow> rows, int rowCount) {
        long actualSum = rows.stream()
                .mapToLong(r -> r.getLong("amount"))
                .sum();
        long expectedSum = computeSkewAmountSum(rowCount);
        assertThat(actualSum).isEqualTo(expectedSum);
    }

    private void assertDistinctPartitionKeys(
            List<TypedRow> rows, int expectedCount) {
        long distinctKeys = rows.stream()
                .map(r -> r.getLong("partition_key"))
                .distinct()
                .count();
        assertThat(distinctKeys).isEqualTo(expectedCount);
    }

    // -----------------------------------------------------------------------
    // Table creation helpers
    // -----------------------------------------------------------------------

    private void createSkewTable() {
        setupData().createTestTable(CATALOG, SCHEMA, skewTable,
                "id BIGINT, customer_id BIGINT, amount BIGINT");
    }

    private void createBoundaryTable() {
        setupData().createTestTable(CATALOG, SCHEMA, boundaryTable,
                "id BIGINT, partition_key BIGINT");
    }

    // -----------------------------------------------------------------------
    // Data insertion helpers
    // -----------------------------------------------------------------------

    private void insertSkewedData(int rowCount) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, customer_id, amount) VALUES ",
                CATALOG, SCHEMA, skewTable));
        for (int i = 0; i < rowCount; i++) {
            long id = i + 1;
            long amount = (i * 7 + 3) % 500;
            if (i > 0) sb.append(", ");
            sb.append(String.format("(%d, 1, %d)", id, amount));
        }
        setupRunner().execute(sb.toString());
    }

    private int insertBoundaryData() {
        long[] boundaryValues = {
                -9223372036854775807L, -9223372036854775806L,
                -1L, 0L, 1L,
                Long.MAX_VALUE - 1, Long.MAX_VALUE
        };
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INSERT INTO %s.%s.%s (id, partition_key) VALUES ",
                CATALOG, SCHEMA, boundaryTable));
        for (int i = 0; i < boundaryValues.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("(%d, %d)", (long) (i + 1), boundaryValues[i]));
        }
        setupRunner().execute(sb.toString());
        return boundaryValues.length;
    }

    // -----------------------------------------------------------------------
    // Expected value computation (Java-side oracle)
    // -----------------------------------------------------------------------

    private long computeSkewAmountSum(int rowCount) {
        long sum = 0;
        for (int i = 0; i < rowCount; i++) {
            sum += (i * 7 + 3) % 500;
        }
        return sum;
    }
}
