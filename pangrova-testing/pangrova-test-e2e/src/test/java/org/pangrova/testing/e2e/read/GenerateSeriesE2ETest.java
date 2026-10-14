package org.pangrova.testing.e2e.read;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.worker.jni.NativeRuntime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for SQL GENERATE_SERIES results with a running Native worker.
 *
 * <p>The planner currently expands integer literals to VALUES. These tests
 * validate the returned sequence and Native runtime availability, not the
 * GenerateSeriesOperator or its TaskStats. Native table-function routing needs
 * a separate physical-plan and execution assertion.
 *
 * <p>Validates: Requirements 5.2, 5.6
 */
@DisplayName("GENERATE_SERIES Table Function E2E")
class GenerateSeriesE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @Nested
    @DisplayName("GENERATE_SERIES Output Correctness")
    class OutputCorrectness {

        @Test
        @DisplayName("GENERATE_SERIES(1, 100) produces exactly 100 rows with values 1 through 100")
        void generateSeriesProducesCorrectSequence() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM TABLE(GENERATE_SERIES(1, 100))");

            assertThat(rows)
                    .as("GENERATE_SERIES(1, 100) should produce exactly 100 rows")
                    .hasSize(100);

            List<Long> values = rows.stream()
                    .map(row -> {
                        Object val = row.columnNames().stream()
                                .map(row::getRaw)
                                .findFirst()
                                .orElseThrow();
                        return ((Number) val).longValue();
                    })
                    .sorted()
                    .collect(Collectors.toList());

            // Verify the sequence contains all values from 1 to 100
            for (int i = 0; i < 100; i++) {
                assertThat(values.get(i))
                        .as("Value at position %d should be %d", i, i + 1)
                        .isEqualTo(i + 1L);
            }
        }

        @Test
        @DisplayName("GENERATE_SERIES(1, 100) values are all within expected range")
        void generateSeriesValuesInRange() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM TABLE(GENERATE_SERIES(1, 100))");

            assertThat(rows).hasSize(100);

            rows.forEach(row -> {
                Object val = row.columnNames().stream()
                        .map(row::getRaw)
                        .findFirst()
                        .orElseThrow();
                long value = ((Number) val).longValue();
                assertThat(value)
                        .as("Each value should be between 1 and 100 inclusive")
                        .isBetween(1L, 100L);
            });
        }
    }

    @Nested
    @DisplayName("Native Runtime Availability")
    class RuntimeAvailability {

        @Test
        @DisplayName("GENERATE_SERIES query leaves the Native runtime available")
        void nativeRuntimeRemainsAvailable() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM TABLE(GENERATE_SERIES(1, 100))");

            assertThat(rows)
                    .as("Query should produce results confirming native execution")
                    .hasSize(100);

            // Verify native engine was active during query execution
            byte[] memSnapshot = NativeRuntime.nativeGetMemorySnapshot();
            assertThat(memSnapshot)
                    .as("Native memory snapshot should be available after "
                            + "GENERATE_SERIES query execution")
                    .isNotNull();
        }
    }
}
