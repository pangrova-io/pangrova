package org.pangrova.testing.integration.e2e.index.boundary;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.fixture.IndexTestFixture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying TIMESTAMP column index support for BTREE, SPARSE,
 * and BLOOM_FILTER index types.
 *
 * <p>Test data: 4 separate INSERT batches with monthly-separated TIMESTAMP
 * values to force 4 data files with non-overlapping time ranges.</p>
 *
 * <p>Validates: Requirements R9.1, R9.2, R9.3, R9.4</p>
 */
@DisplayName("Index TIMESTAMP Type E2E — time-range queries with indexes")
class IndexTimestampTypeE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    // -----------------------------------------------------------------------
    // 15.2: BTREE on TIMESTAMP range query
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BTREE TIMESTAMP Range")
    class BTreeTimestampRange {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_ts_btree", info);
        }

        @Test
        @DisplayName("BTREE range query on TIMESTAMP returns correct results")
        void btreeTimestampRangeReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt_ts ON " + fqn
                    + "(ts) USING BTREE FORCE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE ts BETWEEN TIMESTAMP '2025-01-01 00:00:00'"
                            + " AND TIMESTAMP '2025-02-01 00:00:00'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                String name = r.getRaw("name").toString();
                return "Alice".equals(name) || "Bob".equals(name)
                        || "Charlie".equals(name);
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for BTREE TIMESTAMP range")
        void btreeTimestampCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "ts BETWEEN TIMESTAMP '2025-01-01 00:00:00'"
                            + " AND TIMESTAMP '2025-02-01 00:00:00'",
                    "CREATE INDEX idx_bt_ts ON " + fqn + "(ts) USING BTREE FORCE");
        }
    }

    // -----------------------------------------------------------------------
    // 15.3: SPARSE on TIMESTAMP range query
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("SPARSE TIMESTAMP Range")
    class SparseTimestampRange {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_ts_sparse", info);
        }

        @Test
        @DisplayName("SPARSE range query on TIMESTAMP returns correct results")
        void sparseTimestampRangeReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_sp_ts ON " + fqn
                    + "(ts) USING SPARSE FORCE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE ts BETWEEN TIMESTAMP '2025-01-01 00:00:00'"
                            + " AND TIMESTAMP '2025-02-01 00:00:00'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                String name = r.getRaw("name").toString();
                return "Alice".equals(name) || "Bob".equals(name)
                        || "Charlie".equals(name);
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for SPARSE TIMESTAMP range")
        void sparseTimestampCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "ts BETWEEN TIMESTAMP '2025-01-01 00:00:00'"
                            + " AND TIMESTAMP '2025-02-01 00:00:00'",
                    "CREATE INDEX idx_sp_ts ON " + fqn + "(ts) USING SPARSE FORCE");
        }
    }

    // -----------------------------------------------------------------------
    // 15.4: BLOOM_FILTER on TIMESTAMP equality query
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BLOOM_FILTER TIMESTAMP Equality")
    class BloomFilterTimestampEquality {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_ts_bloom", info);
        }

        @Test
        @DisplayName("BLOOM_FILTER equality query on TIMESTAMP returns correct results")
        void bloomFilterTimestampEqualityReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf_ts ON " + fqn
                    + "(ts) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE ts = TIMESTAMP '2025-01-01 00:00:00'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r ->
                    "Alice".equals(r.getRaw("name").toString()));
        }

        @Test
        @DisplayName("Correctness invariant holds for BLOOM_FILTER TIMESTAMP equality")
        void bloomFilterTimestampCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "ts = TIMESTAMP '2025-01-01 00:00:00'",
                    "CREATE INDEX idx_bf_ts ON " + fqn + "(ts) USING BLOOM_FILTER");
        }
    }
}
