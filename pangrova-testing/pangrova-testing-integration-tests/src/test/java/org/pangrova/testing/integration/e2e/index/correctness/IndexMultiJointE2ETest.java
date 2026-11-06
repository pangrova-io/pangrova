package org.pangrova.testing.integration.e2e.index.correctness;

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
 * E2E tests verifying multi-index joint query correctness — queries that hit
 * multiple different index types on the same table in a single WHERE clause.
 *
 * <p>Correctness invariant: for any combination of indexes and compatible
 * compound predicates, the result set WITH indexes must be a superset of
 * (containsAll) the result set WITHOUT indexes.</p>
 *
 * <p>Each nested class creates its own table per test method to avoid
 * interference. Data is inserted across 4 separate INSERT batches to force
 * multiple data files with non-overlapping value ranges.</p>
 *
 * <p>Validates: Requirements R4.1, R4.2, R4.3</p>
 */
@DisplayName("Index Multi-Joint Query E2E — multi-index compound predicates")
class IndexMultiJointE2ETest extends SqlE2ETestBase {

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

    /**
     * Core correctness assertion: withIndex result must contain all baseline rows.
     */
    private static void assertCorrectnessInvariant(
            String fqn, String query, String... indexDdls) {
        List<TypedRow> baseline = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE " + query);
        for (String ddl : indexDdls) {
            sqlRunner.executeDdl(ddl);
        }
        List<TypedRow> withIndex = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE " + query);
        assertThat(withIndex).containsAll(baseline);
    }

    // -----------------------------------------------------------------------
    // 13.2: Dual-index joint query (BLOOM_FILTER on user_id + BTREE on amount)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Dual-Index Joint Query")
    class DualIndexJointQuery {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_dual_joint", info);
        }

        @Test
        @DisplayName("BLOOM_FILTER + BTREE: joint query returns correct results")
        void bloomFilterAndBtreeJointQuery() {
            assertCorrectnessInvariant(fqn,
                    "user_id = 1 AND amount BETWEEN 40.0 AND 80.0",
                    "CREATE INDEX idx_bf_uid ON " + fqn + "(user_id) USING BLOOM_FILTER",
                    "CREATE INDEX idx_bt_amt ON " + fqn + "(amount) USING BTREE");
        }

        @Test
        @DisplayName("BLOOM_FILTER + BTREE: joint query result values are correct")
        void bloomFilterAndBtreeResultValues() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE user_id = 1 AND amount BETWEEN 40.0 AND 80.0");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long uid = ((Number) r.getRaw("user_id")).longValue();
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                return uid == 1L && amt >= 40.0 && amt <= 80.0;
            });
        }
    }

    // -----------------------------------------------------------------------
    // 13.3: Triple-index joint query (BLOOM_FILTER + BTREE + BITMAP)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Triple-Index Joint Query")
    class TripleIndexJointQuery {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_triple_joint", info);
        }

        @Test
        @DisplayName("BLOOM_FILTER + BTREE + BITMAP: triple joint query returns correct results")
        void tripleIndexJointQuery() {
            assertCorrectnessInvariant(fqn,
                    "user_id = 1 AND (amount BETWEEN 40.0 AND 80.0) AND status = 'ACTIVE'",
                    "CREATE INDEX idx_bf_uid ON " + fqn + "(user_id) USING BLOOM_FILTER",
                    "CREATE INDEX idx_bt_amt ON " + fqn + "(amount) USING BTREE",
                    "CREATE INDEX idx_bm_st ON " + fqn + "(status) USING BITMAP");
        }

        @Test
        @DisplayName("BLOOM_FILTER + BTREE + BITMAP: triple joint query result values are correct")
        void tripleIndexResultValues() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE user_id = 1 AND (amount BETWEEN 40.0 AND 80.0)"
                            + " AND status = 'ACTIVE'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long uid = ((Number) r.getRaw("user_id")).longValue();
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                String st = r.getRaw("status").toString();
                return uid == 1L && amt >= 40.0 && amt <= 80.0 && "ACTIVE".equals(st);
            });
        }
    }
}
