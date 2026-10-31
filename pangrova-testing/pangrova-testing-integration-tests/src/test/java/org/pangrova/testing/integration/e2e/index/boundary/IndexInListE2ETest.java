package org.pangrova.testing.integration.e2e.index.boundary;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E tests verifying InList (IN clause) predicate handling for index types
 * that support it: BLOOM_FILTER, HASH, BITMAP, BTREE.
 *
 * <p>Each test creates an index, executes a {@code WHERE col IN (v1, v2, v3)}
 * query, and verifies both result correctness and the containsAll invariant.</p>
 *
 * <p>Validates: Requirements R7.1, R7.2</p>
 */
@DisplayName("Index InList E2E — IN clause predicate with indexes")
class IndexInListE2ETest extends SqlE2ETestBase {

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
    // 14.4: InList tests per index type
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BLOOM_FILTER InList")
    class BloomFilterInList {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_inlist_bf", info);
        }

        @Test
        @DisplayName("IN clause returns correct results with BLOOM_FILTER index")
        void inListReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id IN (1, 10, 100)");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long uid = ((Number) r.getRaw("user_id")).longValue();
                return uid == 1L || uid == 10L || uid == 100L;
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for IN clause")
        void inListCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "user_id IN (1, 10, 100)",
                    "CREATE INDEX idx_bf ON " + fqn + "(user_id) USING BLOOM_FILTER");
        }
    }

    @Nested
    @DisplayName("HASH InList")
    class HashInList {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_inlist_hash", info);
        }

        @Test
        @DisplayName("IN clause returns correct results with HASH index")
        void inListReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_hash ON " + fqn
                    + "(session_id) USING HASH");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE session_id IN ('sess-001', 'sess-010', 'sess-100')");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                String sid = r.getRaw("session_id").toString();
                return "sess-001".equals(sid) || "sess-010".equals(sid)
                        || "sess-100".equals(sid);
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for IN clause")
        void inListCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "session_id IN ('sess-001', 'sess-010', 'sess-100')",
                    "CREATE INDEX idx_hash ON " + fqn + "(session_id) USING HASH");
        }
    }

    @Nested
    @DisplayName("BITMAP InList")
    class BitmapInList {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_inlist_bm", info);
        }

        @Test
        @DisplayName("IN clause returns correct results with BITMAP index")
        void inListReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE status IN ('ACTIVE', 'PENDING')");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                String st = r.getRaw("status").toString();
                return "ACTIVE".equals(st) || "PENDING".equals(st);
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for IN clause")
        void inListCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "status IN ('ACTIVE', 'PENDING')",
                    "CREATE INDEX idx_bm ON " + fqn + "(status) USING BITMAP");
        }
    }

    @Nested
    @DisplayName("BTREE InList")
    class BTreeInList {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createGenericTable(sqlRunner, CATALOG, SCHEMA, "t_inlist_bt", info);
        }

        @Test
        @DisplayName("IN clause returns correct results with BTREE index")
        void inListReturnsCorrectResults() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn
                            + " WHERE amount IN (50.0, 150.0, 250.0)");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                double amt = ((Number) r.getRaw("amount")).doubleValue();
                return amt == 50.0 || amt == 150.0 || amt == 250.0;
            });
        }

        @Test
        @DisplayName("Correctness invariant holds for IN clause")
        void inListCorrectnessInvariant() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "amount IN (50.0, 150.0, 250.0)",
                    "CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE");
        }
    }
}
