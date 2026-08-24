package org.pangrova.testing.e2e.index.boundary;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.fixture.IndexTestFixture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests verifying NULL value handling for index types that support
 * equality predicates (BLOOM_FILTER, HASH, BITMAP, BTREE).
 *
 * <p>Each INSERT batch mixes normal rows with NULL rows to verify that:
 * (1) non-NULL equality queries exclude NULL rows, (2) IS NULL queries
 * return only NULL rows, and (3) the correctness invariant holds with
 * NULL-containing data.</p>
 *
 * <p>Validates: Requirements R5.1, R5.2, R5.3</p>
 */
@DisplayName("Index NULL Handling E2E — NULL exclusion and IS NULL queries")
class IndexNullHandlingE2ETest extends SqlE2ETestBase {

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
    // 14.1 + 14.2: NULL exclusion tests per index type
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("BLOOM_FILTER NULL Handling")
    class BloomFilterNullHandling {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createTableWithNulls(sqlRunner, CATALOG, SCHEMA, "t_null_bf", info);
        }

        @Test
        @DisplayName("Non-NULL equality query excludes NULL rows")
        void nonNullEqualityExcludesNulls() {
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE user_id = 1");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> r.getRaw("user_id") != null);
        }

        @Test
        @DisplayName("Correctness invariant holds with NULL-containing data")
        void correctnessWithNulls() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "user_id = 1",
                    "CREATE INDEX idx_bf ON " + fqn + "(user_id) USING BLOOM_FILTER");
        }
    }

    @Nested
    @DisplayName("HASH NULL Handling")
    class HashNullHandling {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createTableWithNulls(sqlRunner, CATALOG, SCHEMA, "t_null_hash", info);
        }

        @Test
        @DisplayName("Non-NULL equality query excludes NULL rows")
        void nonNullEqualityExcludesNulls() {
            sqlRunner.executeDdl("CREATE INDEX idx_hash ON " + fqn
                    + "(session_id) USING HASH");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE session_id = 'sess-001'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> r.getRaw("session_id") != null);
        }
    }

    @Nested
    @DisplayName("BITMAP NULL Handling")
    class BitmapNullHandling {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createTableWithNulls(sqlRunner, CATALOG, SCHEMA, "t_null_bm", info);
        }

        @Test
        @DisplayName("Non-NULL equality query excludes NULL rows")
        void nonNullEqualityExcludesNulls() {
            sqlRunner.executeDdl("CREATE INDEX idx_bm ON " + fqn
                    + "(status) USING BITMAP");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE status = 'ACTIVE'");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> r.getRaw("status") != null);
        }
    }

    @Nested
    @DisplayName("BTREE NULL Handling")
    class BTreeNullHandling {

        private String fqn;

        @BeforeEach
        void createTable(TestInfo info) {
            fqn = IndexTestFixture.createTableWithNulls(sqlRunner, CATALOG, SCHEMA, "t_null_bt", info);
        }

        @Test
        @DisplayName("Non-NULL equality query excludes NULL rows")
        void nonNullEqualityExcludesNulls() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount BETWEEN 100.0 AND 200.0");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> r.getRaw("amount") != null);
        }

        @Test
        @DisplayName("IS NULL query returns only NULL rows")
        void isNullReturnsOnlyNulls() {
            sqlRunner.executeDdl("CREATE INDEX idx_bt ON " + fqn
                    + "(amount) USING BTREE");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn + " WHERE amount IS NULL");
            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> r.getRaw("amount") == null);
        }

        @Test
        @DisplayName("Correctness invariant holds with NULL-containing data")
        void correctnessWithNulls() {
            IndexTestFixture.assertCorrectnessInvariant(sqlRunner, fqn,
                    "amount BETWEEN 100.0 AND 200.0",
                    "CREATE INDEX idx_bt ON " + fqn + "(amount) USING BTREE");
        }
    }
}
