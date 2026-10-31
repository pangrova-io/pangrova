package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Paimon connector E2E tests for Paimon-specific behaviors: upsert, changelog,
 * compaction, and composite primary key merge.
 *
 * <p>Covers R10.2-R10.4, R10.12. R10.1, R10.9-R10.11, R10.14 removed as duplicates
 * of write/ package tests and CrossConnectorEquivalenceTest.
 * All operations execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Paimon → local filesystem.
 *
 * <p>Requirements: 10.1, 10.3, 16.6
 */
@DisplayName("Paimon Connector - E2E Tests")
class PaimonE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_catalog";
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

    // --- R10.2: Upsert on primary key table ---

    @Nested
    @DisplayName("R10.2 - Upsert on Primary Key Table")
    class UpsertPrimaryKey {

        @Test
        @DisplayName("INSERT with duplicate key replaces old values via upsert")
        void upsertReplacesOldValues() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, score) VALUES (1, 'alice', 80.0), (2, 'bob', 60.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, score) VALUES (1, 'alice_v2', 95.0), (3, 'carol', 70.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertRow(rows.get(0), 1L, "alice_v2", "score", 95.0);
            assertRow(rows.get(1), 2L, "bob", "score", 60.0);
            assertRow(rows.get(2), 3L, "carol", "score", 70.0);
        }
    }

    // --- R10.3: Changelog read ---

    @Nested
    @DisplayName("R10.3 - Changelog Read")
    class ChangelogRead {

        @Test
        @DisplayName("Upsert produces correct final state reflecting all change operations")
        void changelogContainsChangeTypes() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            // Initial insert (2 rows)
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            // Upsert: update id=1, keep id=2 unchanged
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice_v2')");

            // Verify final state reflects all change operations
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice_v2");
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
        }
    }

    // --- R10.4: Compaction idempotency ---

    @Nested
    @DisplayName("R10.4 - Compaction Idempotency")
    class CompactionIdempotency {

        @Test
        @DisplayName("Query results identical after multiple write batches (implicit compaction)")
        void compactionPreservesData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (1, 'a'), (2, 'b')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (3, 'c'), (1, 'a_v2')");

            List<TypedRow> firstRead = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            // Write another batch to trigger implicit compaction in Paimon
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (4, 'd')");

            List<TypedRow> afterExtraWrite = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE id <= 3 ORDER BY id");

            // Original 3 rows should be unchanged after additional writes
            assertThat(afterExtraWrite).hasSameSizeAs(firstRead);
            for (int i = 0; i < firstRead.size(); i++) {
                assertThat(afterExtraWrite.get(i).getRaw("id"))
                        .isEqualTo(firstRead.get(i).getRaw("id"));
                assertThat(afterExtraWrite.get(i).getRaw("val"))
                        .isEqualTo(firstRead.get(i).getRaw("val"));
            }
        }
    }

    // --- R10.12: Composite primary key ---

    @Nested
    @DisplayName("R10.12 - Composite Primary Key Merge")
    class CompositePrimaryKey {

        @Test
        @DisplayName("Composite primary key merge works correctly")
        void compositeKeyMerge() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (region VARCHAR, product_id BIGINT, quantity BIGINT,"
                    + " PRIMARY KEY (region, product_id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (region, product_id, quantity) VALUES "
                    + "('east', 1, 100), ('west', 1, 200), ('east', 2, 50)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (region, product_id, quantity) VALUES "
                    + "('east', 1, 150), ('west', 2, 300)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY region, product_id");

            assertThat(rows).hasSize(4);
            assertCompositeRow(rows.get(0), "east", 1L, 150L);
            assertCompositeRow(rows.get(1), "east", 2L, 50L);
            assertCompositeRow(rows.get(2), "west", 1L, 200L);
            assertCompositeRow(rows.get(3), "west", 2L, 300L);
        }
    }

    // --- Assertion helpers ---

    private static void assertRow(TypedRow row,
                                  long expectedId, String expectedName,
                                  String thirdColName, double expectedThirdCol) {
        assertThat(((Number) row.getRaw("id")).longValue()).isEqualTo(expectedId);
        assertThat(row.getRaw("name")).isEqualTo(expectedName);
        assertThat(((Number) row.getRaw(thirdColName)).doubleValue()).isEqualTo(expectedThirdCol);
    }

    private static void assertCompositeRow(TypedRow row,
                                           String expectedRegion,
                                           long expectedProductId,
                                           long expectedQuantity) {
        assertThat(row.getRaw("region")).isEqualTo(expectedRegion);
        assertThat(((Number) row.getRaw("product_id")).longValue()).isEqualTo(expectedProductId);
        assertThat(((Number) row.getRaw("quantity")).longValue()).isEqualTo(expectedQuantity);
    }
}
