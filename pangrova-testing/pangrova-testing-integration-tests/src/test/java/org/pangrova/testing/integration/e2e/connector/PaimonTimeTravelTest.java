package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Paimon time travel E2E tests.
 *
 * <p>Verifies that Paimon tables support time travel via:
 * <ul>
 *   <li>{@code FOR VERSION AS OF <snapshot-id>} — read at a specific snapshot</li>
 *   <li>{@code VERSION AS OF TAG} — resolve and read a named tag</li>
 *   <li>Clear error when snapshot does not exist</li>
 * </ul>
 *
 * <p>Requirements: C6
 */
@DisplayName("Paimon Time Travel E2E Tests")
class PaimonTimeTravelTest extends SqlE2ETestBase {

    private static final String CATALOG = "paimon_tt";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(
                        CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.paimon(
                        CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    /** Keeps time-travel SQL verification independent of the separate Native $snapshots scan. */
    private long latestSnapshotId() throws Exception {
        try (var catalog = paimonCatalog()) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create(SCHEMA, tableName));
            Long snapshotId = table.snapshotManager().latestSnapshotId();
            assertThat(snapshotId).as("Paimon committed snapshot").isNotNull();
            return snapshotId;
        }
    }

    /** Verifies that the named tag was durably created by SQL before reading at its snapshot. */
    private long taggedSnapshotId(String tagName) throws Exception {
        try (var catalog = paimonCatalog()) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create(SCHEMA, tableName));
            return table.tagManager().tags().entrySet().stream()
                    .filter(entry -> entry.getValue().contains(tagName))
                    .mapToLong(entry -> entry.getKey().id())
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Paimon tag was not committed: " + tagName));
        }
    }

    private org.apache.paimon.catalog.Catalog paimonCatalog() {
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }


    // --- 2.13.3: snapshot-id time travel ---

    @Nested
    @DisplayName("2.13.3 - Snapshot ID Time Travel")
    class SnapshotIdTimeTravel {

        @Test
        @DisplayName("FOR VERSION AS OF returns data at historical snapshot")
        void timeTravelBySnapshotIdReturnsHistoricalData() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            // Batch 1
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            long snapshotAfterBatch1 = latestSnapshotId();

            // Batch 2
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (3, 'carol'), (4, 'dave')");

            // Time travel to snapshot after batch 1
            List<TypedRow> historicalRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " FOR VERSION AS OF " + snapshotAfterBatch1 + " ORDER BY id");

            // Should only see batch 1 data
            assertThat(historicalRows).hasSize(2);
            assertThat(((Number) historicalRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(historicalRows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) historicalRows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(historicalRows.get(1).getRaw("name")).isEqualTo("bob");

            // Current query should see all data
            List<TypedRow> currentRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(currentRows).hasSize(4);
        }
    }

    // --- 2.13.4: tag-name time travel ---

    @Nested
    @DisplayName("2.13.4 - Tag Name Time Travel")
    class TagNameTimeTravel {

        @Test
        void unavailableTagDoesNotReadCurrentSnapshot() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + fqn() + " (id, val) VALUES (1, 'current')");

            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + fqn() + " VERSION AS OF TAG 'missing_tag'");

            assertThat(error.message()).contains("Selected source ref is unavailable: missing_tag");
        }

        @Test
        @DisplayName("Query at tag returns data at tagged snapshot")
        void timeTravelByTagNameReturnsTaggedData() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            // Insert initial data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (1, 'v1'), (2, 'v1')");

            // Create tag at current state
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE TAG release_v1");
            long snapshotAtTagCreation = latestSnapshotId();

            // Insert more data after tag
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (3, 'v2'), (4, 'v2')");

            long tagSnapshotId = taggedSnapshotId("release_v1");
            assertThat(tagSnapshotId).isEqualTo(snapshotAtTagCreation);

            // Time travel to the tagged snapshot
            List<TypedRow> taggedRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " VERSION AS OF TAG 'release_v1' ORDER BY id");

            // Should only see data at tag point (batch 1)
            assertThat(taggedRows).hasSize(2);
            assertThat(((Number) taggedRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(taggedRows.get(0).getRaw("val")).isEqualTo("v1");
            assertThat(((Number) taggedRows.get(1).getRaw("id")).longValue()).isEqualTo(2L);

            // Current query should see all data
            List<TypedRow> currentRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(currentRows).hasSize(4);
        }
    }

    // --- 2.13.5: non-existent snapshot clear error ---

    @Nested
    @DisplayName("2.13.5 - Non-existent Snapshot Error")
    class NonExistentSnapshotError {

        @Test
        @DisplayName("FOR VERSION AS OF with non-existent snapshot-id returns clear error")
        void nonExistentSnapshotIdReturnsError() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice')");

            // Use a snapshot ID that does not exist (very large number)
            long nonExistentSnapshotId = 999999999L;

            QueryError error = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + fqn()
                    + " FOR VERSION AS OF " + nonExistentSnapshotId);

            assertThat(error).isNotNull();
            assertThat(error.message())
                    .contains("Selected snapshot is unavailable: " + nonExistentSnapshotId);
        }
    }
}
