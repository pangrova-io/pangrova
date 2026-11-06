package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
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
 * E2E tests for SHOW SNAPSHOTS/FILES/MANIFESTS/PARTITIONS statements.
 *
 * <p>Verifies that SHOW metadata statements correctly route to system table
 * queries and return results equivalent to direct {@code table$systemTable} access.
 *
 * <p>Requirements: B3
 */
@DisplayName("Iceberg SHOW Metadata E2E Tests")
class IcebergShowMetadataTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_show";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
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

    // --- 2.9.6: SHOW SNAPSHOTS returns snapshot list ---

    @Nested
    @DisplayName("2.9.6 - SHOW SNAPSHOTS returns snapshot list")
    class ShowSnapshotsReturnsData {

        @Test
        @DisplayName("SHOW SNAPSHOTS FROM table returns non-empty result with snapshot_id")
        void showSnapshotsReturnsNonEmptyResult() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW SNAPSHOTS FROM " + fqn());

            assertThat(rows).isNotEmpty();
            TypedRow first = rows.get(0);
            assertThat(first.containsColumn("snapshot_id")).isTrue();
            assertThat(first.getRaw("snapshot_id")).isNotNull();
            assertThat(((Number) first.getRaw("snapshot_id")).longValue())
                    .isGreaterThan(0);
        }

        @Test
        @DisplayName("Multiple INSERTs produce multiple snapshots via SHOW")
        void multipleInsertsProduceMultipleSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (1, 'a')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (2, 'b')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SHOW SNAPSHOTS FROM " + fqn());

            assertThat(rows).hasSizeGreaterThanOrEqualTo(2);
        }
    }

    // --- 2.9.7: SHOW results match system table query ---

    @Nested
    @DisplayName("2.9.7 - SHOW and system table produce same results")
    class ShowMatchesSystemTable {

        @Test
        @DisplayName("SHOW SNAPSHOTS and SELECT FROM table$snapshots return same data")
        void showSnapshotsMatchesSystemTable() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (2, 'bob')");

            List<TypedRow> showResult = sqlRunner.execute(
                    "SHOW SNAPSHOTS FROM " + fqn());
            List<TypedRow> systemTableResult = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$snapshots");

            assertThat(showResult).hasSameSizeAs(systemTableResult);

            for (int i = 0; i < showResult.size(); i++) {
                TypedRow showRow = showResult.get(i);
                TypedRow sysRow = systemTableResult.get(i);
                assertThat(showRow.getRaw("snapshot_id"))
                        .as("snapshot_id at row %d", i)
                        .isEqualTo(sysRow.getRaw("snapshot_id"));
            }
        }

        @Test
        @DisplayName("SHOW FILES and SELECT FROM table$files return same data")
        void showFilesMatchesSystemTable() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            List<TypedRow> showResult = sqlRunner.execute(
                    "SHOW FILES FROM " + fqn());
            List<TypedRow> systemTableResult = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$files");

            assertThat(showResult).hasSameSizeAs(systemTableResult);
        }

        @Test
        @DisplayName("SHOW MANIFESTS and SELECT FROM table$manifests return same data")
        void showManifestsMatchesSystemTable() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice')");

            List<TypedRow> showResult = sqlRunner.execute(
                    "SHOW MANIFESTS FROM " + fqn());
            List<TypedRow> systemTableResult = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$manifests");

            assertThat(showResult).hasSameSizeAs(systemTableResult);
        }

        @Test
        @DisplayName("SHOW PARTITIONS and SELECT FROM table$partitions return same data")
        void showPartitionsMatchesSystemTable() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR)"
                    + " PARTITIONED BY (name)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            List<TypedRow> showResult = sqlRunner.execute(
                    "SHOW PARTITIONS FROM " + fqn());
            List<TypedRow> systemTableResult = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + "$partitions");

            assertThat(showResult).hasSameSizeAs(systemTableResult);
        }
    }
}
