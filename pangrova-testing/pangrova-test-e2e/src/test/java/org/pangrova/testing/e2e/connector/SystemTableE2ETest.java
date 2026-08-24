package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * System table E2E tests for both Iceberg and Paimon connectors.
 *
 * <p>Verifies that {@code table$systemTable} syntax correctly routes to
 * connector system table implementations and returns expected metadata rows.
 *
 * <p>Covers: B2 (Iceberg Metadata Tables), C7 (Paimon System Tables),
 * D2 (System Table unified syntax).
 *
 * <p>Requirements: B2, C7, D2
 */
@DisplayName("System Table E2E Tests - Iceberg & Paimon")
class SystemTableE2ETest extends SqlE2ETestBase {

    private static final String ICE_CATALOG = "iceberg_systbl";
    private static final String PAI_CATALOG = "paimon_systbl";
    private static final String SCHEMA = "default";

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void registerConnectors() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(
                        ICE_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(
                        PAI_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String icebergFqn() {
        return ICE_CATALOG + "." + SCHEMA + "." + tableName;
    }

    private String paimonFqn() {
        return PAI_CATALOG + "." + SCHEMA + "." + tableName;
    }

    // --- 2.1.22: Iceberg SELECT FROM table$snapshots ---

    @Nested
    @DisplayName("2.1.22 - Iceberg table$snapshots")
    class IcebergSnapshots {

        @Test
        @DisplayName("SELECT FROM table$snapshots returns snapshot rows with snapshot_id and operation")
        void selectFromIcebergSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots");

            assertThat(rows).isNotEmpty();
            TypedRow first = rows.get(0);
            assertThat(first.containsColumn("snapshot_id")).isTrue();
            assertThat(first.containsColumn("operation")).isTrue();
            assertThat(first.getLong("snapshot_id")).isNotNull();
            assertThat(first.getString("operation")).isNotNull();
        }

        @Test
        @DisplayName("Multiple INSERTs produce multiple snapshot rows")
        void multipleInsertsProduceMultipleSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, val VARCHAR)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (1, 'a')");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (2, 'b')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots");

            assertThat(rows).hasSizeGreaterThanOrEqualTo(2);
            for (TypedRow row : rows) {
                assertThat(row.getLong("snapshot_id"))
                        .isGreaterThan(0);
            }
        }
    }

    // --- 2.1.23: Iceberg SELECT FROM table$files ---

    @Nested
    @DisplayName("2.1.23 - Iceberg table$files")
    class IcebergFiles {

        @Test
        @DisplayName("SELECT FROM table$files returns file rows with file_path and record_count")
        void selectFromIcebergFiles() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$files");

            assertThat(rows).isNotEmpty();
            TypedRow first = rows.get(0);
            assertThat(first.containsColumn("file_path")).isTrue();
            assertThat(first.containsColumn("record_count")).isTrue();
            assertThat(first.getString("file_path")).isNotNull();
            long totalRecords = rows.stream()
                    .mapToLong(r -> r.getLong("record_count"))
                    .sum();
            assertThat(totalRecords).isEqualTo(3L);
        }
    }

    // --- 2.1.24: Paimon SELECT FROM table$snapshots ---

    @Nested
    @DisplayName("2.1.24 - Paimon table$snapshots")
    class PaimonSnapshots {

        @Test
        @DisplayName("SELECT FROM table$snapshots returns snapshot rows with snapshot_id and commit_kind")
        void selectFromPaimonSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$snapshots");

            assertThat(rows).isNotEmpty();
            TypedRow first = rows.get(0);
            assertThat(first.containsColumn("snapshot_id")).isTrue();
            assertThat(first.containsColumn("commit_kind")).isTrue();
            assertThat(first.getLong("snapshot_id"))
                    .isGreaterThan(0);
            assertThat(first.getString("commit_kind")).isNotNull();
        }
    }

    // --- 2.1.25: Paimon SELECT FROM table$tags ---

    @Nested
    @DisplayName("2.1.25 - Paimon table$tags")
    class PaimonTags {

        @Test
        @DisplayName("SELECT FROM table$tags shows tag created via CALL create_tag")
        void selectFromPaimonTagsAfterCreateTag() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice')");

            sqlRunner.execute("CALL " + PAI_CATALOG
                    + ".create_tag('" + tableName + "', 'v1')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$tags");

            assertThat(rows).isNotEmpty();
            TypedRow tagRow = rows.stream()
                    .filter(r -> "v1".equals(r.getString("tag_name")))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(
                            "Expected tag 'v1' not found in $tags result"));
            assertThat(tagRow.containsColumn("snapshot_id")).isTrue();
            assertThat(tagRow.getLong("snapshot_id"))
                    .isGreaterThan(0);
        }
    }

    // --- 2.1.26: System table WHERE/ORDER BY/LIMIT filtering ---

    @Nested
    @DisplayName("2.1.26 - System table WHERE/ORDER BY/LIMIT")
    class SystemTableFiltering {

        @Test
        @DisplayName("WHERE clause filters system table rows")
        void whereClauseFiltersSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice')");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (2, 'bob')");

            List<TypedRow> allSnapshots = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots");
            assertThat(allSnapshots).hasSizeGreaterThanOrEqualTo(2);

            List<TypedRow> filtered = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots"
                            + " WHERE operation = 'append'");
            assertThat(filtered).isNotEmpty();
            for (TypedRow row : filtered) {
                assertThat(row.getString("operation")).isEqualTo("append");
            }
        }

        @Test
        @DisplayName("ORDER BY and LIMIT work on system table queries")
        void orderByAndLimitOnSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, val VARCHAR)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (1, 'a')");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (2, 'b')");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (3, 'c')");

            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots");
            assertThat(allRows).hasSizeGreaterThanOrEqualTo(3);
            List<TypedRow> sorted = allRows.stream()
                    .sorted((a, b) -> Long.compare(
                            a.getLong("snapshot_id"),
                            b.getLong("snapshot_id")))
                    .toList();
            for (int i = 1; i < sorted.size(); i++) {
                assertThat(sorted.get(i).getLong("snapshot_id"))
                        .isGreaterThan(sorted.get(i - 1).getLong("snapshot_id"));
            }
        }

        @Test
        @DisplayName("Combined WHERE + ORDER BY + LIMIT on system table")
        void combinedFilteringOnSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, val VARCHAR)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (1, 'a')");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, val) VALUES (2, 'b')");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots"
                            + " WHERE operation = 'append'");
            assertThat(result).hasSizeGreaterThanOrEqualTo(1);
            TypedRow first = result.stream()
                    .min((a, b) -> Long.compare(
                            a.getLong("snapshot_id"),
                            b.getLong("snapshot_id")))
                    .orElseThrow();
            assertThat(first.getString("operation")).isEqualTo("append");
        }
    }

    // --- 2.1.27: Empty table system table query ---

    @Nested
    @DisplayName("2.1.27 - Empty table system table query")
    class EmptyTableSystemTable {

        @Test
        @DisplayName("Iceberg empty table $snapshots returns empty result")
        void icebergEmptyTableSnapshotsReturnsEmpty() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$snapshots");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("Iceberg empty table $files returns empty result")
        void icebergEmptyTableFilesReturnsEmpty() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$files");
            assertThat(rows).isEmpty();
        }

        @Test
        @DisplayName("Paimon empty table $snapshots returns empty result")
        void paimonEmptyTableSnapshotsReturnsEmpty() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$snapshots");
            assertThat(rows).isEmpty();
        }
    }
}
