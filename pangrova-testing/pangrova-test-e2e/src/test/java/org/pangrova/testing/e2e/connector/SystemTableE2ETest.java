package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * System table E2E tests for both Iceberg and Paimon connectors.
 *
 * <p>Verifies that {@code table$systemTable} syntax routes through the
 * connector split and Native scan path and returns expected metadata rows.
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
    @DisplayName("Iceberg table$manifests (pinned Native Avro list)")
    class IcebergManifests {
        @Test
        void emptyAndCommittedManifestListHaveOfficialTypedColumns() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, region VARCHAR)");
            assertThat(sqlRunner.execute("SELECT * FROM " + icebergFqn() + "$manifests"))
                    .isEmpty();

            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, region) VALUES (1, 'east'), (2, 'west')");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn() + "$manifests");
            assertThat(rows).isNotEmpty();
            for (TypedRow row : rows) {
                assertThat(row.columnNames()).containsExactly(
                        "content", "path", "length", "partition_spec_id",
                        "added_snapshot_id", "added_data_files_count",
                        "existing_data_files_count", "deleted_data_files_count",
                        "added_delete_files_count", "existing_delete_files_count",
                        "deleted_delete_files_count", "partition_summaries");
                assertThat(row.getInt("content")).isZero();
                assertThat(row.getString("path")).isNotBlank();
                assertThat(row.getLong("length")).isPositive();
                assertThat(row.getLong("added_snapshot_id")).isPositive();
                assertThat(row.getRaw("partition_summaries")).isEqualTo(List.of());
            }
        }

        @Test
        void partitionBoundsRemainNestedTypedValuesThroughSql() {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)");
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, region) VALUES (1, 'east'), (2, 'west')");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT partition_summaries FROM " + icebergFqn() + "$manifests");
            assertThat(rows).isNotEmpty();
            java.util.Set<String> observedBounds = new java.util.HashSet<>();
            assertThat(rows).allSatisfy(row -> {
                assertThat(row.getRaw("partition_summaries")).isInstanceOf(List.class);
                List<?> summaries = (List<?>) row.getRaw("partition_summaries");
                assertThat(summaries).hasSize(1);
                assertThat(summaries.get(0)).isInstanceOf(java.util.Map.class);
                java.util.Map<?, ?> summary = (java.util.Map<?, ?>) summaries.get(0);
                assertThat(summary.keySet().stream().map(Object::toString).toList())
                        .containsExactlyInAnyOrder(
                        "contains_null", "contains_nan", "lower_bound", "upper_bound");
                assertThat(summary.get("contains_null")).isEqualTo(false);
                assertThat(summary.get("lower_bound")).isInstanceOf(String.class);
                assertThat(summary.get("upper_bound")).isInstanceOf(String.class);
                observedBounds.add((String) summary.get("lower_bound"));
                observedBounds.add((String) summary.get("upper_bound"));
            });
            assertThat(observedBounds).contains("east", "west");
        }
    }

    @Nested
    @DisplayName("2.1.23 - Iceberg table$files")
    class IcebergFiles {

        @Test
        @DisplayName("SELECT FROM table$files returns file rows with file_path and record_count")
        void selectFromIcebergFiles() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + icebergFqn()
                    + " (id BIGINT, name VARCHAR)");
            assertThat(sqlRunner.execute("SELECT file_path, record_count FROM "
                    + icebergFqn() + "$files")).isEmpty();
            assertThatThrownBy(() -> sqlRunner.execute("SELECT column_sizes FROM "
                    + icebergFqn() + "$files"))
                    .hasMessageContaining("PLANNER-0001")
                    .hasMessageContaining("column_sizes");
            assertThat(sqlRunner.execute("SELECT * FROM " + icebergFqn() + "$files"))
                    .isEmpty();
            sqlRunner.execute("INSERT INTO " + icebergFqn()
                    + " (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");

            String deletePath;
            Map<String, ContentFile<?>> sdkFiles = new LinkedHashMap<>();
            try (var catalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString())) {
                var table = catalog.loadTable(TableIdentifier.of(SCHEMA, tableName));
                deletePath = table.location() + "/data/position-delete.parquet";
                var delete = FileMetadata.deleteFileBuilder(table.spec())
                        .ofPositionDeletes()
                        .withPath(deletePath)
                        .withFileSizeInBytes(128L)
                        .withRecordCount(5)
                        .withFormat(FileFormat.PARQUET)
                        .build();
                table.newRowDelta().addDeletes(delete).commit();
                table.refresh();
                assertThat(table.currentSnapshot().allManifests(table.io()))
                        .extracting(manifest -> manifest.content())
                        .contains(ManifestContent.DATA, ManifestContent.DELETES);
                assertThat(MetadataTableUtils.createMetadataTableInstance(table,
                        MetadataTableType.FILES).schema().columns())
                        .extracting(field -> field.name())
                        .contains("content", "file_path", "file_format", "spec_id",
                                "record_count", "file_size_in_bytes", "readable_metrics");
                for (var manifest : table.currentSnapshot().allManifests(table.io())) {
                    if (manifest.content() == ManifestContent.DATA) {
                        try (var reader = ManifestFiles.read(manifest, table.io(), table.specs())) {
                            for (var file : reader) {
                                assertThat(sdkFiles.put(file.path().toString(), file)).isNull();
                            }
                        }
                    } else {
                        try (var reader = ManifestFiles.readDeleteManifest(
                                manifest, table.io(), table.specs())) {
                            for (var file : reader) {
                                assertThat(sdkFiles.put(file.path().toString(), file)).isNull();
                            }
                        }
                    }
                }
            }

            assertThat(sdkFiles).containsKey(deletePath);
            assertThat(sdkFiles.values()).extracting(file -> file.content().id())
                    .contains(0, 1);

            List<TypedRow> rows = sqlRunner.execute("SELECT content, file_path, file_format,"
                    + " spec_id, record_count, file_size_in_bytes FROM " + icebergFqn()
                    + "$files ORDER BY content");
            assertThat(rows).hasSize(sdkFiles.size());
            assertThat(rows).extracting(row -> row.getInt("content")).contains(0, 1);
            assertThat(sqlRunner.execute("SELECT * FROM " + icebergFqn() + "$files"))
                    .hasSize(sdkFiles.size())
                    .allSatisfy(row -> assertThat(row.columnNames()).containsExactly(
                            "content", "file_path", "file_format", "spec_id",
                            "record_count", "file_size_in_bytes"));
            for (TypedRow row : rows) {
                assertThat(row.columnNames()).containsExactly("content", "file_path",
                        "file_format", "spec_id", "record_count", "file_size_in_bytes");
                var expected = sdkFiles.get(row.getString("file_path"));
                assertThat(expected).isNotNull();
                assertThat(row.getInt("content")).isEqualTo(expected.content().id());
                assertThat(row.getString("file_format")).isEqualTo(expected.format().name());
                assertThat(row.getInt("spec_id")).isEqualTo(expected.specId());
                assertThat(row.getLong("record_count")).isEqualTo(expected.recordCount());
                assertThat(row.getLong("file_size_in_bytes"))
                        .isEqualTo(expected.fileSizeInBytes());
            }
            assertThat(rows.stream().filter(row -> row.getInt("content") == 0)
                    .mapToLong(row -> row.getLong("record_count")).sum()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Paimon table$schemas")
    class PaimonSchemas {
        @Test
        void historicalSchemasAreReadThroughSqlAndNative() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.executeDdl("ALTER TABLE " + paimonFqn() + " ADD COLUMN note VARCHAR");
            sqlRunner.executeDdl("ALTER TABLE " + paimonFqn() + " ADD COLUMN score BIGINT");

            List<TypedRow> rows = sqlRunner.execute("SELECT schema_id, fields, options,"
                    + " update_time FROM " + paimonFqn() + "$schemas"
                    + " WHERE schema_id >= 0 ORDER BY schema_id");
            assertThat(rows).hasSize(3);
            assertThat(rows).extracting(row -> row.getLong("schema_id"))
                    .containsExactly(0L, 1L, 2L);
            assertThat(rows.get(0).getString("fields")).contains("name").doesNotContain("note");
            assertThat(rows.get(1).getString("fields")).contains("note").doesNotContain("score");
            assertThat(rows.get(2).getString("fields")).contains("score");
            assertThat(rows.get(2).getString("options")).startsWith("{");
            assertThat(rows.get(2).getTimestamp("update_time")).isNotNull();
            List<TypedRow> latest = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$schemas WHERE schema_id = 2");
            assertThat(latest).hasSize(1);
            assertThat(latest.get(0).columnNames()).containsExactly(
                    "schema_id", "fields", "partition_keys", "primary_keys", "options",
                    "comment", "update_time");
        }

        @Test
        void emptyDataTableStillExposesInitialSchema() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, PRIMARY KEY (id) NOT ENFORCED)");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT schema_id, primary_keys FROM " + paimonFqn() + "$schemas");
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong("schema_id")).isZero();
            assertThat(rows.get(0).getString("primary_keys")).contains("id");
        }
    }

    @Nested
    @DisplayName("Paimon table$branches (local directory status)")
    class PaimonBranches {
        @Test
        void branchesAreReadThroughSqlAndNativeDirectoryStat() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, PRIMARY KEY (id) NOT ENFORCED)");
            assertThat(sqlRunner.execute("SELECT * FROM " + paimonFqn() + "$branches"))
                    .isEmpty();

            sqlRunner.executeDdl("ALTER TABLE " + paimonFqn() + " CREATE BRANCH dev");
            sqlRunner.executeDdl("ALTER TABLE " + paimonFqn() + " CREATE BRANCH release");
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT branch_name, create_time FROM " + paimonFqn()
                            + "$branches WHERE branch_name <> 'missing' ORDER BY branch_name");
            assertThat(rows).hasSize(2);
            assertThat(rows).extracting(row -> row.getString("branch_name"))
                    .containsExactly("dev", "release");
            LocalDateTime nowLocal = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
            for (TypedRow row : rows) {
                assertThat(row.columnNames()).containsExactly("branch_name", "create_time");
                assertThat(row.getTimestamp("create_time"))
                        .isBetween(nowLocal.minusMinutes(5), nowLocal.plusMinutes(5));
            }
        }
    }

    // --- 2.1.24: Paimon SELECT FROM table$snapshots ---

    @Nested
    @DisplayName("2.1.24 - Paimon table$snapshots")
    class PaimonSnapshots {

        @Test
        @DisplayName("SELECT FROM table$snapshots returns the Paimon 1.4 snapshot schema")
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
            assertThat(first.columnNames()).containsExactly(
                    "snapshot_id", "schema_id", "commit_user", "commit_identifier",
                    "commit_kind", "commit_time", "base_manifest_list", "delta_manifest_list",
                    "changelog_manifest_list", "total_record_count", "delta_record_count",
                    "changelog_record_count", "watermark", "next_row_id");
            assertThat(first.getLong("snapshot_id"))
                    .isGreaterThan(0);
            assertThat(first.getString("commit_user")).isNotBlank();
            assertThat(first.getString("commit_kind")).isNotNull();
            assertThat(first.getString("base_manifest_list")).isNotBlank();
            assertThat(first.getString("delta_manifest_list")).isNotBlank();
            LocalDateTime nowUtc = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
            assertThat(first.getTimestamp("commit_time"))
                    .isBetween(nowUtc.minusMinutes(5), nowUtc.plusMinutes(5));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(queryManager.getActiveQueryIds())
                            .as("Paimon metadata query and its writes physically drain")
                            .isEmpty());
        }

        @Test
        @DisplayName("Paimon snapshot metadata obeys SQL filtering, ordering and limit")
        void filterOrderAndLimitPaimonSnapshots() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn() + " VALUES (1, 'alice')");
            sqlRunner.execute("INSERT INTO " + paimonFqn() + " VALUES (2, 'bob')");

            List<TypedRow> all = sqlRunner.execute(
                    "SELECT snapshot_id FROM " + paimonFqn() + "$snapshots");
            assertThat(all).hasSizeGreaterThanOrEqualTo(2);
            long latest = all.stream().mapToLong(row -> row.getLong("snapshot_id"))
                    .max().orElseThrow();

            List<TypedRow> selected = sqlRunner.execute(
                    "SELECT snapshot_id, commit_time FROM " + paimonFqn()
                            + "$snapshots WHERE snapshot_id > 0"
                            + " ORDER BY snapshot_id DESC LIMIT 1");
            assertThat(selected).hasSize(1);
            assertThat(selected.get(0).getLong("snapshot_id")).isEqualTo(latest);
            assertThat(selected.get(0).getTimestamp("commit_time")).isNotNull();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(queryManager.getActiveQueryIds()).isEmpty());
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
            assertThat(tagRow.columnNames()).containsExactly(
                    "tag_name", "snapshot_id", "schema_id", "commit_time",
                    "record_count", "create_time", "time_retained");
            assertThat(tagRow.getLong("snapshot_id"))
                    .isGreaterThan(0);
            assertThat(tagRow.getTimestamp("commit_time")).isNotNull();
            assertThat(tagRow.getLong("record_count")).isEqualTo(1);
            assertThat(tagRow.getTimestamp("create_time")).isNull();
            assertThat(tagRow.getString("time_retained")).isNull();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(queryManager.getActiveQueryIds()).isEmpty());
        }

        @Test
        void emptyPaimonTagsReturnsTypedEmptyRows() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");

            assertThat(sqlRunner.execute("SELECT * FROM " + paimonFqn() + "$tags"))
                    .isEmpty();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(queryManager.getActiveQueryIds()).isEmpty());
        }

        @Test
        void ttlTagCreatedBySqlExposesSdkRetentionAndCreationTime() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR,"
                    + " PRIMARY KEY (id) NOT ENFORCED)");
            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name) VALUES (1, 'alice')");
            long snapshotId = sqlRunner.execute("SELECT snapshot_id FROM "
                    + paimonFqn() + "$snapshots ORDER BY snapshot_id DESC LIMIT 1")
                    .get(0).getLong("snapshot_id");

            sqlRunner.execute("CALL " + PAI_CATALOG + ".create_tag('"
                    + tableName + "', 'retained', " + snapshotId + ", '2 d')");

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM "
                    + paimonFqn() + "$tags WHERE tag_name = 'retained'");
            assertThat(rows).hasSize(1);
            TypedRow tag = rows.get(0);
            assertThat(tag.getLong("snapshot_id")).isEqualTo(snapshotId);
            assertThat(tag.getTimestamp("create_time")).isNotNull();
            assertThat(tag.getString("time_retained")).isEqualTo("PT48H");
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(queryManager.getActiveQueryIds()).isEmpty());
        }
    }

    @Nested
    @DisplayName("Paimon table$options")
    class PaimonOptions {

        @Test
        void persistedOptionsAreReadThroughNativeSql() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ('bucket' = '1')");

            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + paimonFqn() + "$options");
            assertThat(rows).isNotEmpty();
            assertThat(rows.get(0).columnNames()).containsExactly("key", "value");
            assertThat(rows).anySatisfy(row -> {
                assertThat(row.getString("key")).isEqualTo("bucket");
                assertThat(row.getString("value")).isEqualTo("1");
            });
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(queryManager.getActiveQueryIds()).isEmpty());
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

            List<TypedRow> sorted = sqlRunner.execute(
                    "SELECT * FROM " + icebergFqn()
                            + "$snapshots ORDER BY snapshot_id ASC LIMIT 2");
            assertThat(sorted).hasSize(2);
            assertThat(sorted.get(1).getLong("snapshot_id"))
                    .isGreaterThan(sorted.get(0).getLong("snapshot_id"));
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
                            + " WHERE operation = 'append'"
                            + " ORDER BY snapshot_id DESC LIMIT 1");
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getString("operation")).isEqualTo("append");
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
                    "SELECT file_path, record_count FROM " + icebergFqn() + "$files");
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
