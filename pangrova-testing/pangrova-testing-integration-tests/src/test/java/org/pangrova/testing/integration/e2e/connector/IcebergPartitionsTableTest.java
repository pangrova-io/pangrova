package org.pangrova.testing.integration.e2e.connector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real manifest sources and Native relational partition statistics checked against the pinned SDK. */
class IcebergPartitionsTableTest extends SqlE2ETestBase {
    private static final String CATALOG = "partitions_table";
    private String name;

    @Override protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeEach void uniqueName() { name = "pt_" + UUID.randomUUID().toString().replace("-", ""); }

    @AfterEach void drainAndDrop() {
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(queryManager.getActiveQueryIds()).isEmpty();
                assertThat(queryManager.getActiveQueryCount()).isZero();
            });
        } finally {
            if (name != null) sqlRunner.executeDdl("DROP TABLE IF EXISTS " + table());
        }
    }

    private String table() { return CATALOG + ".default." + name; }

    private Table sdkTable() {
        return new HadoopTables(new Configuration()).load(
                icebergWarehouse().resolve("default").resolve(name).toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyAndNonemptyStatisticsAndShowMatchSdk(boolean partitioned) throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT, region VARCHAR)"
                + (partitioned ? " PARTITIONED BY (region)" : ""));
        assertMatchesSdk();
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1, 'apac'), (2, NULL), (3, 'apac')");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (4, 'emea'), (5, 'apac')");
        assertMatchesSdk();
        var direct = sqlRunner.execute("SELECT * FROM " + table() + "$partitions");
        assertThat(sqlRunner.execute("SHOW PARTITIONS FROM " + table()))
                .extracting(row -> row.toRawMap()).containsExactlyInAnyOrderElementsOf(
                        direct.stream().map(row -> row.toRawMap()).toList());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void liveDataAndDeleteCountersIncludingDeleteOnlyPartitionsMatchSdk(int version) throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)"
                + " TBLPROPERTIES ('format-version' = '" + version + "')");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1, 'apac'), (2, 'emea')");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (3, 'apac')");
        Table sdk = sdkTable();
        var position = org.apache.iceberg.FileMetadata.deleteFileBuilder(sdk.spec()).ofPositionDeletes()
                .withFileSizeInBytes(33).withRecordCount(1).withPartitionPath("region=apac");
        if (version == 3) {
            try (var tasks = sdk.newScan().filter(org.apache.iceberg.expressions.Expressions.equal("region", "apac"))
                    .planFiles()) {
                position.withFormat(org.apache.iceberg.FileFormat.PUFFIN)
                        .withPath(sdk.location() + "/data/position.puffin")
                        .withReferencedDataFile(tasks.iterator().next().file().location())
                        .withContentOffset(4).withContentSizeInBytes(16);
            }
        } else {
            position.withPath(sdk.location() + "/data/position.parquet");
        }
        // These are metadata scans: the SDK writes actual manifests, without reading delete payloads.
        var equality = org.apache.iceberg.FileMetadata.deleteFileBuilder(sdk.spec()).ofEqualityDeletes(1)
                .withPath(sdk.location() + "/data/equality.parquet").withFileSizeInBytes(44)
                .withRecordCount(2).withPartitionPath("region=delete_only").build();
        sdk.newRowDelta().addDeletes(position.build()).addDeletes(equality).commit();
        assertMatchesSdk();
        var deletedOnly = sqlRunner.execute("SELECT * FROM " + table()
                + "$partitions p WHERE p.\"partition\".region = 'delete_only'");
        assertThat(deletedOnly).singleElement().satisfies(row -> {
            assertThat(row.getLong("record_count")).isZero();
            assertThat(row.getInt("file_count")).isZero();
            assertThat(row.getLong("equality_delete_record_count")).isEqualTo(2);
        });
        org.apache.iceberg.DataFile removed;
        try (var tasks = sdk.newScan().filter(org.apache.iceberg.expressions.Expressions.equal("region", "emea"))
                .planFiles()) {
            removed = tasks.iterator().next().file().copy();
        }
        sdk.newDelete().deleteFile(removed).commit();
        assertMatchesSdk();
        assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + table()
                + "$partitions p WHERE p.\"partition\".region = 'emea'").get(0).getLong("n")).isZero();
    }

    @Test void missingDimensionsAndExplicitNullValuesRetainDistinctSdkGroups() throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT, region VARCHAR)");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1, NULL)");
        Table sdk = sdkTable();
        sdk.updateSpec().addField("region").commit();
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (2, NULL)");
        sdk.refresh();
        sdk.updateSpec().addField("id_bucket", org.apache.iceberg.expressions.Expressions.bucket("id", 16)).commit();
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (NULL, NULL)");
        sdk.refresh();
        sdk.updateSpec().renameField("region", "market").commit();
        assertMatchesSdk();
        var rows = sqlRunner.execute("SELECT * FROM " + table() + "$partitions");
        assertThat(rows).hasSize(3);
        assertThat(rows).allSatisfy(row -> {
            var partition = (Map<?, ?>) row.getRaw("partition");
            assertThat(partition.get("market")).isNull();
            assertThat(partition.get("id_bucket")).isNull();
            assertThat(row.getLong("record_count")).isEqualTo(1);
        });
    }

    @Test void expiredEntrySnapshotDoesNotInventLastUpdateOrSpecId() throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT, region VARCHAR)");
        var sdk = sdkTable();
        sdk.updateSpec().addField("region").commit();
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1, 'older')");
        sdk.refresh();
        long older = sdk.currentSnapshot().snapshotId();
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (2, 'newer')");
        sdk.refresh();
        sdk.expireSnapshots().expireSnapshotId(older).cleanExpiredFiles(false).commit();
        assertMatchesSdk();
        var row = sqlRunner.execute("SELECT * FROM " + table()
                + "$partitions p WHERE p.\"partition\".region = 'older'").get(0);
        assertThat(row.getInt("spec_id")).isZero();
        assertThat(row.getRaw("last_updated_at")).isNull();
        assertThat(row.getRaw("last_updated_snapshot_id")).isNull();
        assertThat(row.getLong("record_count")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FLOAT", "DOUBLE"})
    void signedZeroNanInfinitiesSubnormalsAndNullMatchSdkGrouping(String type) throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT, p " + type + ") PARTITIONED BY (p)");
        var sdk = sdkTable();
        var append = sdk.newFastAppend();
        // Metadata-only fixture: actual SDK manifest tuples retain the raw floating bits.
        var values = type.equals("FLOAT")
                ? new Object[] {0.0f, -0.0f, Float.intBitsToFloat(0x7fc00001), Float.intBitsToFloat(0xffc01234),
                        Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, -Float.MIN_VALUE, null}
                : new Object[] {0.0d, -0.0d, Double.longBitsToDouble(0x7ff8000000000001L),
                        Double.longBitsToDouble(0xfff8000000001234L), Double.POSITIVE_INFINITY,
                        Double.NEGATIVE_INFINITY, Double.MIN_VALUE, -Double.MIN_VALUE, null};
        for (int i = 0; i < values.length; i++) {
            var partition = new org.apache.iceberg.PartitionData(sdk.spec().partitionType());
            partition.set(0, values[i]);
            append.appendFile(org.apache.iceberg.DataFiles.builder(sdk.spec()).withPartition(partition)
                    .withPath(sdk.location() + "/data/floating-" + i + ".parquet")
                    .withRecordCount(i + 1).withFileSizeInBytes(100 + i).build());
        }
        append.commit();
        assertMatchesSdk();
        assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + table() + "$partitions")
                .get(0).getLong("n")).isEqualTo(8);
    }

    @Test void namedFieldsAndSimpleCaseRetainValuesThroughSqlAndNative() {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1), (NULL)");
        var rows = sqlRunner.execute("SELECT ROW(\"Spec ID\" => CASE id WHEN 1 THEN 7 ELSE 9 END,"
                + " \"snap\"\"id\" => id) AS value FROM " + table());
        assertThat(rows).hasSize(2);
        for (var row : rows) {
            var value = (Map<?, ?>) row.getRaw("value");
            assertThat(new ArrayList<Object>(value.keySet())).containsExactly("Spec ID", "snap\"id");
            assertThat(value.get("Spec ID")).isEqualTo(value.get("snap\"id") == null ? 9 : 7);
        }
    }

    private void assertMatchesSdk() throws Exception {
        var sdk = MetadataTableUtils.createMetadataTableInstance(sdkTable(), MetadataTableType.PARTITIONS);
        var expected = new ArrayList<Object>();
        if (sdkTable().currentSnapshot() != null) {
            try (var tasks = sdk.newScan().planFiles()) {
                for (var task : tasks) {
                    try (var rows = ((DataTask) task).rows()) {
                        for (var row : rows) expected.add(sdkValue(sdk.schema().asStruct(), row));
                    }
                }
            }
        }
        assertThat(sqlRunner.execute("SELECT * FROM " + table() + "$partitions"))
                .extracting(row -> (Object) row.toRawMap()).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static Object sdkValue(Type type, Object value) {
        if (value == null) return null;
        return switch (type.typeId()) {
            case STRUCT -> {
                var values = new LinkedHashMap<String, Object>();
                var fields = type.asStructType().fields();
                for (int i = 0; i < fields.size(); i++) {
                    var field = fields.get(i);
                    values.put(field.name(), sdkValue(field.type(), ((StructLike) value).get(i, Object.class)));
                }
                yield values;
            }
            case TIMESTAMP -> LocalDateTime.ofInstant(Instant.ofEpochSecond(
                    Math.floorDiv((long) value, 1_000_000), Math.floorMod((long) value, 1_000_000) * 1_000),
                    ZoneOffset.UTC);
            case STRING -> value.toString();
            default -> value;
        };
    }
}
