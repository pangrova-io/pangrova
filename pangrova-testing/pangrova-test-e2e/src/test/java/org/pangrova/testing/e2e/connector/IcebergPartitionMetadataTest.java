package org.pangrova.testing.e2e.connector;

import java.util.List;
import java.util.UUID;
import java.time.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real SQL/JNI manifest partition projection, including spec evolution. */
class IcebergPartitionMetadataTest extends SqlE2ETestBase {
    private static final String CATALOG = "partition_metadata";
    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeEach
    void uniqueTable() {
        tableName = "partition_" + UUID.randomUUID().toString().replace("-", "");
    }

    private String table() { return CATALOG + ".default." + tableName; }

    @AfterEach
    void physicalQueryOwnershipDrains() {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(queryManager.getActiveQueryIds()).isEmpty();
            assertThat(queryManager.getActiveQueryCount()).isZero();
        });
    }

    private void createAndInsert(int version) {
        sqlRunner.executeDdl("CREATE TABLE " + table()
                + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)"
                + " TBLPROPERTIES ('format-version' = '" + version + "')");
        sqlRunner.execute("INSERT INTO " + table()
                + " VALUES (1, 'apac'), (2, 'apac'), (3, 'emea'), (4, NULL)");
    }

    @Test
    void groupsRealManifestPartitionStructsIncludingNull() {
        createAndInsert(2);
        var rows = sqlRunner.execute("SELECT f.\"partition\".region AS region,"
                + " SUM(record_count) AS records FROM " + table() + "$files f"
                + " GROUP BY f.\"partition\".region");
        assertThat(rows).hasSize(3);
        var totals = new java.util.HashMap<String, Long>();
        rows.forEach(row -> totals.put(row.getString("region"), row.getLong("records")));
        assertThat(totals).containsEntry("apac", 2L).containsEntry("emea", 1L).containsEntry(null, 1L);
    }

    @Test
    void fullFileProjectionRetainsStructIdentityAndNestedNullsThroughFlight() {
        createAndInsert(2);
        var rows = sqlRunner.execute("SELECT * FROM " + table() + "$files");
        assertThat(rows).hasSize(3);
        var regions = new java.util.HashSet<Object>();
        for (var row : rows) {
            assertThat(row.columnNames()).containsExactly(
                    "content", "file_path", "file_format", "spec_id", "partition",
                    "record_count", "file_size_in_bytes", "column_sizes", "split_offsets",
                    "equality_ids", "sort_order_id");
            assertThat(row.getRaw("partition")).isInstanceOf(java.util.Map.class);
            var partition = (java.util.Map<?, ?>) row.getRaw("partition");
            assertThat(partition.size()).isEqualTo(1);
            assertThat(partition.containsKey("region")).isTrue();
            regions.add(partition.get("region"));
        }
        assertThat(regions).containsExactlyInAnyOrder("apac", "emea", null);

    }

    @Test
    void renamedFieldRetainsValueAndOldSpecHasNullForNewDimension() {
        createAndInsert(2);
        var iceberg = new HadoopTables(new Configuration()).load(
                icebergWarehouse().resolve("default").resolve(tableName).toString());
        iceberg.updateSpec().renameField("region", "market")
                .addField("id_bucket", Expressions.bucket("id", 16)).commit();
        var rows = sqlRunner.execute("SELECT f.\"partition\".market AS market,"
                + " f.\"partition\".id_bucket AS id_bucket, record_count FROM " + table() + "$files f");
        assertThat(rows).hasSize(3);
        assertThat(rows).allSatisfy(row -> assertThat(row.getInt("id_bucket")).isNull());
        assertThat(rows.stream().map(row -> row.getString("market")))
                .containsExactlyInAnyOrder("apac", "emea", null);
        assertThat(rows.stream().mapToLong(row -> row.getLong("record_count")).sum()).isEqualTo(4);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void nestedEntriesMatchSdkDataAndDeleteManifestProjections(int version) throws Exception {
        createAndInsert(version);
        var iceberg = new HadoopTables(new Configuration()).load(
                icebergWarehouse().resolve("default").resolve(tableName).toString());
        // The feature under test reads real SDK-written manifests; delete payload files are not scanned.
        var position = org.apache.iceberg.FileMetadata.deleteFileBuilder(iceberg.spec()).ofPositionDeletes()
                .withFileSizeInBytes(33).withRecordCount(1).withPartitionPath("region=apac");
        if (version == 3) {
            try (var tasks = iceberg.newScan().filter(Expressions.equal("region", "apac")).planFiles()) {
                position.withFormat(org.apache.iceberg.FileFormat.PUFFIN)
                        .withPath(iceberg.location() + "/data/position-delete.puffin")
                        .withReferencedDataFile(tasks.iterator().next().file().location())
                        .withContentOffset(4).withContentSizeInBytes(16);
            }
        } else {
            position.withPath(iceberg.location() + "/data/position-delete.parquet");
        }
        var equality = org.apache.iceberg.FileMetadata.deleteFileBuilder(iceberg.spec()).ofEqualityDeletes(1)
                .withPath(iceberg.location() + "/data/equality-delete.parquet").withFileSizeInBytes(44)
                .withRecordCount(2).withPartitionPath("region=emea").build();
        iceberg.newRowDelta().addDeletes(position.build()).addDeletes(equality).commit();
        var sdk = org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance(iceberg,
                org.apache.iceberg.MetadataTableType.ENTRIES);
        var columns = new java.util.ArrayList<>(List.of(
                "status", "snapshot_id", "sequence_number", "file_sequence_number"));
        for (String name : List.of("content", "file_path", "file_format", "spec_id", "partition", "record_count",
                "file_size_in_bytes", "column_sizes", "split_offsets", "equality_ids", "sort_order_id", "first_row_id")) {
            columns.add("data_file." + name);
        }
        var schema = sdk.schema().select(columns);
        var expected = new java.util.HashMap<String, Object>();
        try (var tasks = sdk.newScan().select(columns).planFiles()) {
            for (var task : tasks) {
                try (var values = ((org.apache.iceberg.DataTask) task).rows()) {
                    for (var value : values) {
                        var row = (java.util.Map<?, ?>) sdkValue(schema.asStruct(), value);
                        var file = (java.util.Map<?, ?>) row.get("data_file");
                        expected.put(file.get("file_path").toString(), row);
                    }
                }
            }
        }
        var actual = sqlRunner.execute("SELECT * FROM " + table() + "$entries");
        assertThat(actual).hasSize(5);
        var contents = new java.util.HashSet<Object>();
        for (var row : actual) {
            var file = (java.util.Map<?, ?>) row.getRaw("data_file");
            contents.add(file.get("content"));
            var fields = new java.util.LinkedHashMap<String, Object>();
            row.columnNames().forEach(name -> fields.put(name, row.getRaw(name)));
            assertThat(fields).isEqualTo(expected.remove(file.get("file_path").toString()));
        }
        assertThat(contents).containsExactlyInAnyOrder(0, 1, 2);
        assertThat(expected).isEmpty();
    }

    private static Object sdkValue(org.apache.iceberg.types.Type type, Object value) {
        if (value == null) return null;
        return switch (type.typeId()) {
            case STRUCT -> {
                var fields = type.asStructType().fields();
                var struct = (org.apache.iceberg.StructLike) value;
                var values = new java.util.LinkedHashMap<String, Object>();
                for (int i = 0; i < fields.size(); i++) {
                    var field = fields.get(i);
                    values.put(field.name(), sdkValue(field.type(), struct.get(i, Object.class)));
                }
                yield values;
            }
            case LIST -> ((java.util.List<?>) value).stream()
                    .map(item -> sdkValue(type.asListType().elementType(), item)).toList();
            case MAP -> {
                var values = new java.util.LinkedHashMap<Object, Object>();
                ((java.util.Map<?, ?>) value).forEach((key, item) -> values.put(
                        sdkValue(type.asMapType().keyType(), key), sdkValue(type.asMapType().valueType(), item)));
                yield values;
            }
            case STRING -> value.toString();
            default -> value;
        };
    }
}
