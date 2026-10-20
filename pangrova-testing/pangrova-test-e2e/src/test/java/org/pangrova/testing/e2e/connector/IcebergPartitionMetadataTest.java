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

    private void createAndInsert() {
        sqlRunner.executeDdl("CREATE TABLE " + table()
                + " (id BIGINT, region VARCHAR) PARTITIONED BY (region)");
        sqlRunner.execute("INSERT INTO " + table()
                + " VALUES (1, 'apac'), (2, 'apac'), (3, 'emea'), (4, NULL)");
    }

    @Test
    void groupsRealManifestPartitionStructsIncludingNull() {
        createAndInsert();
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
        createAndInsert();
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
        createAndInsert();
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
}
