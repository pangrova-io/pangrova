package org.pangrova.testing.integration.e2e.connector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual snapshot metadata scans compared with the committed SDK snapshots. */
class IcebergSnapshotsMetadataTest extends SqlE2ETestBase {
    private static final String CATALOG = "snapshots_metadata";
    private String name;

    @Override protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeEach void createTable() {
        name = "snapshots_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT)");
    }

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

    @Test void emptySnapshotsHaveNoRowsAndNoInventedAggregateRecord() {
        assertThat(sqlRunner.execute("SELECT * FROM " + table() + "$snapshots")).isEmpty();
        var result = sqlRunner.execute("SELECT MAX_BY(snapshot_id, committed_at) AS latest,"
                + " COUNT(*) AS n FROM " + table() + "$snapshots").get(0);
        assertThat(result.getRaw("latest")).isNull();
        assertThat(result.getLong("n")).isZero();
    }

    @Test void completeSchemaAndAllValuesMatchTheSdk() {
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1)");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (2)");
        Table sdk = sdkTable();
        var rows = sqlRunner.execute("SELECT * FROM " + table() + "$snapshots ORDER BY committed_at, snapshot_id");
        assertThat(rows).hasSize(2);
        for (var row : rows) {
            assertThat(row.columnNames()).containsExactly("committed_at", "snapshot_id", "parent_id",
                    "operation", "manifest_list", "summary");
            var snapshot = sdk.snapshot(row.getLong("snapshot_id"));
            assertThat(snapshot).isNotNull();
            assertThat(row.getTimestamp("committed_at")).isEqualTo(LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(snapshot.timestampMillis()), ZoneOffset.UTC));
            assertThat(row.getLong("parent_id")).isEqualTo(snapshot.parentId());
            assertThat(row.getString("operation")).isEqualTo(snapshot.operation());
            assertThat(row.getString("manifest_list")).isEqualTo(snapshot.manifestListLocation());
            assertThat(row.getRaw("summary")).isEqualTo(snapshot.summary());
        }
        assertThat(sqlRunner.execute("SHOW SNAPSHOTS FROM " + table()))
                .extracting(row -> row.toRawMap()).containsExactlyInAnyOrderElementsOf(
                        rows.stream().map(row -> row.toRawMap()).toList());
    }

    @Test void timestampSelectionAndSummaryProjectionExecuteInNative() {
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1)");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (2)");
        Table sdk = sdkTable();
        var latest = sqlRunner.execute("SELECT MAX_BY(snapshot_id, committed_at) AS latest,"
                + " MAX(committed_at) AS committed FROM " + table() + "$snapshots").get(0);
        var chosen = sdk.snapshot(latest.getLong("latest"));
        assertThat(latest.getTimestamp("committed")).isEqualTo(LocalDateTime.ofInstant(
                Instant.ofEpochMilli(chosen.timestampMillis()), ZoneOffset.UTC));
        for (var snapshot : sdk.snapshots()) {
            assertThat(snapshot.timestampMillis()).isLessThanOrEqualTo(chosen.timestampMillis());
        }
        var row = sqlRunner.execute("SELECT summary, manifest_list FROM " + table()
                + "$snapshots WHERE snapshot_id = " + chosen.snapshotId()).get(0);
        assertThat(row.getRaw("summary")).isEqualTo(chosen.summary());
        assertThat(row.getString("manifest_list")).isEqualTo(chosen.manifestListLocation());
    }
}
