package org.pangrova.testing.integration.e2e.connector;

import java.time.Duration;
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

/** Actual SQL/JNI refs reads compared with the committed Iceberg SDK references. */
class IcebergRefsMetadataTest extends SqlE2ETestBase {
    private static final String CATALOG = "refs_metadata";
    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeEach
    void createTable() {
        tableName = "refs_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT)");
    }

    @AfterEach
    void drainAndDropTable() {
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(queryManager.getActiveQueryIds()).isEmpty();
                assertThat(queryManager.getActiveQueryCount()).isZero();
            });
        } finally {
            if (tableName != null) sqlRunner.executeDdl("DROP TABLE IF EXISTS " + table());
        }
    }

    private String table() { return CATALOG + ".default." + tableName; }

    private Table sdkTable() {
        return new HadoopTables(new Configuration()).load(
                icebergWarehouse().resolve("default").resolve(tableName).toString());
    }

    @Test
    void emptyTableReturnsNoInventedMainReference() {
        assertThat(sqlRunner.execute("SELECT * FROM " + table() + "$refs")).isEmpty();
        assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + table() + "$refs")
                .get(0).getLong("n")).isZero();
    }

    @Test
    void fullProjectionMatchesSdkRetentionAndNullableDefaults() {
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1)");
        Table sdk = sdkTable();
        long snapshot = sdk.currentSnapshot().snapshotId();
        sdk.manageSnapshots().createBranch("audit", snapshot).createTag("release", snapshot)
                .setMaxRefAgeMs("audit", 9000000000L).setMinSnapshotsToKeep("audit", 3)
                .setMaxSnapshotAgeMs("audit", 7000L).setMaxRefAgeMs("release", 8000L).commit();
        sdk.refresh();
        var rows = sqlRunner.execute("SELECT * FROM " + table() + "$refs ORDER BY name");
        assertThat(rows).hasSize(sdk.refs().size());
        for (var row : rows) {
            assertThat(row.columnNames()).containsExactly("name", "type", "snapshot_id",
                    "max_reference_age_in_ms", "min_snapshots_to_keep", "max_snapshot_age_in_ms");
            var reference = sdk.refs().get(row.getString("name"));
            assertThat(reference).isNotNull();
            assertThat(row.getString("type")).isEqualTo(reference.type().name());
            assertThat(row.getLong("snapshot_id")).isEqualTo(reference.snapshotId());
            assertThat(row.getLong("max_reference_age_in_ms")).isEqualTo(reference.maxRefAgeMs());
            assertThat(row.getInt("min_snapshots_to_keep")).isEqualTo(reference.minSnapshotsToKeep());
            assertThat(row.getLong("max_snapshot_age_in_ms")).isEqualTo(reference.maxSnapshotAgeMs());
        }
        assertThat(rows.stream().filter(row -> row.getString("name").equals("main")))
                .singleElement().satisfies(row -> {
                    assertThat(row.getLong("max_reference_age_in_ms")).isNull();
                    assertThat(row.getInt("min_snapshots_to_keep")).isNull();
                    assertThat(row.getLong("max_snapshot_age_in_ms")).isNull();
                });
    }

    @Test
    void sqlMutationsRefreshRefsWithoutMovingTheTag() {
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (1)");
        long initial = sdkTable().currentSnapshot().snapshotId();
        assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + table() + "$refs")
                .get(0).getLong("n")).isEqualTo(1L);
        sqlRunner.executeDdl("ALTER TABLE " + table() + " CREATE TAG release");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " CREATE BRANCH audit");
        sqlRunner.execute("INSERT INTO " + table() + " VALUES (2)");
        var rows = sqlRunner.execute("SELECT name, snapshot_id FROM " + table()
                + "$refs WHERE type = 'TAG' OR name = 'audit'");
        assertThat(rows).hasSize(2).allSatisfy(row -> assertThat(row.getLong("snapshot_id")).isEqualTo(initial));
        assertThat(sqlRunner.execute("SELECT snapshot_id FROM " + table() + "$refs WHERE name = 'main'")
                .get(0).getLong("snapshot_id")).isNotEqualTo(initial);
        sqlRunner.executeDdl("ALTER TABLE " + table() + " DROP TAG release");
        sqlRunner.executeDdl("ALTER TABLE " + table() + " DROP BRANCH audit");
        assertThat(sqlRunner.execute("SELECT name FROM " + table() + "$refs"))
                .singleElement().satisfies(row -> assertThat(row.getString("name")).isEqualTo("main"));
    }
}
