package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Paimon cross-partition update E2E tests.
 *
 * <p>Validates that when a PK table is partitioned by a non-PK column,
 * cross-partition row moves (DELETE + INSERT) correctly migrate rows
 * across partitions (with cross-partition-upsert=true) and that bare
 * inserts without the option produce duplicates.
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native →
 * transport → Client.
 *
 * <p>Covers: cross-partition upsert for Paimon PK tables
 */
@DisplayName("Paimon Cross-Partition Update - E2E Tests")
class PaimonCrossPartitionUpdateTest extends SqlE2ETestBase {

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

    @Nested
    @DisplayName("Cross-Partition Upsert Correctness")
    class CrossPartitionUpsert {

        @Test
        @DisplayName("Upsert that changes partition key migrates row to new partition")
        void upsertMigratesRowToNewPartition() {
            // PK = id, partition key = region (PK != partition key).
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, val DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " PARTITIONED BY (region)"
                    + " TBLPROPERTIES ("
                    + "   'cross-partition-upsert' = 'true',"
                    + "   'changelog-producer' = 'lookup',"
                    + "   'file.format' = 'parquet'"
                    + " )");

            // Insert initial rows: id=1 in 'east', id=2 in 'west'
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, val) VALUES (1, 'east', 10.0), (2, 'west', 20.0)");

            // Cross-partition move: delete id=1 from 'east', re-insert into 'west'.
            // Batch writes do not support automatic cross-partition dedup, so
            // an explicit DELETE + INSERT is required.
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 1");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, val) VALUES (1, 'west', 15.0)");

            // Verify: id=1 should now be in 'west' with updated val
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);

            // id=1 migrated to 'west' with new val
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("region")).isEqualTo("west");
            assertThat(rows.get(0).getDouble("val")).isEqualTo(15.0);

            // id=2 unchanged in 'west'
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(1).getString("region")).isEqualTo("west");
            assertThat(rows.get(1).getDouble("val")).isEqualTo(20.0);
        }
    }

    @Nested
    @DisplayName("Cross-Partition Upsert Not Enabled")
    class CrossPartitionUpdateDisabled {

        @Test
        @DisplayName("Cross-partition upsert without option produces duplicate in each partition")
        void crossPartitionUpsertWithoutOptionCreatesDuplicate() {
            // Create a PK table partitioned by region WITHOUT cross-partition-upsert.
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, val DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " PARTITIONED BY (region)"
                    + " TBLPROPERTIES ("
                    + "   'file.format' = 'parquet'"
                    + " )");

            // Insert initial row: id=1 in 'east'
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, val) VALUES (1, 'east', 10.0)");

            // Attempt cross-partition upsert: id=1 from 'east' to 'west'.
            // Without cross-partition-upsert=true, Paimon treats each partition
            // independently for upsert — the old row in 'east' is NOT removed.
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, val) VALUES (1, 'west', 15.0)");

            // Verify: id=1 exists in BOTH partitions (the problematic behavior
            // that cross-partition-upsert=true is designed to prevent).
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY region");

            assertThat(rows).hasSize(2);

            // id=1 still in 'east' (not removed)
            assertThat(rows.get(0).getString("region")).isEqualTo("east");
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);

            // id=1 also in 'west' (new insert)
            assertThat(rows.get(1).getString("region")).isEqualTo("west");
            assertThat(rows.get(1).getLong("id")).isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("No Residual Data After Cross-Partition Update")
    class NoResidualAfterCrossPartitionUpdate {

        @Test
        @DisplayName("Old partition has no residual data after cross-partition move")
        void oldPartitionCleanAfterMove() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, val DOUBLE,"
                    + " PRIMARY KEY (id) NOT ENFORCED)"
                    + " PARTITIONED BY (region)"
                    + " TBLPROPERTIES ("
                    + "   'cross-partition-upsert' = 'true',"
                    + "   'changelog-producer' = 'lookup',"
                    + "   'file.format' = 'parquet'"
                    + " )");

            // Insert rows across partitions
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, val) VALUES "
                    + "(1, 'east', 10.0), (2, 'east', 20.0), (3, 'west', 30.0)");

            // Cross-partition move: delete id=1 from 'east', re-insert into 'west'.
            // Batch writes do not support automatic cross-partition dedup, so
            // an explicit DELETE + INSERT is required.
            sqlRunner.execute("DELETE FROM " + fqn() + " WHERE id = 1");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, val) VALUES (1, 'west', 15.0)");

            // Query old partition 'east': id=1 should NOT be there
            List<TypedRow> eastRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'east' ORDER BY id");

            // Only id=2 should remain in 'east'
            assertThat(eastRows).hasSize(1);
            assertThat(eastRows.get(0).getLong("id")).isEqualTo(2L);

            // Query new partition 'west': id=1 and id=3 should be there
            List<TypedRow> westRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'west' ORDER BY id");

            assertThat(westRows).hasSize(2);
            assertThat(westRows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(westRows.get(0).getDouble("val")).isEqualTo(15.0);
            assertThat(westRows.get(1).getLong("id")).isEqualTo(3L);
            assertThat(westRows.get(1).getDouble("val")).isEqualTo(30.0);

            // Full table scan: exactly 3 rows, no duplicates
            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(allRows).hasSize(3);
        }
    }
}
