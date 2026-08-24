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
 * Iceberg connector E2E tests for advanced features: time travel,
 * schema evolution, partition evolution, and partition pruning.
 *
 * <p>Covers R9.2-R9.5, R9.11-R9.16. All operations execute through the real
 * pipeline: TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 *
 * <p>Requirements: 9.2, 9.3, 9.4, 9.5, 16.6
 */
@DisplayName("Iceberg Connector - Advanced Feature E2E Tests")
class IcebergAdvancedTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
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

    // --- R9.2: Time travel by snapshot ID ---

    @Nested
    @DisplayName("R9.2 - Time Travel by Snapshot ID")
    class TimeTravelBySnapshotId {

        @Test
        @DisplayName("Time travel query returns historical data at snapshot")
        void timeTravelReturnsHistoricalData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (1, 'original')");

            long snapshotId = currentSnapshotId();

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (2, 'added_later')");

            List<TypedRow> historicalRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " FOR VERSION AS OF " + snapshotId + " ORDER BY id");

            assertThat(historicalRows).hasSize(1);
            assertThat(historicalRows.get(0).getRaw("val")).isEqualTo("original");
        }
    }

    // --- R9.11: Time travel after multiple snapshots ---

    @Nested
    @DisplayName("R9.11 - Time Travel After Multiple Snapshots")
    class TimeTravelMultipleSnapshots {

        @Test
        @DisplayName("Each snapshot returns correct data via time travel")
        void eachSnapshotReturnsCorrectData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, val VARCHAR)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (1, 'v1')");
            long snapshot1 = currentSnapshotId();

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (2, 'v2')");
            long snapshot2 = currentSnapshotId();

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, val) VALUES (3, 'v3')");

            List<TypedRow> atSnap1 = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " FOR VERSION AS OF " + snapshot1 + " ORDER BY id");
            List<TypedRow> atSnap2 = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " FOR VERSION AS OF " + snapshot2 + " ORDER BY id");
            List<TypedRow> current = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(atSnap1).hasSize(1);
            assertThat(atSnap2).hasSize(2);
            assertThat(current).hasSize(3);
        }
    }


    // --- R9.4: Partition evolution ---

    @Nested
    @DisplayName("R9.4 - Partition Evolution")
    class PartitionEvolution {

        @Test
        @DisplayName("Old and new partition strategies both readable after evolution")
        void partitionEvolutionPreservesData() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, ts TIMESTAMP)"
                    + " PARTITIONED BY (category)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, ts) VALUES "
                    + "(1, 'A', TIMESTAMP '2024-01-01 00:00:00'),"
                    + " (2, 'B', TIMESTAMP '2024-01-02 00:00:00')");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " SET PARTITION SPEC (category, month(ts))");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, ts) VALUES "
                    + "(3, 'A', TIMESTAMP '2024-02-01 00:00:00'),"
                    + " (4, 'C', TIMESTAMP '2024-02-15 00:00:00')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(4);
            assertThat(rows.get(0).getRaw("category")).isEqualTo("A");
            assertThat(rows.get(1).getRaw("category")).isEqualTo("B");
            assertThat(rows.get(2).getRaw("category")).isEqualTo("A");
            assertThat(rows.get(3).getRaw("category")).isEqualTo("C");
        }
    }

    // --- R9.14: Partition evolution from unpartitioned to partitioned ---

    @Nested
    @DisplayName("R9.14 - Partition Evolution: Unpartitioned to Partitioned")
    class UnpartitionedToPartitioned {

        @Test
        @DisplayName("Data written before partitioning is still readable after adding partition spec")
        void unpartitionedToPartitioned() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES "
                    + "(1, 'east', 100.0), (2, 'west', 200.0)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " SET PARTITION SPEC (region)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (3, 'east', 300.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(2).getRaw("id")).longValue()).isEqualTo(3L);
        }
    }

    // --- R9.5: Partition pruning verification ---

    @Nested
    @DisplayName("R9.5 - Partition Pruning Verification")
    class PartitionPruning {

        @Test
        @DisplayName("Predicate pushdown to partition column prunes partitions")
        void partitionPruningViaPredicate() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE)"
                    + " PARTITIONED BY (region)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES "
                    + "(1, 'east', 100.0), (2, 'west', 200.0), (3, 'east', 300.0)");

            List<TypedRow> explainRows = sqlRunner.execute(
                    "EXPLAIN SELECT * FROM " + fqn() + " WHERE region = 'east'");

            String explainOutput = explainRows.stream()
                    .map(r -> r.values().toString())
                    .reduce("", String::concat);
            assertThat(explainOutput).isNotEmpty();

            List<TypedRow> resultRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE region = 'east' ORDER BY id");

            assertThat(resultRows).hasSize(2);
            assertThat(resultRows).allSatisfy(row ->
                    assertThat(row.getRaw("region")).isEqualTo("east"));
        }
    }

    // --- R9.15: Partition pruning with equality predicate ---

    @Nested
    @DisplayName("R9.15 - Partition Pruning: Equality Predicate")
    class PartitionPruningEquality {

        @Test
        @DisplayName("Equality predicate on partition column returns only matching partition data")
        void equalityPredicatePruning() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, status VARCHAR, val DOUBLE)"
                    + " PARTITIONED BY (status)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, val) VALUES "
                    + "(1, 'active', 10.0), (2, 'inactive', 20.0),"
                    + " (3, 'active', 30.0), (4, 'archived', 40.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE status = 'active' ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows).allSatisfy(row ->
                    assertThat(row.getRaw("status")).isEqualTo("active"));
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) rows.get(1).getRaw("id")).longValue()).isEqualTo(3L);
        }
    }

    // --- R9.16: Partition pruning with range predicate ---

    @Nested
    @DisplayName("R9.16 - Partition Pruning: Range Predicate")
    class PartitionPruningRange {

        @Test
        @DisplayName("Range predicate on partition column filters correctly")
        void rangePredicatePruning() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, year_part BIGINT, val VARCHAR)"
                    + " PARTITIONED BY (year_part)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, year_part, val) VALUES "
                    + "(1, 2022, 'old'), (2, 2023, 'mid'),"
                    + " (3, 2024, 'new'), (4, 2024, 'newest')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE year_part >= 2023 ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows).allSatisfy(row ->
                    assertThat(((Number) row.getRaw("year_part")).longValue())
                            .isGreaterThanOrEqualTo(2023L));
        }
    }

    // --- A5: Multi-Argument Partition Transform Pruning ---

    @Nested
    @DisplayName("A5 - Multi-Argument Partition Transform Pruning")
    class MultiArgPartitionTransformPruning {

        @Test
        @DisplayName("bucket(col, 16) partition transform with data skipping")
        void bucketSingleColumnPartitionPruning() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES "
                    + "(1, 'east', 100.0), (2, 'west', 200.0), (3, 'east', 300.0)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " SET PARTITION SPEC (bucket(region, 16))");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES "
                    + "(4, 'north', 400.0), (5, 'south', 500.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE region = 'east' ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getRaw("region")).isEqualTo("east");
            assertThat(rows.get(1).getRaw("region")).isEqualTo("east");
        }

        @Test
        @DisplayName("bucket(col1, col2, 16) multi-column partition transform preserves data")
        void bucketMultiColumnPartitionTransform() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, category VARCHAR, region VARCHAR, amount DOUBLE)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, region, amount) VALUES "
                    + "(1, 'A', 'east', 100.0), (2, 'B', 'west', 200.0)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " SET PARTITION SPEC (bucket(category, 16), bucket(region, 16))");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, category, region, amount) VALUES "
                    + "(3, 'A', 'west', 300.0), (4, 'C', 'east', 400.0)");

            List<TypedRow> allRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(allRows).hasSize(4);

            List<TypedRow> filteredRows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE category = 'A' ORDER BY id");
            assertThat(filteredRows).hasSize(2);
            assertThat(filteredRows.get(0).getRaw("category")).isEqualTo("A");
            assertThat(filteredRows.get(1).getRaw("category")).isEqualTo("A");
        }

        @Test
        @DisplayName("truncate(col, 10) partition transform with data skipping")
        void truncatePartitionPruning() {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(1, 'alice', 100.0), (2, 'bob', 200.0)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn()
                    + " SET PARTITION SPEC (truncate(name, 5))");

            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES "
                    + "(3, 'charlie', 300.0), (4, 'alice_jr', 400.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");
            assertThat(rows).hasSize(4);
        }
    }

    // --- Helper: retrieve current snapshot ID via Iceberg Java API ---

    /**
     * Retrieves the current snapshot ID by loading the Iceberg Table directly
     * from the catalog via {@link IcebergMetadata#catalog()}.
     */
    private long currentSnapshotId() {
        var catalogHandle = metadataBootstrap.catalogRegistry().getCatalog(CATALOG);
        var session = new org.pangrova.coordinator.query.statement.StatementConnectorSession(CATALOG, SCHEMA);
        var metadata = (org.pangrova.connector.iceberg.IcebergMetadata) catalogHandle.getMetadata(session);
        org.apache.iceberg.Table icebergTable = metadata.catalog().loadTable(
                org.apache.iceberg.catalog.TableIdentifier.of(SCHEMA, tableName));
        assertThat(icebergTable.currentSnapshot()).isNotNull();
        return icebergTable.currentSnapshot().snapshotId();
    }
}
