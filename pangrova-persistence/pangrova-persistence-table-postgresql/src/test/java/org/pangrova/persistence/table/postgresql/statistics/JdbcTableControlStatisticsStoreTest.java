package org.pangrova.persistence.table.postgresql.statistics;

import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlProvenance;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.statistics.TableControlStatistics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcTableControlStatisticsStoreTest extends ContainerTestBase {

    private static final Instant NOW = Instant.parse("2026-06-01T00:00:00Z");

    private DataSource dataSource;
    private JdbcTableControlStatisticsStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = MigratedPostgres.create();
        store = new JdbcTableControlStatisticsStore(dataSource);
    }

    @Test
    void statisticsRoundTripWithColumnStats() {
        UUID tableId = UUID.randomUUID();
        TableStatistics statistics = new TableStatistics(120, 9_000, 4,
                Map.of("amount", new TableStatistics.ColumnStatistics(
                        3, 40, 1.5d, 99.5d, 8.0)));

        store.save(new TableControlStatistics(
                tableId, 2, "ICEBERG:uuid:7", statistics, "ANALYZE", NOW));

        TableControlStatistics reloaded = store.find(tableId).orElseThrow();
        assertThat(reloaded.tableId()).isEqualTo(tableId);
        assertThat(reloaded.statsVersion()).isEqualTo(2);
        assertThat(reloaded.versionToken()).isEqualTo("ICEBERG:uuid:7");
        assertThat(reloaded.source()).isEqualTo("ANALYZE");
        assertThat(reloaded.computedAt()).isEqualTo(NOW);
        assertThat(reloaded.statistics().rowCount()).isEqualTo(120);
        assertThat(reloaded.statistics().dataSize()).isEqualTo(9_000);
        assertThat(reloaded.statistics().sourceWorkUnits()).isEqualTo(4);
        assertThat(reloaded.statistics().columnStats()).containsKey("amount");
        TableStatistics.ColumnStatistics amount =
                reloaded.statistics().columnStats().get("amount");
        assertThat(amount.nullCount()).isEqualTo(3);
        assertThat(amount.ndv()).isEqualTo(40);
        assertThat(amount.avgSize()).isEqualTo(8.0);
        assertThat(amount.min()).isEqualTo(1.5d);
        assertThat(amount.max()).isEqualTo(99.5d);
    }

    @Test
    void savingAgainReplacesTheRowForTheSameTable() {
        UUID tableId = UUID.randomUUID();
        store.save(new TableControlStatistics(tableId, 0, "v1",
                new TableStatistics(10, 100), "ANALYZE", NOW));
        store.save(new TableControlStatistics(tableId, 1, "v2",
                new TableStatistics(20, 200), "ANALYZE", NOW.plusSeconds(60)));

        TableControlStatistics reloaded = store.find(tableId).orElseThrow();
        assertThat(reloaded.statsVersion()).isEqualTo(1);
        assertThat(reloaded.versionToken()).isEqualTo("v2");
        assertThat(reloaded.statistics().rowCount()).isEqualTo(20);
    }

    @Test
    void deleteRemovesTheRow() {
        UUID tableId = UUID.randomUUID();
        store.save(new TableControlStatistics(tableId, 0, "v1",
                new TableStatistics(10, 100), "ANALYZE", NOW));

        store.delete(tableId);

        assertThat(store.find(tableId)).isEmpty();
    }

    @Test
    void planningObservesDeploymentAndOnlyReturnsVersionMatchedStatistics() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("""
                    INSERT INTO catalog_definitions (name, type, provider, state, properties, incarnation_id, revision)
                    VALUES ('cat', 'CONNECTOR', 'iceberg', 'ACTIVE', '{}', 'active-incarnation', 2)
                    """);
        }
        UUID tableId = UUID.randomUUID();
        var matched = new TableControlStatistics(tableId, 0, "v1", new TableStatistics(10, 100), "ANALYZE", NOW);
        store.save(matched);
        var owner = TableControlRecord.builder().tableId(tableId).name("cat", "db", "table")
                .format("ICEBERG").ownershipMode(OwnershipMode.MANAGED).state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED)
                .catalogIncarnationId("active-incarnation").catalogRevision(2L)
                .version("v1", NOW).createdAt(NOW).updatedAt(NOW).build();
        var changedTable = owner.toBuilder().version("v2", NOW).build();
        var changedCatalog = owner.toBuilder().catalogRevision(1L).build();
        var missingCatalog = owner.toBuilder().name("missing", "db", "table").build();
        var observations = store.observeForPlanning(Set.of(owner, changedTable, changedCatalog, missingCatalog));

        assertThat(observations.get(owner).statistics()).contains(matched);
        var deployment = new CatalogDeploymentIdentity("active-incarnation", 2L);
        for (var stale : Set.of(changedTable, changedCatalog)) {
            assertThat(observations.get(stale).deployment()).contains(deployment);
            assertThat(observations.get(stale).statistics()).isEmpty();
        }
        assertThat(observations.get(missingCatalog).deployment()).isEmpty();
        assertThat(observations.get(missingCatalog).statistics()).isEmpty();
    }

    @Test
    void unknownStatisticsRoundTripAsUnknown() {
        UUID tableId = UUID.randomUUID();
        store.save(new TableControlStatistics(tableId, 0, "v1",
                TableStatistics.UNKNOWN, "ANALYZE", NOW));

        TableStatistics reloaded = store.find(tableId).orElseThrow().statistics();
        assertThat(reloaded.rowCount()).isEqualTo(-1);
        assertThat(reloaded.dataSize()).isEqualTo(-1);
        assertThat(reloaded.sourceWorkUnits()).isEqualTo(-1);
        assertThat(reloaded.columnStats()).isEmpty();
    }
}
