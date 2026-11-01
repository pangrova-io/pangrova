package org.pangrova.table.statistics;

import org.pangrova.table.api.control.CatalogDeploymentIdentity;
import org.pangrova.table.api.control.OwnershipMode;
import org.pangrova.table.api.control.TableControlProvenance;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.table.api.control.TableControlStore;
import org.pangrova.table.api.control.TableVersionObservation;
import org.pangrova.table.api.statistics.TableControlStatistics;
import org.pangrova.table.control.TableControlService;
import org.pangrova.table.control.TableControlSettings;
import org.pangrova.table.store.memory.control.InMemoryTableControlStatisticsStore;
import org.pangrova.table.store.memory.control.InMemoryTableControlStore;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlStatisticsServiceTest {

    private static final Instant NOW = Instant.parse("2026-06-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String VERSION = "ICEBERG:uuid-orders:7";

    private final InMemoryTableControlStore records = new InMemoryTableControlStore();
    private final TableControlService control = new TableControlService(
            records, name -> new CatalogDeploymentIdentity("test-deployment", 1), new TableControlSettings(true, true, java.util.Set.of()), CLOCK);
    private final InMemoryTableControlStatisticsStore statistics =
            new InMemoryTableControlStatisticsStore();
    private final TableControlStatisticsService service =
            new TableControlStatisticsService(control, statistics, CLOCK);

    @Test
    void recordsStatisticsBoundToTheCurrentVersionAndIncrementsStatsVersion() {
        TableControlRecord record = managed("orders", VERSION);
        records.save(record);

        TableControlStatistics first = service.record("cat", "db", "orders",
                new TableStatistics(100, 1_000), "ANALYZE");
        TableControlStatistics second = service.record("cat", "db", "orders",
                new TableStatistics(120, 1_200), "ANALYZE");

        assertThat(first.tableId()).isEqualTo(record.tableId());
        assertThat(first.versionToken()).isEqualTo(VERSION);
        assertThat(first.computedAt()).isEqualTo(NOW);
        assertThat(second.statsVersion()).isEqualTo(first.statsVersion() + 1);
        assertThat(service.fresh("cat", "db", "orders")).contains(second);
    }

    @Test
    void freshHidesStatisticsOnceTheTableVersionAdvances() {
        TableControlRecord record = managed("orders", VERSION);
        records.save(record);
        service.record("cat", "db", "orders", new TableStatistics(100, 1_000), "ANALYZE");

        records.save(record.toBuilder()
                .version("ICEBERG:uuid-orders:8", NOW.plusSeconds(60))
                .metadataRevision(record.metadataRevision() + 1)
                .build());

        assertThat(service.fresh("cat", "db", "orders")).isEmpty();
    }

    @Test
    void observedVersionAdvancesTheRecordSoNewStatisticsAreNotBornStale() {
        TableControlRecord record = managed("orders", VERSION);
        records.save(record);

        TableControlStatistics collected = service.record("cat", "db", "orders",
                new TableStatistics(100, 1_000), "ANALYZE", "ICEBERG:uuid-orders:9");

        assertThat(collected.versionToken()).isEqualTo("ICEBERG:uuid-orders:9");
        assertThat(records.find("cat", "db", "orders")).get()
                .extracting(TableControlRecord::versionToken)
                .isEqualTo("ICEBERG:uuid-orders:9");
        assertThat(service.fresh("cat", "db", "orders")).contains(collected);
    }

    @Test
    void delayedCollectionCannotRollBackTheControlVersion() {
        TableControlRecord record = managed("orders", VERSION);
        records.save(record);
        var current = record.toBuilder().metadataRevision(record.metadataRevision() + 1)
                .version("ICEBERG:uuid-orders:9", NOW.plusSeconds(5)).build();
        records.save(current);

        assertThatThrownBy(() -> service.record(record,
                new TableStatistics(100, 1_000), "ANALYZE", "ICEBERG:uuid-orders:8"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("changed during collection");
        assertThat(records.findById(record.tableId())).contains(current);
        assertThat(statistics.find(record.tableId())).isEmpty();
    }

    @Test
    void delayedCollectionCannotPublishToAReplacementControlRecord() {
        TableControlRecord original = managed("orders", VERSION);
        records.save(original);
        control.markDropped(control.find("cat", "db", "orders").orElseThrow());
        TableControlRecord replacement = managed("orders", VERSION);
        records.save(replacement);

        assertThatThrownBy(() -> service.record(original,
                new TableStatistics(100, 1_000), "ANALYZE", VERSION))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("changed during collection");
        assertThat(statistics.find(replacement.tableId())).isEmpty();
    }

    @Test
    void observedTablesAreRejected() {
        assertThatThrownBy(() -> service.record("cat", "db", "external",
                new TableStatistics(100, 1_000), "ANALYZE"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
    }

    @Test
    void nonActiveRecordsAreRejected() {
        records.save(managed("orders", VERSION).toBuilder()
                .state(TableControlState.QUARANTINED)
                .build());

        assertThatThrownBy(() -> service.record("cat", "db", "orders",
                new TableStatistics(100, 1_000), "ANALYZE"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("QUARANTINED");
    }

    @Test
    void statisticsWithoutAVersionTokenAreRejected() {
        records.save(managed("orders", null));

        assertThatThrownBy(() -> service.record("cat", "db", "orders",
                new TableStatistics(100, 1_000), "ANALYZE"))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("no version token");
    }

    @Test
    void unknownConnectorStatisticsAreRejectedInsteadOfStoredAsZero() {
        records.save(managed("orders", VERSION));

        assertThatThrownBy(() -> service.record("cat", "db", "orders",
                TableStatistics.UNKNOWN, "ANALYZE"))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("no statistics");
        assertThat(statistics.find(records.find("cat", "db", "orders").orElseThrow().tableId()))
                .isEmpty();
    }

    @Test
    void aRecordRetiredDuringObservationRejectsStatistics() {
        TableControlRecord record = managed("orders", VERSION);
        records.save(record);
        InMemoryTableControlStore delegate = new InMemoryTableControlStore();
        delegate.save(record);
        TableControlStore retiring = new TableControlStore() {
            @Override
            public java.util.Optional<TableControlRecord> find(String catalog,
                    String schema, String table) {
                return delegate.find(catalog, schema, table);
            }

            @Override
            public java.util.Optional<TableControlRecord> findById(UUID tableId) {
                return java.util.Optional.empty();
            }

            @Override
            public java.util.List<TableControlRecord> listByCatalog(String catalog) {
                return delegate.listByCatalog(catalog);
            }

            @Override
            public java.util.List<TableControlRecord> listAll() {
                return delegate.listAll();
            }

            @Override
            public void save(TableControlRecord value) {
                delegate.save(value);
            }

            @Override
            public void saveWithObservation(TableControlRecord value, TableVersionObservation observation) {
                delegate.saveWithObservation(value, observation);
            }
        };
        TableControlService retiringControl = new TableControlService(retiring, name -> new CatalogDeploymentIdentity("test-deployment", 1),
                new TableControlSettings(true, true, java.util.Set.of()), CLOCK);
        TableControlStatisticsService retiringService =
                new TableControlStatisticsService(retiringControl, statistics, CLOCK);

        assertThatThrownBy(() -> retiringService.record("cat", "db", "orders",
                new TableStatistics(100, 1_000), "ANALYZE", "ICEBERG:uuid-orders:9"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("could not accept");
        assertThat(statistics.find(record.tableId())).isEmpty();
    }

    @Test
    void purgeRemovesStatisticsForRetiredTables() {
        TableControlRecord record = managed("orders", VERSION);
        records.save(record);
        service.record("cat", "db", "orders", new TableStatistics(100, 1_000), "ANALYZE");

        service.purge(record.tableId());

        assertThat(statistics.find(record.tableId())).isEmpty();
        assertThat(service.fresh("cat", "db", "orders")).isEmpty();
    }

    private static TableControlRecord managed(String table, String versionToken) {
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name("cat", "db", table)
                .catalogIncarnationId("test-deployment")
                .catalogRevision(1L)
                .format("ICEBERG")
                .formatTableUuid("uuid-" + table)
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED)
                .metadataRevision(1)
                .version(versionToken, versionToken == null ? null : NOW)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
