package org.pangrova.indexing.store.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.store.jdbc.JdbcSupport;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link JdbcSupport} instants stored through the index store survive cross-connection zones. */
class JdbcIndexMetadataStoreZoneTest {

    @Test
    void indexDescriptorFenceSurvivesCrossConnectionTimeZones() throws Exception {
        DataSource base = SharedPostgresContainer.migratedDataSource("index_fence_zones");
        DataSource utc = inTimeZone(base, "UTC");
        DataSource losAngeles = inTimeZone(base, "America/Los_Angeles");
        Instant initial = Instant.parse("2026-09-19T04:05:06.123456789Z");
        Instant initialExpected = Instant.parse("2026-09-19T04:05:06.123456Z");
        Instant next = Instant.parse("2026-09-19T04:06:06.987654321Z");
        Instant nextExpected = Instant.parse("2026-09-19T04:06:06.987654Z");

        new JdbcIndexMetadataStore(utc).registerIndex(new IndexDescriptor(
                "cross-zone-index", "cross_zone", "lake", "sales", "orders",
                List.of("embedding"), IndexType.VECTOR, Map.of(),
                IndexState.BUILDING, null, 0, "snapshot-1", null,
                initial, initial, "tenant-a"));

        JdbcIndexMetadataStore losAngelesStore = new JdbcIndexMetadataStore(losAngeles);
        IndexDescriptor observed = losAngelesStore.findIndex("cross-zone-index").orElseThrow();
        assertThat(observed.createdAt()).isEqualTo(initialExpected);
        assertThat(observed.updatedAt()).isEqualTo(initialExpected);
        assertThat(losAngelesStore.updateIndexState(
                observed.indexId(), IndexState.READY.name(), next, observed.updatedAt()))
                .isTrue();

        assertThat(new JdbcIndexMetadataStore(utc).findIndex(observed.indexId()).orElseThrow())
                .satisfies(updated -> {
                    assertThat(updated.state()).isEqualTo(IndexState.READY);
                    assertThat(updated.updatedAt()).isEqualTo(nextExpected);
                });
    }

    private static DataSource inTimeZone(DataSource source, String timeZone) {
        PGSimpleDataSource original = (PGSimpleDataSource) source;
        PGSimpleDataSource configured = new PGSimpleDataSource();
        configured.setURL(original.getURL());
        configured.setUser(original.getUser());
        configured.setPassword(original.getPassword());
        configured.setCurrentSchema(original.getCurrentSchema());
        configured.setOptions("-c TimeZone=" + timeZone);
        return configured;
    }
}
