package org.pangrova.metadata.core.cache;

import org.pangrova.metadata.core.event.MetadataEvent;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates TieredMetadataCache invalidates entries on MetadataEvent reception.
 * Validates: Requirements 76.3.
 */
class TieredMetadataCacheEventTest {

    private TieredMetadataCache cache;

    @BeforeEach
    void setUp() {
        cache = new TieredMetadataCache();
    }

    @Test
    void validateOnHitEvictsStaleEntryAfterVersionBump() {
        // A-CACHE-01: markTableChanged bumps the table version WITHOUT removing the L1 entry; the
        // now-stale cached schema must be evicted on the next getTable (validate-on-hit), never served.
        cache.putTable("cat", "s", "orders", sampleMetadata());
        assertThat(cache.getTable("cat", "s", "orders")).as("fresh entry hits").isPresent();

        cache.markTableChanged("cat", "s", "orders");

        assertThat(cache.getTable("cat", "s", "orders")).as("stale entry evicted on hit").isEmpty();
        assertThat(cache.getTable("cat", "s", "orders")).as("stays evicted").isEmpty();
    }

    @Test
    void validateOnHitDoesNotEvictUnrelatedTable() {
        // A-CACHE-01: a version bump on a different table must not evict this entry.
        cache.putTable("cat", "s", "orders", sampleMetadata());
        cache.markTableChanged("cat", "s", "customers");
        assertThat(cache.getTable("cat", "s", "orders")).as("unrelated change must not evict").isPresent();
    }

    @Test
    void catalogDroppedEventInvalidatesAllEntriesForCatalog() {
        cache.putTable("iceberg", "db", "orders", sampleMetadata());
        cache.putTable("iceberg", "db", "users", sampleMetadata());
        cache.putTable("paimon", "db", "events", sampleMetadata());

        cache.onEvent(new MetadataEvent.CatalogDroppedEvent("iceberg"));

        assertThat(cache.getTable("iceberg", "db", "orders")).isEmpty();
        assertThat(cache.getTable("iceberg", "db", "users")).isEmpty();
        assertThat(cache.getTable("paimon", "db", "events")).isPresent();
    }

    @Test
    void catalogStateChangedEventInvalidatesCatalog() {
        cache.putTable("test_cat", "schema1", "t1", sampleMetadata());

        cache.onEvent(new MetadataEvent.CatalogStateChangedEvent(
                "test_cat",
                org.pangrova.metadata.api.catalog.CatalogState.ACTIVE,
                org.pangrova.metadata.api.catalog.CatalogState.DEGRADED));

        assertThat(cache.getTable("test_cat", "schema1", "t1")).isEmpty();
    }

    @Test
    void tableDroppedEventInvalidatesSingleTable() {
        cache.putTable("cat", "s", "keep", sampleMetadata());
        cache.putTable("cat", "s", "drop_me", sampleMetadata());

        cache.onEvent(new MetadataEvent.TableDroppedEvent(new QualifiedTableRef("cat", "s", "drop_me")));

        assertThat(cache.getTable("cat", "s", "drop_me")).isEmpty();
        assertThat(cache.getTable("cat", "s", "keep")).isPresent();
    }

    @Test
    void tableAlteredEventInvalidatesSingleTable() {
        cache.putTable("cat", "s", "altered", sampleMetadata());

        cache.onEvent(new MetadataEvent.TableAlteredEvent(
                new QualifiedTableRef("cat", "s", "altered"), List.of()));

        assertThat(cache.getTable("cat", "s", "altered")).isEmpty();
    }

    @Test
    void catalogCreatedEventDoesNotInvalidate() {
        cache.putTable("existing", "s", "t", sampleMetadata());

        cache.onEvent(new MetadataEvent.CatalogCreatedEvent(null));

        assertThat(cache.getTable("existing", "s", "t")).isPresent();
    }

    @Test
    void metadataSyncedEventInvalidatesCatalog() {
        cache.putTable("synced", "db", "t1", sampleMetadata());

        cache.onEvent(new MetadataEvent.MetadataSyncedEvent("synced", null));

        assertThat(cache.getTable("synced", "db", "t1")).isEmpty();
    }

    private static TableMetadata sampleMetadata() {
        return new TableMetadata("cat", "s", "t", List.of(), null, null, null, null, null);
    }
}
