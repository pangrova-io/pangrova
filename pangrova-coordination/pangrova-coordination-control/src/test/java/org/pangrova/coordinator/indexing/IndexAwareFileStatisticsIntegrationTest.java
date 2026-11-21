package org.pangrova.coordinator.indexing;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.indexing.api.pruning.IndexHint;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Real PostgreSQL-to-pruning statistics boundary; not public SQL or distributed acceptance. */
@Tag("integration")
class IndexAwareFileStatisticsIntegrationTest {
    private static final String FILE = "s3://warehouse/data.parquet";
    private static final String COLUMN = "category";
    private JdbcIndexMetadataStore store;
    private IndexAwareSplitFetcher fetcher;

    @BeforeEach
    void setUp() throws Exception {
        store = new JdbcIndexMetadataStore(SharedPostgresContainer.migratedDataSource("query_file_statistics"), TableWriteAuthorityFixture.create()::requireCurrent);
        fetcher = new IndexAwareSplitFetcher(null, null, store, null);
        store.batchInsertFileStatistics("tenant-a", "lake", "sales", "events",
                List.of(statistics("17", "alpha"), statistics("18", "new")));
        store.batchInsertFileStatistics("tenant-b", "lake", "sales", "events",
                List.of(statistics("17", "secret")));
    }

    @Test
    void queryConsumerSelectsOnlyTheBoundTenantAndExactSnapshot() {
        assertThat(fetch("tenant-a", "17", FILE, COLUMN))
                .containsEntry(FILE, statistics("17", "alpha").columnStatistics());
        assertThat(fetch("tenant-b", "17", FILE, COLUMN))
                .containsEntry(FILE, statistics("17", "secret").columnStatistics());
        assertThat(fetch("tenant-a", "18", FILE, COLUMN))
                .containsEntry(FILE, statistics("18", "new").columnStatistics());
    }

    @Test
    void missingSnapshotNeverPrunesUsingUnboundCachedStatistics() {
        assertThat(fetch("tenant-a", null, FILE, COLUMN)).isEmpty();
        assertThat(fetch("tenant-a", " ", FILE, COLUMN)).isEmpty();
        assertThat(fetch("tenant-a", "unknown", FILE, COLUMN)).isEmpty();
    }

    @Test
    void statisticsFromOtherFilesOrColumnsCannotEnterThePruningInput() {
        assertThat(fetch("tenant-a", "17", "s3://warehouse/other.parquet", COLUMN)).isEmpty();
        assertThat(fetch("tenant-a", "17", FILE, "other-column")).isEmpty();
        assertThat(fetch("absent-tenant", "17", FILE, COLUMN)).isEmpty();
    }

    private java.util.Map<String, List<FileColumnStatistics>> fetch(
            String tenantId, String snapshot, String file, String column) {
        var hint = new IndexHint("index", IndexType.BLOOM_FILTER, 0, new EqualTo(column, "alpha"));
        return fetcher.fetchFileStatistics(tenantId, "lake", "sales", "events", snapshot,
                List.of(new FileSplit(file)), List.of(hint), null);
    }

    private static DataFileStatistics statistics(String snapshot, String bound) {
        return new DataFileStatistics(FILE, snapshot, 1, 16,
                List.of(new FileColumnStatistics(COLUMN, bound, bound, 0, 1, 1, 16)));
    }

    private record FileSplit(String file) implements ConnectorSplit {
        @Override
        public List<String> dataFilePaths() { return List.of(file); }
    }
}
