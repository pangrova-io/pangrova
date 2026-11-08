package org.pangrova.persistence.indexing.postgresql.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import org.pangrova.common.concurrent.ExecutorDrain;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** PostgreSQL contract tests using the complete production schema migration chain. */
@Tag("integration")
class JdbcIndexStatisticsStoreIntegrationTest {

    private static final String CATALOG = "catalog";
    private static final String SCHEMA = "analytics";
    private static final String TABLE = "events";
    private static final String FILE = "/warehouse/events/first.parquet";
    private static final String COLUMN = "category";
    private static final String TENANT = "tenant-a";
    private static final String SNAPSHOT = "snapshot-17";

    private DataSource dataSource;
    private JdbcIndexStatisticsStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("file_statistics");
        store = new JdbcIndexStatisticsStore(dataSource);
    }

    @Test
    void writesAndReadsStatisticsWithDatabaseGeneratedCreationTime() throws SQLException {
        LocalDateTime databaseBefore = databaseTime();
        DataFileStatistics statistics = statistics(FILE, COLUMN);

        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(statistics));

        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, List.of(FILE), List.of(COLUMN)))
                .containsOnlyKeys(FILE);
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, List.of(FILE), List.of(COLUMN)).get(FILE))
                .containsExactly(statistics.columnStatistics().get(0));
        LocalDateTime databaseAfter = databaseTime();
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT snapshot_id, created_at FROM file_column_statistics WHERE data_file_path=?")) {
            statement.setString(1, FILE);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("snapshot_id")).isEqualTo("snapshot-17");
                assertThat(result.getObject("created_at", LocalDateTime.class))
                        .isNotNull().isBetween(databaseBefore, databaseAfter);
                assertThat(result.next()).isFalse();
            }
        }
    }

    @Test
    void repeatedWritesReplaceTheExactFileSnapshotIdempotently() throws SQLException {
        DataFileStatistics statistics = statistics(FILE, COLUMN);

        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(statistics));
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(statistics));

        assertThat(rowCount()).isEqualTo(1L);
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, List.of(FILE), List.of(COLUMN)).get(FILE))
                .containsExactly(statistics.columnStatistics().get(0));
    }

    @Test
    void failedPostgresBatchRollsBackAllNewRowsAndPreservesCommittedRows() throws SQLException {
        DataFileStatistics committed = statistics(FILE, COLUMN);
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(committed));
        String newFile = "/warehouse/events/new.parquet";
        // The production schema has VARCHAR(255), so PostgreSQL rejects the second batch item.
        DataFileStatistics invalid = statistics("/warehouse/events/invalid.parquet", "x".repeat(256));
        var replacement = new DataFileStatistics(FILE, SNAPSHOT, 1, 1,
                List.of(new FileColumnStatistics(COLUMN, "replacement", "replacement", 0, 1, 1, 1)));

        assertThatThrownBy(() -> store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE,
                List.of(replacement, statistics(newFile, COLUMN), invalid)))
                .isInstanceOf(MetastoreException.class)
                .hasCauseInstanceOf(SQLException.class);

        assertThat(rowCount()).isEqualTo(1L);
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, List.of(newFile), List.of(COLUMN)))
                .isEmpty();
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, List.of(FILE), List.of(COLUMN)).get(FILE))
                .containsExactly(committed.columnStatistics().get(0));
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(statistics(newFile, COLUMN)));
        assertThat(rowCount()).isEqualTo(2L);
    }

    @Test
    void concurrentOverlappingBatchesInOppositeOrdersNeverDuplicateOrMixFiles() throws Exception {
        var first = statistics(FILE, COLUMN);
        var second = new DataFileStatistics("/warehouse/events/second.parquet", "snapshot-18", 11, 512,
                first.columnStatistics());
        var ready = new CountDownLatch(6);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(6);
        var futures = new ArrayList<Future<?>>();
        try {
            for (int writer = 0; writer < 6; writer++) {
                var batch = writer % 2 == 0 ? List.of(first, second) : List.of(second, first);
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("Writers were not released");
                    store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, batch);
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (var future : futures) future.get(10, TimeUnit.SECONDS);
            assertThat(rowCount()).isEqualTo(2);
            assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT,
                    List.of(FILE, second.dataFilePath()), List.of(COLUMN)))
                    .containsOnlyKeys(FILE).containsEntry(FILE, first.columnStatistics());
            assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, "snapshot-18",
                    List.of(FILE, second.dataFilePath()), List.of(COLUMN)))
                    .containsOnlyKeys(second.dataFilePath())
                    .containsEntry(second.dataFilePath(), second.columnStatistics());
        } finally {
            start.countDown();
            ExecutorDrain.shutdownAndAwait(executor, 10_000);
        }
    }

    @Test
    void ambiguousDuplicateFilesOrColumnsRollBackWithoutChangingCommittedStatistics() throws SQLException {
        var committed = statistics(FILE, COLUMN);
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(committed));
        assertThatThrownBy(() -> store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE,
                List.of(committed, committed))).isInstanceOf(IllegalArgumentException.class);
        var duplicateColumns = new DataFileStatistics(FILE, SNAPSHOT, 11, 512,
                List.of(committed.columnStatistics().get(0), committed.columnStatistics().get(0)));
        assertThatThrownBy(() -> store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE,
                List.of(duplicateColumns))).isInstanceOf(IllegalArgumentException.class);

        assertThat(rowCount()).isEqualTo(1);
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE), List.of(COLUMN))).containsEntry(FILE, committed.columnStatistics());
    }

    @Test
    void emptyStatisticsBatchDoesNotCreateRows() throws SQLException {
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of());

        assertThat(rowCount()).isZero();
    }

    @Test
    void aLargeFileBatchDoesNotExhaustPostgresSharedLocks() throws SQLException {
        var statistics = java.util.stream.IntStream.range(0, 20_000)
                .mapToObj(file -> statistics("/warehouse/events/batch-" + file + ".parquet", COLUMN)).toList();

        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, statistics);

        assertThat(rowCount()).isEqualTo(statistics.size());
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(statistics.get(0).dataFilePath(), statistics.get(statistics.size() - 1).dataFilePath()),
                List.of(COLUMN))).hasSize(2);
    }

    @Test
    void readsLargeFileAndColumnSelectionsWithoutExpandingJdbcBindParameters() {
        var committed = statistics(FILE, COLUMN);
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(committed));
        store.batchInsertFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, List.of(committed));
        var paths = largeSelection(FILE, "/warehouse/events/missing-");
        var columns = largeSelection(COLUMN, "missing_column_");

        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, paths, columns))
                .containsOnlyKeys(FILE).containsEntry(FILE, committed.columnStatistics());
    }

    @Test
    void deletesLargeFileSelectionsAtomicallyWithoutTouchingOtherScopes() {
        var committed = statistics(FILE, COLUMN);
        String retainedFile = "/warehouse/events/retained.parquet";
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE,
                List.of(committed, statistics(retainedFile, COLUMN)));
        store.batchInsertFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, List.of(committed));

        store.deleteFileStatistics(TENANT, CATALOG, SCHEMA, TABLE,
                largeSelection(FILE, "/warehouse/events/missing-"));

        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE, retainedFile), List.of(COLUMN)))
                .containsOnlyKeys(retainedFile);
        assertThat(store.getFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE), List.of(COLUMN))).containsEntry(FILE, committed.columnStatistics());
    }

    private static List<String> largeSelection(String selected, String absentPrefix) {
        var selection = new ArrayList<String>(70_000);
        selection.add(selected);
        java.util.stream.IntStream.range(1, 70_000)
                .mapToObj(index -> absentPrefix + index).forEach(selection::add);
        return List.copyOf(selection);
    }

    @Test
    void readsOnlyTheExactTenantAndSnapshotForIdenticalFileAndColumnNames() {
        var first = statistics(FILE, COLUMN);
        var other = new DataFileStatistics(FILE, SNAPSHOT, 5, 256,
                List.of(new FileColumnStatistics(COLUMN, "secret", "secret", 0, 5, 1, 256)));
        var newer = new DataFileStatistics(FILE, "snapshot-18", 19, 1024,
                List.of(new FileColumnStatistics(COLUMN, "new", "new", 0, 19, 1, 1024)));
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(first, newer));
        store.batchInsertFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, List.of(other));

        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT, List.of(FILE), List.of(COLUMN)))
                .containsEntry(FILE, first.columnStatistics());
        assertThat(store.getFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE), List.of(COLUMN))).containsEntry(FILE, other.columnStatistics());
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, "snapshot-18",
                List.of(FILE), List.of(COLUMN))).containsEntry(FILE, newer.columnStatistics());
        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, "absent-snapshot",
                List.of(FILE), List.of(COLUMN))).isEmpty();
    }

    @Test
    void deletingRemovedFilesNeverDeletesAnotherTenantsStatistics() {
        var statistics = statistics(FILE, COLUMN);
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(statistics));
        store.batchInsertFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, List.of(statistics));

        store.deleteFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(FILE));

        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE), List.of(COLUMN))).isEmpty();
        assertThat(store.getFileStatistics("tenant-b", CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE), List.of(COLUMN))).containsEntry(FILE, statistics.columnStatistics());
    }

    @Test
    void rejectsMissingAuthorityEvenWhenTheRequestedBatchIsEmpty() {
        assertThatThrownBy(() -> store.batchInsertFileStatistics(null, CATALOG, SCHEMA, TABLE, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tenantId");
        assertThatThrownBy(() -> store.deleteFileStatistics(" ", CATALOG, SCHEMA, TABLE, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tenantId");
        assertThatThrownBy(() -> store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, null,
                List.of(), List.of())).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("snapshotId");
        var unscoped = new DataFileStatistics(FILE, " ", 1, 1, statistics(FILE, COLUMN).columnStatistics());
        assertThatThrownBy(() -> store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(unscoped)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("snapshotId");
    }

    @Test
    void unscopedHistoricalRowsArePreservedButNeverReadOrDeletedByTenantAccess() throws SQLException {
        store.batchInsertFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(statistics(FILE, COLUMN)));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE file_column_statistics SET tenant_id=NULL, snapshot_id=NULL");
        }

        assertThat(store.getFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, SNAPSHOT,
                List.of(FILE), List.of(COLUMN))).isEmpty();
        store.deleteFileStatistics(TENANT, CATALOG, SCHEMA, TABLE, List.of(FILE));
        assertThat(rowCount()).isEqualTo(1L);
    }

    private static DataFileStatistics statistics(String file, String column) {
        return new DataFileStatistics(file, SNAPSHOT, 11L, 512L,
                List.of(new FileColumnStatistics(column, "alpha", "omega", 1L, 11L, 7L, 512L)));
    }

    private LocalDateTime databaseTime() throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT LOCALTIMESTAMP")) {
            if (!result.next()) {
                throw new IllegalStateException("PostgreSQL did not return its current timestamp");
            }
            return result.getObject(1, LocalDateTime.class);
        }
    }

    private long rowCount() throws SQLException {
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT COUNT(*) FROM file_column_statistics")) {
            if (!result.next()) {
                throw new IllegalStateException("PostgreSQL did not return the statistics row count");
            }
            return result.getLong(1);
        }
    }
}
