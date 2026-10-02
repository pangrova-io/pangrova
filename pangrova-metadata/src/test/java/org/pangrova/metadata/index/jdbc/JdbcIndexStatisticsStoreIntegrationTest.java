package org.pangrova.metadata.index.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

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

        store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE, List.of(statistics));

        assertThat(store.getFileStatistics(CATALOG, SCHEMA, TABLE, List.of(FILE), List.of(COLUMN)))
                .containsOnlyKeys(FILE);
        assertThat(store.getFileStatistics(CATALOG, SCHEMA, TABLE, List.of(FILE), List.of(COLUMN)).get(FILE))
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
    void repeatedWritesRetainDuplicatesUnderTheCurrentSchema() throws SQLException {
        DataFileStatistics statistics = statistics(FILE, COLUMN);

        store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE, List.of(statistics));
        store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE, List.of(statistics));

        // This store currently has no uniqueness/idempotency contract; do not hide that with a fake store.
        assertThat(rowCount()).isEqualTo(2L);
        assertThat(store.getFileStatistics(CATALOG, SCHEMA, TABLE, List.of(FILE), List.of(COLUMN)).get(FILE))
                .containsExactly(statistics.columnStatistics().get(0), statistics.columnStatistics().get(0));
    }

    @Test
    void failedPostgresBatchRollsBackAllNewRowsAndPreservesCommittedRows() throws SQLException {
        DataFileStatistics committed = statistics(FILE, COLUMN);
        store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE, List.of(committed));
        String newFile = "/warehouse/events/new.parquet";
        // The production schema has VARCHAR(255), so PostgreSQL rejects the second batch item.
        DataFileStatistics invalid = statistics("/warehouse/events/invalid.parquet", "x".repeat(256));

        assertThatThrownBy(() -> store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE,
                List.of(statistics(newFile, COLUMN), invalid)))
                .isInstanceOf(MetastoreException.class)
                .hasCauseInstanceOf(SQLException.class);

        assertThat(rowCount()).isEqualTo(1L);
        assertThat(store.getFileStatistics(CATALOG, SCHEMA, TABLE, List.of(newFile), List.of(COLUMN)))
                .isEmpty();
        assertThat(store.getFileStatistics(CATALOG, SCHEMA, TABLE, List.of(FILE), List.of(COLUMN)).get(FILE))
                .containsExactly(committed.columnStatistics().get(0));
        store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE, List.of(statistics(newFile, COLUMN)));
        assertThat(rowCount()).isEqualTo(2L);
    }

    @Test
    void emptyStatisticsBatchDoesNotCreateRows() throws SQLException {
        store.batchInsertFileStatistics(CATALOG, SCHEMA, TABLE, List.of());

        assertThat(rowCount()).isZero();
    }

    private static DataFileStatistics statistics(String file, String column) {
        return new DataFileStatistics(file, "snapshot-17", 11L, 512L,
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
