package org.pangrova.metadata.index.jdbc;

import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC operations for file column statistics (L0 pruning support).
 */
final class JdbcIndexStatisticsStore extends JdbcStoreSupport {

    // PreparedStatement parameter indices beyond the single-digit range for the insert statement.
    private static final int PARAM_ROW_COUNT = 11;
    private static final int PARAM_APPROXIMATE_NDV = 12;
    private static final int PARAM_TOTAL_SIZE_BYTES = 13;

    JdbcIndexStatisticsStore(DataSource dataSource) {
        super(dataSource);
    }

    void batchInsertFileStatistics(
            String tenantId, String catalog, String schema, String table,
            List<DataFileStatistics> statistics) {
        requireScope(tenantId, "tenantId");
        statistics.forEach(statisticsRow -> requireScope(statisticsRow.snapshotId(), "snapshotId"));
        if (statistics.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO file_column_statistics "
                + "(tenant_id, catalog_name, schema_name, table_name, data_file_path, snapshot_id, "
                + "column_name, min_value, max_value, null_count, row_count, "
                + "approximate_ndv, total_size_bytes, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            try {
                replaceSnapshotFiles(conn, tenantId, catalog, schema, table, statistics);
                bindStatisticsBatch(ps, tenantId, catalog, schema, table, statistics);
                ps.executeBatch();
                conn.commit();
            } catch (SQLException | RuntimeException failure) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException e) {
            throw new MetastoreException("Batch insert file statistics failed", e);
        }
    }

    private static void replaceSnapshotFiles(Connection connection, String tenantId,
            String catalog, String schema, String table, List<DataFileStatistics> statistics) throws SQLException {
        var identities = statistics.stream().map(file ->
                statisticsIdentity(tenantId, catalog, schema, table, file)).sorted().toList();
        if (identities.stream().distinct().count() != identities.size()) {
            throw new IllegalArgumentException("Statistics batch contains duplicate snapshot/file identities");
        }
        // One transaction-scoped lock per table/snapshot, not per file: large
        // commits must not exhaust PostgreSQL's shared advisory-lock capacity.
        // Hash collisions may add contention, but never establish row identity;
        // the deletion below always compares every exact identity component.
        // Order by the actual lock key, not the full identity: even a hash
        // collision must not invert lock order between two overlapping batches.
        var lockKeys = statistics.stream().map(file ->
                statisticsScopeIdentity(tenantId, catalog, schema, table, file.snapshotId()))
                .distinct().map(identity -> Long.parseUnsignedLong(
                        DigestUtils.sha256Hex("file-statistics:" + identity).substring(0, 16), 16))
                .distinct().sorted().toList();
        try (var lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
            for (long lockKey : lockKeys) {
                lock.setLong(1, lockKey);
                try (var result = lock.executeQuery()) {
                    if (!result.next()) throw new SQLException("Statistics ownership lock returned no result");
                }
            }
        }
        String sql = "DELETE FROM file_column_statistics WHERE tenant_id=? AND catalog_name=? "
                + "AND schema_name=? AND table_name=? AND snapshot_id=? AND data_file_path=?";
        try (var delete = connection.prepareStatement(sql)) {
            for (var file : statistics) {
                delete.setString(1, tenantId);
                delete.setString(2, catalog);
                delete.setString(3, schema);
                delete.setString(4, table);
                delete.setString(5, file.snapshotId());
                delete.setString(6, file.dataFilePath());
                delete.addBatch();
            }
            delete.executeBatch();
        }
    }

    private static String statisticsIdentity(String tenantId, String catalog, String schema,
            String table, DataFileStatistics file) {
        return statisticsScopeIdentity(tenantId, catalog, schema, table, file.snapshotId())
                + file.dataFilePath().length() + ':' + file.dataFilePath();
    }

    private static String statisticsScopeIdentity(String tenantId, String catalog, String schema,
            String table, String snapshotId) {
        var key = new StringBuilder();
        for (String part : List.of(tenantId, catalog, schema, table, snapshotId)) {
            key.append(part.length()).append(':').append(part);
        }
        return key.toString();
    }

    private static void bindStatisticsBatch(PreparedStatement ps, String tenantId, String catalog,
                                            String schema, String table,
                                            List<DataFileStatistics> statistics) throws SQLException {
        for (var fileStat : statistics) {
            if (fileStat.columnStatistics().stream().map(FileColumnStatistics::columnName).distinct().count()
                    != fileStat.columnStatistics().size()) {
                throw new IllegalArgumentException("Statistics file contains duplicate column names");
            }
            for (var colStat : fileStat.columnStatistics()) {
                ps.setString(1, tenantId);
                ps.setString(2, catalog);
                ps.setString(3, schema);
                ps.setString(4, table);
                ps.setString(5, fileStat.dataFilePath());
                ps.setString(6, fileStat.snapshotId());
                ps.setString(7, colStat.columnName());
                ps.setString(8, colStat.minValue() != null ? colStat.minValue().toString() : null);
                ps.setString(9, colStat.maxValue() != null ? colStat.maxValue().toString() : null);
                ps.setLong(10, colStat.nullCount());
                ps.setLong(PARAM_ROW_COUNT, colStat.rowCount());
                ps.setLong(PARAM_APPROXIMATE_NDV, colStat.approximateNdv());
                ps.setLong(PARAM_TOTAL_SIZE_BYTES, colStat.totalSizeBytes());
                ps.addBatch();
            }
        }
    }

    void deleteFileStatistics(String tenantId, String catalog, String schema, String table,
                             List<String> dataFilePaths) {
        requireScope(tenantId, "tenantId");
        if (dataFilePaths.isEmpty()) {
            return;
        }
        String sql = "DELETE FROM file_column_statistics "
                + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=? "
                + "AND data_file_path = ANY(?)";
        try (Connection connection = dataSource().getConnection();
                TextSelection paths = TextSelection.create(connection, dataFilePaths);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, catalog);
            statement.setString(3, schema);
            statement.setString(4, table);
            statement.setArray(5, paths.array());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new MetastoreException("Delete scoped file statistics failed", failure);
        }
    }

    Map<String, List<FileColumnStatistics>> getFileStatistics(
            String tenantId, String catalog, String schema, String table, String snapshotId,
            List<String> dataFilePaths, List<String> columns) {
        requireScope(tenantId, "tenantId");
        requireScope(snapshotId, "snapshotId");
        if (dataFilePaths.isEmpty() || columns.isEmpty()) {
            return Map.of();
        }
        String sql = "SELECT data_file_path, column_name, min_value, max_value, "
                + "null_count, row_count, approximate_ndv, total_size_bytes "
                + "FROM file_column_statistics "
                + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=? AND snapshot_id=? "
                + "AND data_file_path = ANY(?) AND column_name = ANY(?)";
        try (Connection connection = dataSource().getConnection();
                TextSelection paths = TextSelection.create(connection, dataFilePaths);
                TextSelection columnNames = TextSelection.create(connection, columns);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindFileStatsQuery(statement, tenantId, catalog, schema, table, snapshotId, paths, columnNames);
            try (var result = statement.executeQuery()) {
                return mapFileStatistics(result);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Read scoped file statistics failed", failure);
        }
    }

    private static void bindFileStatsQuery(PreparedStatement ps, String tenantId, String catalog,
                                           String schema, String table, String snapshotId,
                                           TextSelection paths, TextSelection columns) throws SQLException {
        int idx = 1;
        ps.setString(idx++, tenantId);
        ps.setString(idx++, catalog);
        ps.setString(idx++, schema);
        ps.setString(idx++, table);
        ps.setString(idx++, snapshotId);
        ps.setArray(idx++, paths.array());
        ps.setArray(idx, columns.array());
    }

    /** Owns one typed selection for the lifetime of its JDBC statement. */
    private record TextSelection(java.sql.Array array) implements AutoCloseable {
        static TextSelection create(Connection connection, List<String> values) throws SQLException {
            return new TextSelection(connection.createArrayOf("text", values.toArray(String[]::new)));
        }

        @Override
        public void close() throws SQLException {
            array.free();
        }
    }

    private static void requireScope(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be present for file statistics access");
        }
    }

    private static Map<String, List<FileColumnStatistics>> mapFileStatistics(
            java.sql.ResultSet rs) throws SQLException {
        Map<String, List<FileColumnStatistics>> result = new HashMap<>();
        while (rs.next()) {
            String filePath = rs.getString("data_file_path");
            var stat = new FileColumnStatistics(
                    rs.getString("column_name"),
                    rs.getString("min_value"),
                    rs.getString("max_value"),
                    rs.getLong("null_count"),
                    rs.getLong("row_count"),
                    rs.getLong("approximate_ndv"),
                    rs.getLong("total_size_bytes"));
            result.computeIfAbsent(filePath, k -> new ArrayList<>()).add(stat);
        }
        return result;
    }
}
