package org.pangrova.metadata.index.jdbc;

import org.pangrova.metadata.exception.MetastoreException;
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
    private static final int PARAM_APPROXIMATE_NDV = 11;
    private static final int PARAM_TOTAL_SIZE_BYTES = 12;

    JdbcIndexStatisticsStore(DataSource dataSource) {
        super(dataSource);
    }

    void batchInsertFileStatistics(
            String catalog, String schema, String table,
            List<DataFileStatistics> statistics) {
        if (statistics.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO file_column_statistics "
                + "(catalog_name, schema_name, table_name, data_file_path, snapshot_id, "
                + "column_name, min_value, max_value, null_count, row_count, "
                + "approximate_ndv, total_size_bytes, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bindStatisticsBatch(ps, catalog, schema, table, statistics);
            ps.executeBatch();
        } catch (SQLException e) {
            throw new MetastoreException("Batch insert file statistics failed", e);
        }
    }

    private static void bindStatisticsBatch(PreparedStatement ps, String catalog,
                                            String schema, String table,
                                            List<DataFileStatistics> statistics) throws SQLException {
        for (var fileStat : statistics) {
            for (var colStat : fileStat.columnStatistics()) {
                ps.setString(1, catalog);
                ps.setString(2, schema);
                ps.setString(3, table);
                ps.setString(4, fileStat.dataFilePath());
                ps.setString(5, fileStat.snapshotId());
                ps.setString(6, colStat.columnName());
                ps.setString(7, colStat.minValue() != null ? colStat.minValue().toString() : null);
                ps.setString(8, colStat.maxValue() != null ? colStat.maxValue().toString() : null);
                ps.setLong(9, colStat.nullCount());
                ps.setLong(10, colStat.rowCount());
                ps.setLong(PARAM_APPROXIMATE_NDV, colStat.approximateNdv());
                ps.setLong(PARAM_TOTAL_SIZE_BYTES, colStat.totalSizeBytes());
                ps.addBatch();
            }
        }
    }

    void deleteFileStatistics(String catalog, String schema, String table,
                             List<String> dataFilePaths) {
        if (dataFilePaths.isEmpty()) {
            return;
        }
        String pathPh = placeholders(dataFilePaths.size());
        String sql = "DELETE FROM file_column_statistics "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=? "
                + "AND data_file_path IN (" + pathPh + ")";
        executeUpdate(sql, ps -> {
            int idx = 1;
            ps.setString(idx++, catalog);
            ps.setString(idx++, schema);
            ps.setString(idx++, table);
            for (String path : dataFilePaths) {
                ps.setString(idx++, path);
            }
        });
    }

    Map<String, List<FileColumnStatistics>> getFileStatistics(
            String catalog, String schema, String table,
            List<String> dataFilePaths, List<String> columns) {
        if (dataFilePaths.isEmpty() || columns.isEmpty()) {
            return Map.of();
        }
        String pathPh = placeholders(dataFilePaths.size());
        String colPh = placeholders(columns.size());
        String sql = "SELECT data_file_path, column_name, min_value, max_value, "
                + "null_count, row_count, approximate_ndv, total_size_bytes "
                + "FROM file_column_statistics "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=? "
                + "AND data_file_path IN (" + pathPh + ") "
                + "AND column_name IN (" + colPh + ")";
        return executeQuery(sql,
                ps -> bindFileStatsQuery(ps, catalog, schema, table, dataFilePaths, columns),
                JdbcIndexStatisticsStore::mapFileStatistics);
    }

    private static void bindFileStatsQuery(PreparedStatement ps, String catalog,
                                           String schema, String table,
                                           List<String> paths, List<String> columns) throws SQLException {
        int idx = 1;
        ps.setString(idx++, catalog);
        ps.setString(idx++, schema);
        ps.setString(idx++, table);
        for (String path : paths) {
            ps.setString(idx++, path);
        }
        for (String col : columns) {
            ps.setString(idx++, col);
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
