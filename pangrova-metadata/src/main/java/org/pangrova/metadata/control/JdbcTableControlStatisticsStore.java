package org.pangrova.metadata.control;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.store.JdbcSupport;
import org.pangrova.metadata.store.MetadataJsonMapper;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.metadata.store.JdbcSupport.setJson;

/**
 * Durable ANALYZE statistics keyed by the stable {@code table_id}. A peer coordinator
 * sees the same numbers, and a drop/recreate cannot inherit the old table's row.
 */
@ThreadSafe
public final class JdbcTableControlStatisticsStore implements TableControlStatisticsStore {

    private static final String UPSERT =
            "INSERT INTO table_control_statistics (table_id, stats_version, version_token, "
                    + "row_count, data_size, source_work_units, column_stats, source, computed_at) "
                    + "VALUES (?,?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (table_id) DO UPDATE SET stats_version=EXCLUDED.stats_version, "
                    + "version_token=EXCLUDED.version_token, row_count=EXCLUDED.row_count, "
                    + "data_size=EXCLUDED.data_size, source_work_units=EXCLUDED.source_work_units, "
                    + "column_stats=EXCLUDED.column_stats, source=EXCLUDED.source, "
                    + "computed_at=EXCLUDED.computed_at";

    private static final String FIND =
            "SELECT table_id, stats_version, version_token, row_count, data_size, "
                    + "source_work_units, column_stats, source, computed_at "
                    + "FROM table_control_statistics WHERE table_id=?";

    private static final String DELETE =
            "DELETE FROM table_control_statistics WHERE table_id=?";

    private final DataSource dataSource;

    public JdbcTableControlStatisticsStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public Optional<TableControlStatistics> find(UUID tableId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(FIND)) {
            statement.setObject(1, tableId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(mapRow(rows)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to load statistics for tableId=" + tableId, e);
        }
    }

    @Override
    public void save(TableControlStatistics statistics) {
        Objects.requireNonNull(statistics, "statistics must not be null");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPSERT)) {
            TableStatistics values = statistics.statistics();
            statement.setObject(1, statistics.tableId());
            statement.setLong(2, statistics.statsVersion());
            statement.setString(3, statistics.versionToken());
            statement.setLong(4, values.rowCount());
            statement.setLong(5, values.dataSize());
            statement.setLong(6, values.sourceWorkUnits());
            setJson(statement, 7, serializeColumnStats(values.columnStats()));
            statement.setString(8, statistics.source());
            JdbcSupport.setInstant(statement, 9, statistics.computedAt());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to save statistics for tableId=" + statistics.tableId(), e);
        }
    }

    @Override
    public void delete(UUID tableId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE)) {
            statement.setObject(1, tableId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to delete statistics for tableId=" + tableId, e);
        }
    }

    private static TableControlStatistics mapRow(ResultSet row) throws SQLException {
        return new TableControlStatistics(
                row.getObject("table_id", UUID.class),
                row.getLong("stats_version"),
                row.getString("version_token"),
                new TableStatistics(
                        row.getLong("row_count"),
                        row.getLong("data_size"),
                        row.getLong("source_work_units"),
                        parseColumnStats(row.getString("column_stats"))),
                row.getString("source"),
                JdbcSupport.getInstant(row, "computed_at"));
    }

    private static String serializeColumnStats(
            Map<String, TableStatistics.ColumnStatistics> columnStats) {
        Map<String, Map<String, Object>> encoded = new LinkedHashMap<>();
        for (var entry : columnStats.entrySet()) {
            TableStatistics.ColumnStatistics column = entry.getValue();
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("nullCount", column.nullCount());
            fields.put("ndv", column.ndv());
            fields.put("min", column.min());
            fields.put("max", column.max());
            fields.put("avgSize", column.avgSize());
            encoded.put(entry.getKey(), fields);
        }
        try {
            return MetadataJsonMapper.writeValueAsString(encoded);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("Failed to serialize column statistics", e);
        }
    }

    private static Map<String, TableStatistics.ColumnStatistics> parseColumnStats(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        Map<String, Map<String, Object>> decoded;
        try {
            decoded = MetadataJsonMapper.readValue(json,
                    new com.fasterxml.jackson.core.type.TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new MetastoreException("Failed to deserialize column statistics", e);
        }
        Map<String, TableStatistics.ColumnStatistics> columnStats = new LinkedHashMap<>();
        for (var entry : decoded.entrySet()) {
            Map<String, Object> fields = entry.getValue();
            columnStats.put(entry.getKey(), new TableStatistics.ColumnStatistics(
                    asLong(fields.get("nullCount"), -1),
                    asLong(fields.get("ndv"), -1),
                    asComparable(fields.get("min")),
                    asComparable(fields.get("max")),
                    asDouble(fields.get("avgSize"), -1.0)));
        }
        return columnStats;
    }

    private static long asLong(Object value, long fallback) {
        return value instanceof Number number ? number.longValue() : fallback;
    }

    private static double asDouble(Object value, double fallback) {
        return value instanceof Number number ? number.doubleValue() : fallback;
    }

    private static Comparable<?> asComparable(Object value) {
        return value instanceof Comparable<?> comparable ? comparable : null;
    }
}
