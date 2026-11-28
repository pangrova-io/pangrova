package org.pangrova.persistence.table.postgresql.statistics;

import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.table.api.exception.TableControlException;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.statistics.TableControlStatistics;
import org.pangrova.table.api.statistics.TableControlStatisticsStore;
import org.pangrova.table.api.statistics.TableStatisticsSnapshotStore;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.persistence.jdbc.operation.JdbcOperations;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.setJson;

/**
 * Durable ANALYZE statistics keyed by the stable {@code table_id}. A peer coordinator
 * sees the same numbers, and a drop/recreate cannot inherit the old table's row.
 */
@ThreadSafe
public final class JdbcTableControlStatisticsStore
        implements TableControlStatisticsStore, TableStatisticsSnapshotStore {

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

    private static final String OBSERVE_FOR_PLANNING = """
            SELECT target.target_ordinal, catalog.incarnation_id AS observed_incarnation,
                   catalog.revision AS observed_revision, stats.table_id, stats.stats_version, stats.version_token,
                   stats.row_count, stats.data_size, stats.source_work_units, stats.column_stats, stats.source,
                   stats.computed_at
            FROM unnest(CAST(? AS uuid[]), CAST(? AS text[]), CAST(? AS text[]),
                        CAST(? AS text[]), CAST(? AS bigint[])) WITH ORDINALITY
                AS target(table_id, version_token, catalog_name, incarnation_id, revision, target_ordinal)
            LEFT JOIN catalog_definitions catalog ON catalog.name=target.catalog_name
            LEFT JOIN table_control_statistics stats ON stats.table_id=target.table_id
                AND stats.version_token=target.version_token
                AND catalog.incarnation_id=target.incarnation_id AND catalog.revision=target.revision
            """;

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
            throw new TableControlException(
                    "Failed to load statistics for tableId=" + tableId, e);
        }
    }

    @Override
    public Map<TableControlRecord, Observation> observeForPlanning(Set<TableControlRecord> owners) {
        if (owners.isEmpty()) return Map.of();
        var ordered = List.copyOf(owners);
        var typedValues = new Object[][] {
            ordered.stream().map(TableControlRecord::tableId).toArray(UUID[]::new),
            ordered.stream().map(TableControlRecord::versionToken).toArray(String[]::new),
            ordered.stream().map(TableControlRecord::catalog).toArray(String[]::new),
            ordered.stream().map(TableControlRecord::catalogIncarnationId).toArray(String[]::new),
            ordered.stream().map(TableControlRecord::catalogRevision).toArray(Long[]::new)
        };
        var types = List.of("uuid", "text", "text", "text", "bigint");
        try (var connection = dataSource.getConnection();
                var query = connection.prepareStatement(OBSERVE_FOR_PLANNING)) {
            var arguments = new Array[typedValues.length];
            try {
                for (int index = 0; index < arguments.length; index++) {
                    arguments[index] = connection.createArrayOf(types.get(index), typedValues[index]);
                    query.setArray(index + 1, arguments[index]);
                }
                try (var rows = query.executeQuery()) {
                    return readStatisticsObservations(ordered, rows);
                }
            } finally {
                for (var argument : arguments) {
                    if (argument != null) argument.free();
                }
            }
        } catch (SQLException e) {
            throw new TableControlException("Failed to load planning statistics and catalog deployments", e);
        }
    }

    private static Map<TableControlRecord, Observation> readStatisticsObservations(
            List<TableControlRecord> owners, ResultSet rows) throws SQLException {
        Map<TableControlRecord, Observation> observations = new LinkedHashMap<>();
        while (rows.next()) {
            int ordinal = rows.getInt("target_ordinal") - 1;
            if (ordinal < 0 || ordinal >= owners.size()) {
                throw new SQLException("Unexpected planning statistics ordinal");
            }
            var deployment = Optional.ofNullable(rows.getString("observed_incarnation"));
            Optional<CatalogDeploymentIdentity> identity = deployment.isEmpty() ? Optional.empty()
                    : Optional.of(new CatalogDeploymentIdentity(
                            deployment.orElseThrow(), rows.getLong("observed_revision")));
            var statistics = rows.getObject("table_id", UUID.class) == null ? Optional.<TableControlStatistics>empty()
                    : Optional.of(mapRow(rows));
            observations.put(owners.get(ordinal), new Observation(identity, statistics));
        }
        if (observations.size() != owners.size()) {
            throw new SQLException("Incomplete planning statistics observations");
        }
        return Map.copyOf(observations);
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
            JdbcOperations.setInstant(statement, 9, statistics.computedAt());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new TableControlException(
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
            throw new TableControlException(
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
                JdbcOperations.getInstant(row, "computed_at"));
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
            return CanonicalJsonMapper.writeValueAsString(encoded);
        } catch (JsonProcessingException e) {
            throw new TableControlException("Failed to serialize column statistics", e);
        }
    }

    private static Map<String, TableStatistics.ColumnStatistics> parseColumnStats(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        Map<String, Map<String, Object>> decoded;
        try {
            decoded = CanonicalJsonMapper.readValue(json,
                    new com.fasterxml.jackson.core.type.TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new TableControlException("Failed to deserialize column statistics", e);
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
