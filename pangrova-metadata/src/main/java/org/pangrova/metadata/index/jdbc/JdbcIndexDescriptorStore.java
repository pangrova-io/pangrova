package org.pangrova.metadata.index.jdbc;

import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.pangrova.metadata.index.IndexMetadataMapper.bindDescriptor;
import static org.pangrova.metadata.index.IndexMetadataMapper.mapDescriptor;
import static org.pangrova.metadata.index.IndexMetadataMapper.placeholders;
import static org.pangrova.metadata.index.IndexMetadataMapper.toJson;
import org.pangrova.metadata.index.IndexMetadataMapper;

/**
 * JDBC operations for {@link IndexDescriptor} CRUD, schema evolution,
 * and state queries.
 */
final class JdbcIndexDescriptorStore extends JdbcStoreSupport {

    JdbcIndexDescriptorStore(DataSource dataSource) {
        super(dataSource);
    }

    void registerIndex(IndexDescriptor descriptor) {
        purgeDroppedByName(descriptor.catalogName(), descriptor.schemaName(),
                descriptor.tableName(), descriptor.indexName());
        String sql = "INSERT INTO index_descriptors "
                + "(index_id, index_name, catalog_name, schema_name, table_name, "
                + "column_names, index_type, properties, state, stale_reason, "
                + "generation, build_snapshot_id, last_indexed_snapshot_id, created_at, updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            executeUpdate(sql, ps -> bindDescriptor(ps, descriptor));
        } catch (MetastoreException e) {
            if (isUniqueConstraintViolation(e)) {
                throw new IndexAlreadyExistsException(descriptor.indexName(), descriptor.tableName());
            }
            throw e;
        }
    }

    private void purgeDroppedByName(String catalog, String schema, String table, String name) {
        executeUpdate("DELETE FROM index_file_mappings WHERE index_id IN "
                        + "(SELECT index_id FROM index_descriptors "
                        + "WHERE catalog_name=? AND schema_name=? AND table_name=? "
                        + "AND index_name=? AND state=?)",
                ps -> {
                    ps.setString(1, catalog);
                    ps.setString(2, schema);
                    ps.setString(3, table);
                    ps.setString(4, name);
                    ps.setString(5, IndexState.DROPPED.name());
                });
        executeUpdate("DELETE FROM index_descriptors "
                        + "WHERE catalog_name=? AND schema_name=? AND table_name=? "
                        + "AND index_name=? AND state=?",
                ps -> {
                    ps.setString(1, catalog);
                    ps.setString(2, schema);
                    ps.setString(3, table);
                    ps.setString(4, name);
                    ps.setString(5, IndexState.DROPPED.name());
                });
    }

    void updateIndexState(String indexId, String newState, Instant timestamp) {
        executeUpdate("UPDATE index_descriptors SET state=?, updated_at=? WHERE index_id=?",
                ps -> {
                    ps.setString(1, newState);
                    ps.setTimestamp(2, Timestamp.from(timestamp));
                    ps.setString(3, indexId);
                });
    }

    void updateIndexState(String indexId, String newState, Instant timestamp, String staleReason) {
        executeUpdate("UPDATE index_descriptors SET state=?, updated_at=?, stale_reason=? WHERE index_id=?",
                ps -> {
                    ps.setString(1, newState);
                    ps.setTimestamp(2, Timestamp.from(timestamp));
                    ps.setString(3, "STALE".equals(newState) ? staleReason : null);
                    ps.setString(4, indexId);
                });
    }

    boolean updateIndexState(String indexId, String newState,
                             Instant timestamp, Instant expectedUpdatedAt) {
        String sql = "UPDATE index_descriptors SET state=?, updated_at=? "
                + "WHERE index_id=? AND updated_at=?";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newState);
            ps.setTimestamp(2, Timestamp.from(timestamp));
            ps.setString(3, indexId);
            ps.setTimestamp(4, Timestamp.from(expectedUpdatedAt));
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new MetastoreException("Failed to update index state with optimistic lock", e);
        }
    }

    boolean updateIndexState(String indexId, String newState,
                             Instant timestamp, Instant expectedUpdatedAt,
                             String expectedState, int expectedGeneration) {
        String sql = "UPDATE index_descriptors SET state=?, updated_at=? "
                + "WHERE index_id=? AND updated_at=? AND state=? AND generation=?";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newState);
            ps.setTimestamp(2, Timestamp.from(timestamp));
            ps.setString(3, indexId);
            ps.setTimestamp(4, Timestamp.from(expectedUpdatedAt));
            ps.setString(5, expectedState);
            ps.setInt(6, expectedGeneration);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to update index state with state+generation fencing", e);
        }
    }

    Optional<IndexDescriptor> findIndex(String indexId) {
        String sql = "SELECT * FROM index_descriptors WHERE index_id=?";
        return executeQuery(sql, ps -> ps.setString(1, indexId),
                rs -> rs.next() ? Optional.of(mapDescriptor(rs)) : Optional.empty());
    }

    List<IndexDescriptor> findIndexesByTable(String catalog, String schema, String table) {
        String sql = "SELECT * FROM index_descriptors "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        }, IndexMetadataMapper::mapDescriptorList);
    }

    List<IndexDescriptor> findReadyIndexesForColumns(
            String catalog, String schema, String table,
            List<String> columns, boolean includeStale) {
        List<String> states = includeStale ? List.of("READY", "STALE") : List.of("READY");
        String sql = buildColumnIndexSql(states.size(), columns.size());

        List<IndexDescriptor> results = executeQuery(sql, ps -> {
            int idx = 1;
            ps.setString(idx++, catalog);
            ps.setString(idx++, schema);
            ps.setString(idx++, table);
            for (String state : states) {
                ps.setString(idx++, state);
            }
            for (String col : columns) {
                ps.setString(idx++, "%\"" + escapeLikePattern(col) + "\"%");
            }
        }, IndexMetadataMapper::mapDescriptorList);

        return results.stream()
                .filter(idx -> idx.columnNames().stream().anyMatch(columns::contains))
                .toList();
    }

    private static String buildColumnIndexSql(int stateCount, int columnCount) {
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM index_descriptors "
                        + "WHERE catalog_name=? AND schema_name=? AND table_name=? "
                        + "AND state IN (" + placeholders(stateCount) + ")");
        if (columnCount > 0) {
            sql.append(" AND (");
            for (int i = 0; i < columnCount; i++) {
                if (i > 0) {
                    sql.append(" OR ");
                }
                sql.append("column_names LIKE ? ESCAPE '\\'");
            }
            sql.append(")");
        }
        return sql.toString();
    }

    void dropIndex(String indexId, Instant timestamp) {
        updateIndexState(indexId, IndexState.DROPPED.name(), timestamp);
    }

    void purgeIndex(String indexId) {
        executeUpdate("DELETE FROM index_file_mappings WHERE index_id=?",
                ps -> ps.setString(1, indexId));
        executeUpdate("DELETE FROM index_descriptors WHERE index_id=?",
                ps -> ps.setString(1, indexId));
    }

    // --- Schema evolution ---

    List<IndexDescriptor> findIndexesByColumn(
            String catalog, String schema, String table, String column) {
        return findIndexesByTable(catalog, schema, table).stream()
                .filter(idx -> idx.columnNames().contains(column))
                .toList();
    }

    void renameColumnInIndexes(
            String catalog, String schema, String table,
            String oldColumn, String newColumn) {
        List<IndexDescriptor> indexes = findIndexesByColumn(catalog, schema, table, oldColumn);
        for (IndexDescriptor idx : indexes) {
            List<String> updatedColumns = idx.columnNames().stream()
                    .map(c -> c.equals(oldColumn) ? newColumn : c)
                    .toList();
            executeUpdate("UPDATE index_descriptors SET column_names=?, updated_at=? WHERE index_id=?",
                    ps -> {
                        ps.setString(1, toJson(updatedColumns));
                        ps.setTimestamp(2, Timestamp.from(Instant.now()));
                        ps.setString(3, idx.indexId());
                    });
        }
    }

    void dropAllIndexesForTable(
            String catalog, String schema, String table, Instant timestamp) {
        executeUpdate("UPDATE index_descriptors SET state=?, updated_at=? "
                        + "WHERE catalog_name=? AND schema_name=? AND table_name=? AND state<>?",
                ps -> {
                    ps.setString(1, IndexState.DROPPED.name());
                    ps.setTimestamp(2, Timestamp.from(timestamp));
                    ps.setString(3, catalog);
                    ps.setString(4, schema);
                    ps.setString(5, table);
                    ps.setString(6, IndexState.DROPPED.name());
                });
    }

    void renameTable(String catalog, String schema, String oldTable, String newTable) {
        executeUpdate("UPDATE index_descriptors SET table_name=?, updated_at=? "
                        + "WHERE catalog_name=? AND schema_name=? AND table_name=?",
                ps -> {
                    ps.setString(1, newTable);
                    ps.setTimestamp(2, Timestamp.from(Instant.now()));
                    ps.setString(3, catalog);
                    ps.setString(4, schema);
                    ps.setString(5, oldTable);
                });
    }

    // --- State queries ---

    List<IndexDescriptor> findIndexesByState(IndexState state) {
        String sql = "SELECT * FROM index_descriptors WHERE state=?";
        return executeQuery(sql, ps -> ps.setString(1, state.name()),
                IndexMetadataMapper::mapDescriptorList);
    }

    Map<String, IndexDescriptor> findIndexesByIds(List<String> indexIds) {
        if (indexIds.isEmpty()) {
            return Map.of();
        }
        String ph = placeholders(indexIds.size());
        String sql = "SELECT * FROM index_descriptors WHERE index_id IN (" + ph + ")";
        return executeQuery(sql, ps -> {
            int idx = 1;
            for (String id : indexIds) {
                ps.setString(idx++, id);
            }
        }, rs -> {
            Map<String, IndexDescriptor> result = new HashMap<>();
            while (rs.next()) {
                IndexDescriptor d = mapDescriptor(rs);
                result.put(d.indexId(), d);
            }
            return result;
        });
    }

    void updateLastIndexedSnapshotId(String indexId, String snapshotId) {
        executeUpdate("UPDATE index_descriptors SET last_indexed_snapshot_id=?, updated_at=? "
                        + "WHERE index_id=?",
                ps -> {
                    ps.setString(1, snapshotId);
                    ps.setTimestamp(2, Timestamp.from(Instant.now()));
                    ps.setString(3, indexId);
                });
    }

    List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
        String sql = "SELECT * FROM index_descriptors WHERE state=? ORDER BY updated_at OFFSET ? LIMIT ?";
        return executeQuery(sql, ps -> {
            ps.setString(1, state.name());
            ps.setInt(2, offset);
            ps.setInt(3, limit);
        }, IndexMetadataMapper::mapDescriptorList);
    }

    private static String escapeLikePattern(String input) {
        return input.replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_");
    }
}
