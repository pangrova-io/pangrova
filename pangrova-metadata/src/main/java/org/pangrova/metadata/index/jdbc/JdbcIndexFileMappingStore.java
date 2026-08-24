package org.pangrova.metadata.index.jdbc;

import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.spi.index.IndexState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.metadata.index.IndexMetadataMapper.bindMapping;
import static org.pangrova.metadata.index.IndexMetadataMapper.placeholders;
import org.pangrova.metadata.index.IndexMetadataMapper;

/**
 * JDBC operations for {@link IndexFileMapping} CRUD and queries.
 */
final class JdbcIndexFileMappingStore extends JdbcStoreSupport {

    JdbcIndexFileMappingStore(DataSource dataSource) {
        super(dataSource);
    }

    void registerIndexFile(IndexFileMapping mapping) {
        String sql = "INSERT INTO index_file_mappings "
                + "(index_id, data_file_path, index_file_path, row_count, index_size_bytes, "
                + "checksum, generation, snapshot_token, sequence_num, last_verified_at, created_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        executeUpdate(sql, ps -> bindMapping(ps, mapping));
    }

    void batchRegisterIndexFiles(List<IndexFileMapping> mappings) {
        if (mappings.isEmpty()) {
            return;
        }
        validateSameIndex(mappings);
        executeBatchInsert(mappings);
    }

    private static void validateSameIndex(List<IndexFileMapping> mappings) {
        String expectedIndexId = mappings.get(0).indexId();
        for (IndexFileMapping m : mappings) {
            if (!m.indexId().equals(expectedIndexId)) {
                throw new IllegalArgumentException(
                        "All mappings must belong to same indexId, found '"
                                + expectedIndexId + "' and '" + m.indexId() + "'");
            }
        }
    }

    private void executeBatchInsert(List<IndexFileMapping> mappings) {
        String sql = "INSERT INTO index_file_mappings "
                + "(index_id, data_file_path, index_file_path, row_count, index_size_bytes, "
                + "checksum, generation, snapshot_token, sequence_num, last_verified_at, created_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IndexFileMapping m : mappings) {
                bindMapping(ps, m);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new MetastoreException("Batch register index files failed", e);
        }
    }

    List<IndexFileMapping> getIndexFileMappings(String indexId) {
        String sql = "SELECT * FROM index_file_mappings WHERE index_id=?";
        return executeQuery(sql, ps -> ps.setString(1, indexId),
                IndexMetadataMapper::mapMappingList);
    }

    Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths) {
        Map<String, Map<String, String>> result = new HashMap<>();
        if (indexIds.isEmpty() || dataFilePaths.isEmpty()) {
            return result;
        }
        for (int offset = 0; offset < dataFilePaths.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = dataFilePaths.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, dataFilePaths.size()));
            queryMappingBatch(indexIds, batch, null, result);
        }
        return result;
    }

    Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths, String querySnapshotToken) {
        if (querySnapshotToken == null) {
            return batchGetIndexFileMappings(indexIds, dataFilePaths);
        }
        Map<String, Map<String, String>> result = new HashMap<>();
        if (indexIds.isEmpty() || dataFilePaths.isEmpty()) {
            return result;
        }
        for (int offset = 0; offset < dataFilePaths.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = dataFilePaths.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, dataFilePaths.size()));
            queryMappingBatch(indexIds, batch, querySnapshotToken, result);
        }
        return result;
    }

    private void queryMappingBatch(List<String> indexIds, List<String> batch,
                                   String snapshotToken,
                                   Map<String, Map<String, String>> result) {
        String sql = buildMappingBatchSql(indexIds.size(), batch.size(), snapshotToken != null);
        executeQuery(sql,
                ps -> bindMappingBatchParams(ps, indexIds, batch, snapshotToken),
                rs -> collectMappingResults(rs, result));
    }

    private String buildMappingBatchSql(int idCount, int pathCount, boolean hasSnapshot) {
        String idPh = placeholders(idCount);
        String pathPh = placeholders(pathCount);
        return "SELECT index_id, data_file_path, index_file_path "
                + "FROM index_file_mappings "
                + "WHERE index_id IN (" + idPh + ") "
                + "AND data_file_path IN (" + pathPh + ")"
                + (hasSnapshot ? " AND (snapshot_token IS NULL OR snapshot_token <= ?)" : "");
    }

    private static void bindMappingBatchParams(PreparedStatement ps, List<String> indexIds,
                                               List<String> batch, String snapshotToken) throws SQLException {
        int idx = 1;
        for (String id : indexIds) {
            ps.setString(idx++, id);
        }
        for (String path : batch) {
            ps.setString(idx++, path);
        }
        if (snapshotToken != null) {
            ps.setLong(idx, Long.parseLong(snapshotToken));
        }
    }

    private static Void collectMappingResults(java.sql.ResultSet rs,
                                              Map<String, Map<String, String>> result) throws SQLException {
        while (rs.next()) {
            result.computeIfAbsent(rs.getString("index_id"), k -> new HashMap<>())
                    .put(rs.getString("data_file_path"), rs.getString("index_file_path"));
        }
        return null;
    }

    void removeIndexFileMapping(String indexId, String dataFilePath) {
        executeUpdate("DELETE FROM index_file_mappings WHERE index_id=? AND data_file_path=?",
                ps -> {
                    ps.setString(1, indexId);
                    ps.setString(2, dataFilePath);
                });
    }

    boolean removeIndexFileMappingIfState(String indexId, String dataFilePath,
                                           IndexState expectedState) {
        String sql = """
                DELETE FROM index_file_mappings
                WHERE index_id = ? AND data_file_path = ?
                AND index_id IN (
                    SELECT index_id FROM index_descriptors
                    WHERE index_id = ? AND state = ?
                )
                """;
        int affected = executeUpdateCount(sql, ps -> {
            ps.setString(1, indexId);
            ps.setString(2, dataFilePath);
            ps.setString(3, indexId);
            ps.setString(4, expectedState.name());
        });
        return affected > 0;
    }

    Map<String, Integer> getIndexFileMappingCounts(
            List<String> indexIds, List<String> dataFilePaths) {
        Map<String, Integer> result = new HashMap<>();
        if (indexIds.isEmpty() || dataFilePaths.isEmpty()) {
            return result;
        }
        for (int offset = 0; offset < dataFilePaths.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = dataFilePaths.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, dataFilePaths.size()));
            queryMappingCounts(indexIds, batch, result);
        }
        return result;
    }

    private void queryMappingCounts(List<String> indexIds, List<String> batch,
                                    Map<String, Integer> result) {
        String sql = "SELECT index_id, COUNT(*) AS cnt "
                + "FROM index_file_mappings "
                + "WHERE index_id IN (" + placeholders(indexIds.size()) + ") "
                + "AND data_file_path IN (" + placeholders(batch.size()) + ") "
                + "GROUP BY index_id";
        executeQuery(sql,
                ps -> bindMappingBatchParams(ps, indexIds, batch, null),
                rs -> {
                    while (rs.next()) {
                        result.merge(rs.getString("index_id"), rs.getInt("cnt"), Integer::sum);
                    }
                    return null;
                });
    }

    List<String> filterUnindexedFiles(String indexId, List<String> dataFilePaths) {
        if (dataFilePaths.isEmpty()) {
            return List.of();
        }
        List<String> unindexed = new ArrayList<>(dataFilePaths);
        for (int offset = 0; offset < dataFilePaths.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = dataFilePaths.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, dataFilePaths.size()));
            unindexed.removeAll(queryIndexedFiles(indexId, batch));
        }
        return unindexed;
    }

    private List<String> queryIndexedFiles(String indexId, List<String> batch) {
        String sql = "SELECT data_file_path FROM index_file_mappings "
                + "WHERE index_id=? AND data_file_path IN (" + placeholders(batch.size()) + ")";
        return executeQuery(sql, ps -> {
            ps.setString(1, indexId);
            int idx = 2;
            for (String path : batch) {
                ps.setString(idx++, path);
            }
        }, rs -> {
            List<String> found = new ArrayList<>();
            while (rs.next()) {
                found.add(rs.getString("data_file_path"));
            }
            return found;
        });
    }

    void updateLastVerifiedAt(String indexId, String dataFilePath, long timestamp) {
        executeUpdate("UPDATE index_file_mappings SET last_verified_at=? "
                        + "WHERE index_id=? AND data_file_path=?",
                ps -> {
                    ps.setLong(1, timestamp);
                    ps.setString(2, indexId);
                    ps.setString(3, dataFilePath);
                });
    }

    void deleteFileMappingsByGeneration(String indexId, int maxGeneration) {
        executeUpdate("DELETE FROM index_file_mappings WHERE index_id=? AND generation<=?",
                ps -> {
                    ps.setString(1, indexId);
                    ps.setInt(2, maxGeneration);
                });
    }
}
