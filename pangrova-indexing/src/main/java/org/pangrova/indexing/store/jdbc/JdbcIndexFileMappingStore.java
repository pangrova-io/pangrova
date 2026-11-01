package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.IndexStoreException;
import org.pangrova.indexing.api.IndexFileMapping;
import org.pangrova.indexing.api.IndexFileSelection;
import org.pangrova.spi.index.IndexState;

import org.pangrova.security.authz.DeferredAuthorizationGuard;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.indexing.control.IndexMetadataMapper.bindMapping;
import static org.pangrova.indexing.control.IndexMetadataMapper.placeholders;
import org.pangrova.indexing.control.IndexMetadataMapper;

/**
 * JDBC operations for {@link IndexFileMapping} CRUD and queries.
 */
final class JdbcIndexFileMappingStore extends JdbcStoreSupport {

    JdbcIndexFileMappingStore(DataSource dataSource, DeferredAuthorizationGuard authorizationGuard) {
        super(dataSource, authorizationGuard);
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

    long copyIndexGenerationMappings(
            String indexId,
            int sourceGeneration,
            int targetGeneration,
            List<String> excludedDataFiles) {
        if (targetGeneration != Math.addExact(sourceGeneration, 1)) {
            throw new IllegalArgumentException(
                    "targetGeneration must advance exactly one generation");
        }
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                copyMappings(connection, indexId, sourceGeneration, targetGeneration);
                deleteExcludedMappings(
                        connection, indexId, targetGeneration,
                        excludedDataFiles == null ? List.of() : excludedDataFiles);
                long copied = countMappings(connection, indexId, targetGeneration);
                connection.commit();
                return copied;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IndexStoreException("Copy index generation mappings failed", failure);
        }
    }

    private static void copyMappings(
            Connection connection, String indexId,
            int sourceGeneration, int targetGeneration) throws SQLException {
        String sql = "INSERT INTO index_file_mappings "
                + "(index_id,data_file_path,index_file_path,row_count,index_size_bytes,"
                + "checksum,generation,snapshot_token,sequence_num,last_verified_at,created_at) "
                + "SELECT index_id,data_file_path,index_file_path,row_count,index_size_bytes,"
                + "checksum,?,snapshot_token,sequence_num,last_verified_at,created_at "
                + "FROM index_file_mappings WHERE index_id=? AND generation=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, targetGeneration);
            statement.setString(2, indexId);
            statement.setInt(3, sourceGeneration);
            statement.executeUpdate();
        }
    }

    private static void deleteExcludedMappings(
            Connection connection, String indexId, int generation,
            List<String> excludedDataFiles) throws SQLException {
        for (int offset = 0; offset < excludedDataFiles.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = excludedDataFiles.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, excludedDataFiles.size()));
            String sql = "DELETE FROM index_file_mappings WHERE index_id=? AND generation=? "
                    + "AND data_file_path IN (" + placeholders(batch.size()) + ")";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, indexId);
                statement.setInt(2, generation);
                int parameter = 3;
                for (String path : batch) {
                    statement.setString(parameter++, path);
                }
                statement.executeUpdate();
            }
        }
    }

    private static long countMappings(
            Connection connection, String indexId, int generation) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COUNT(*) FROM index_file_mappings WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (java.sql.ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
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
            throw new IndexStoreException("Batch register index files failed", e);
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

    Map<String, Map<String, IndexFileMapping>> batchGetIndexArtifacts(
            Map<String, IndexFileSelection> selections,
            List<String> dataFilePaths) {
        Map<String, Map<String, IndexFileMapping>> result = new HashMap<>();
        if (selections.isEmpty() || dataFilePaths.isEmpty()) {
            return result;
        }
        List<String> indexIds = List.copyOf(selections.keySet());
        for (int offset = 0; offset < dataFilePaths.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = dataFilePaths.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, dataFilePaths.size()));
            String sql = "SELECT * FROM index_file_mappings WHERE index_id IN ("
                    + placeholders(indexIds.size()) + ") AND data_file_path IN ("
                    + placeholders(batch.size()) + ")";
            executeQuery(sql,
                    ps -> bindMappingBatchParams(ps, indexIds, batch, null),
                    rs -> collectArtifactResults(rs, selections, result));
        }
        return result;
    }

    private static Void collectArtifactResults(
            java.sql.ResultSet rs,
            Map<String, IndexFileSelection> selections,
            Map<String, Map<String, IndexFileMapping>> result) throws SQLException {
        while (rs.next()) {
            IndexFileMapping mapping = IndexMetadataMapper.mapMapping(rs);
            IndexFileSelection selection = selections.get(mapping.indexId());
            if (selection == null || !selection.matches(mapping)) {
                continue;
            }
            IndexFileMapping previous = result
                    .computeIfAbsent(mapping.indexId(), ignored -> new HashMap<>())
                    .put(mapping.dataFilePath(), mapping);
            if (previous != null) {
                throw new IndexStoreException(
                        "Duplicate index artifact for generation: indexId="
                                + mapping.indexId() + " dataFile="
                                + mapping.dataFilePath());
            }
        }
        return null;
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
                + (hasSnapshot ? " AND snapshot_token = ?" : "");
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
            ps.setString(idx, snapshotToken);
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

    boolean removeIndexFileMappingIfState(IndexFileMapping mapping,
                                           IndexState expectedState,
                                           int expectedGeneration) {
        String sql = """
                DELETE FROM index_file_mappings
                WHERE index_id = ? AND data_file_path = ?
                AND index_file_path = ? AND generation = ?
                AND index_id IN (
                    SELECT index_id FROM index_descriptors
                    WHERE index_id = ? AND state = ? AND generation = ?
                )
                """;
        int affected = executeUpdateCount(sql, ps -> {
            ps.setString(1, mapping.indexId());
            ps.setString(2, mapping.dataFilePath());
            ps.setString(3, mapping.indexFilePath());
            ps.setInt(4, mapping.generation());
            ps.setString(5, mapping.indexId());
            ps.setString(6, expectedState.name());
            ps.setInt(7, expectedGeneration);
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
