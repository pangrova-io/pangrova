package org.pangrova.persistence.indexing.postgresql.metadata;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexFileSelection;
import org.pangrova.indexing.api.enums.IndexState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.bindMapping;

/**
 * JDBC operations for {@link IndexFileMapping} CRUD and queries.
 */
final class JdbcIndexFileMappingStore extends JdbcStoreSupport {

    JdbcIndexFileMappingStore(DataSource dataSource) {
        super(dataSource);
    }

    void batchRegisterIndexFiles(String attemptOwner, List<IndexFileMapping> mappings) {
        List<IndexFileMapping> batch = List.copyOf(mappings);
        if (batch.isEmpty()) {
            return;
        }
        validateBatch(batch);
        executeBatchInsert(attemptOwner, batch);
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
                JdbcIndexMappingWriteFence.requireBuildable(connection, indexId, targetGeneration);
                JdbcIndexMappingWriteFence.requirePublishedSource(connection, indexId, sourceGeneration);
                copyMappings(connection, indexId, sourceGeneration, targetGeneration);
                deleteExcludedMappings(
                        connection, indexId, targetGeneration,
                        excludedDataFiles == null ? List.of() : excludedDataFiles);
                long copied = countMappings(connection, indexId, targetGeneration);
                connection.commit();
                return copied;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Copy index generation mappings failed", failure);
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
            try (var paths = JdbcTextSelection.create(connection, batch);
                    var statement = connection.prepareStatement(
                            "DELETE FROM index_file_mappings WHERE index_id=? AND generation=? "
                                    + "AND data_file_path = ANY (?)")) {
                statement.setString(1, indexId);
                statement.setInt(2, generation);
                statement.setArray(3, paths.array());
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

    private static void validateBatch(List<IndexFileMapping> mappings) {
        String expectedIndexId = mappings.get(0).indexId();
        int expectedGeneration = mappings.get(0).generation();
        for (IndexFileMapping m : mappings) {
            if (!m.indexId().equals(expectedIndexId) || m.generation() != expectedGeneration) {
                throw new IllegalArgumentException(
                        "All mappings must belong to the same index and generation, found '"
                                + expectedIndexId + "' and '" + m.indexId() + "'");
            }
        }
    }

    private void executeBatchInsert(String attemptOwner, List<IndexFileMapping> mappings) {
        String sql = "INSERT INTO index_file_mappings "
                + "(index_id, data_file_path, index_file_path, row_count, index_size_bytes, "
                + "checksum, generation, snapshot_token, sequence_num, last_verified_at, created_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                IndexFileMapping first = mappings.get(0);
                JdbcIndexMappingWriteFence.requireBuildable(connection, first.indexId(), first.generation());
                JdbcIndexMappingWriteFence.requireCurrentAttempt(
                        connection, first.indexId(), first.generation(), attemptOwner);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (IndexFileMapping mapping : mappings) {
                        bindMapping(statement, mapping);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Batch register index files failed", failure);
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
        if (selections.isEmpty() || dataFilePaths.isEmpty()) {
            return Map.of();
        }
        Map<String, Map<String, IndexFileMapping>> result = new HashMap<>();
        List<String> distinctPaths = dataFilePaths.stream().distinct().toList();
        try (Connection connection = dataSource().getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try {
                try (var ids = JdbcTextSelection.create(connection, List.copyOf(selections.keySet()));
                        var statement = connection.prepareStatement("""
                                SELECT m.*, g.source_snapshot
                                FROM index_file_mappings m
                                JOIN index_generation_manifests g USING (index_id, generation)
                                WHERE g.state = 'ACTIVE' AND m.index_id = ANY (?)
                                  AND m.data_file_path = ANY (?)
                                """)) {
                    statement.setArray(1, ids.array());
                    for (int offset = 0; offset < distinctPaths.size(); offset += BATCH_PARTITION_SIZE) {
                        int end = Math.min(offset + BATCH_PARTITION_SIZE, distinctPaths.size());
                        try (var paths = JdbcTextSelection.create(connection, distinctPaths.subList(offset, end))) {
                            statement.setArray(2, paths.array());
                            try (var rows = statement.executeQuery()) {
                                collectArtifactResults(rows, selections, result);
                            }
                        }
                    }
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Index artifact selection failed", failure);
        }
        result.replaceAll((id, files) -> Map.copyOf(files));
        return Map.copyOf(result);
    }

    private static void collectArtifactResults(
            java.sql.ResultSet rows,
            Map<String, IndexFileSelection> selections,
            Map<String, Map<String, IndexFileMapping>> result) throws SQLException {
        while (rows.next()) {
            String indexId = rows.getString("index_id");
            IndexFileSelection selection = selections.get(indexId);
            if (!selection.matchesPublication(rows.getInt("generation"), rows.getString("source_snapshot"))) {
                continue;
            }
            IndexFileMapping mapping = IndexMetadataMapper.mapMapping(rows);
            result.computeIfAbsent(indexId, ignored -> new HashMap<>()).put(mapping.dataFilePath(), mapping);
        }
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
        try (var connection = dataSource().getConnection();
                var ids = JdbcTextSelection.create(connection, indexIds);
                var paths = JdbcTextSelection.create(connection, batch);
                var statement = connection.prepareStatement("""
                        SELECT index_id, data_file_path, index_file_path FROM index_file_mappings
                        WHERE index_id = ANY (?) AND data_file_path = ANY (?)
                        AND (? OR snapshot_token = ?)
                        """)) {
            statement.setArray(1, ids.array());
            statement.setArray(2, paths.array());
            statement.setBoolean(3, snapshotToken == null);
            statement.setString(4, snapshotToken);
            try (var rows = statement.executeQuery()) {
                collectMappingResults(rows, result);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Index mapping selection failed", failure);
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
        try (var connection = dataSource().getConnection();
                var ids = JdbcTextSelection.create(connection, indexIds);
                var paths = JdbcTextSelection.create(connection, batch);
                var statement = connection.prepareStatement("""
                        SELECT index_id, COUNT(*) AS cnt FROM index_file_mappings
                        WHERE index_id = ANY (?) AND data_file_path = ANY (?) GROUP BY index_id
                        """)) {
            statement.setArray(1, ids.array());
            statement.setArray(2, paths.array());
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.merge(rows.getString("index_id"), rows.getInt("cnt"), Integer::sum);
                }
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Index mapping count failed", failure);
        }
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
        try (var connection = dataSource().getConnection();
                var paths = JdbcTextSelection.create(connection, batch);
                var statement = connection.prepareStatement(
                        "SELECT data_file_path FROM index_file_mappings "
                                + "WHERE index_id=? AND data_file_path = ANY (?)")) {
            statement.setString(1, indexId);
            statement.setArray(2, paths.array());
            try (var rows = statement.executeQuery()) {
                List<String> found = new ArrayList<>();
                while (rows.next()) {
                    found.add(rows.getString("data_file_path"));
                }
                return found;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Indexed file selection failed", failure);
        }
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
