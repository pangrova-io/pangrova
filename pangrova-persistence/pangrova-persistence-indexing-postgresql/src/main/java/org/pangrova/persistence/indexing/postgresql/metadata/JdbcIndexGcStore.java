package org.pangrova.persistence.indexing.postgresql.metadata;

import javax.sql.DataSource;

import org.pangrova.persistence.indexing.api.metadata.BreakerState;
import org.pangrova.persistence.indexing.api.metadata.CacheEvent;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexCleanup.OrphanRemoval;
import org.pangrova.indexing.api.enums.IndexState;

import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.SQLException;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * JDBC operations for GC-related queries: orphan marking, stale file mappings,
 * cache events, and circuit breaker state.
 */
final class JdbcIndexGcStore extends JdbcStoreSupport {

    JdbcIndexGcStore(DataSource dataSource) {
        super(dataSource);
    }

    // --- Orphan marking ---

    void markOrphanedByDataFiles(String indexId, List<String> dataFilePaths) {
        if (dataFilePaths.isEmpty()) {
            return;
        }
        for (int offset = 0; offset < dataFilePaths.size(); offset += BATCH_PARTITION_SIZE) {
            List<String> batch = dataFilePaths.subList(
                    offset, Math.min(offset + BATCH_PARTITION_SIZE, dataFilePaths.size()));
            try (var connection = dataSource().getConnection();
                    var paths = JdbcTextSelection.create(connection, batch);
                    var statement = connection.prepareStatement(
                            "UPDATE index_file_mappings SET last_verified_at=0 "
                                    + "WHERE index_id=? AND data_file_path = ANY (?)")) {
                statement.setString(1, indexId);
                statement.setArray(2, paths.array());
                statement.executeUpdate();
            } catch (SQLException failure) {
                throw new MetastoreException("Orphan marking failed", failure);
            }
        }
    }

    // --- Stale file mappings ---

    List<IndexFileMapping> listStaleFileMappings(long olderThanMillis) {
        String sql = "SELECT * FROM index_file_mappings WHERE last_verified_at < ?";
        return executeQuery(sql, ps -> ps.setLong(1, olderThanMillis),
                IndexMetadataMapper::mapMappingList);
    }

    List<IndexFileMapping> listStaleFileMappings(long olderThanMillis, Instant createdBefore) {
        String sql = "SELECT * FROM index_file_mappings WHERE last_verified_at < ? AND created_at < ?";
        return executeQuery(sql, ps -> {
            ps.setLong(1, olderThanMillis);
            ps.setTimestamp(2, Timestamp.from(createdBefore));
        }, IndexMetadataMapper::mapMappingList);
    }

    OrphanRemoval removeOrphanMappings(String indexId, IndexState state, int generation,
                                      List<IndexFileMapping> mappings) {
        if (mappings.isEmpty()) return new OrphanRemoval(0, Set.of());
        if (state == IndexState.BUILDING || mappings.stream().anyMatch(mapping ->
                !indexId.equals(mapping.indexId()) || mapping.generation() >= generation - 1)) {
            throw new IllegalArgumentException("Orphan cleanup requires one index and obsolete generations");
        }
        try (var connection = dataSource().getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                var removed = lockOrphanOwner(connection, indexId, state, generation)
                        ? removeOrphanBatches(connection, indexId, mappings) : new OrphanRemoval(0, Set.of());
                connection.commit();
                return removed;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to remove obsolete mappings for index=" + indexId, failure);
        }
    }

    private static boolean lockOrphanOwner(Connection connection, String indexId, IndexState state,
                                           int generation) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT index_id FROM index_descriptors
                WHERE index_id = ? AND state = ? AND generation = ?
                FOR UPDATE
                """)) {
            statement.setString(1, indexId);
            statement.setString(2, state.name());
            statement.setInt(3, generation);
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static OrphanRemoval removeOrphanBatches(Connection connection, String indexId,
                                                     List<IndexFileMapping> mappings) throws SQLException {
        int removed = 0;
        Set<String> unreferenced = new LinkedHashSet<>();
        for (int offset = 0; offset < mappings.size(); offset += BATCH_PARTITION_SIZE) {
            var batch = mappings.subList(offset, Math.min(offset + BATCH_PARTITION_SIZE, mappings.size()));
            var result = removeOrphanBatch(connection, indexId, batch);
            removed += result.mappingsRemoved();
            unreferenced.addAll(result.pathsToDelete());
        }
        return new OrphanRemoval(removed, unreferenced);
    }

    private static OrphanRemoval removeOrphanBatch(Connection connection, String indexId,
                                                   List<IndexFileMapping> candidates) throws SQLException {
        // A data-modifying CTE and its SELECT share a PostgreSQL snapshot.
        // Exclude deleted identities explicitly before publishing a path for removal.
        String placeholders = "(?::text,?::text,?::integer,?::bigint,?::timestamp)";
        String values = String.join(",", Collections.nCopies(candidates.size(), placeholders));
        String sql = """
                WITH candidates(data_file_path, index_file_path, generation, last_verified_at, created_at) AS (
                    VALUES %s
                ),
                removed AS (
                    DELETE FROM index_file_mappings AS mapping
                    USING candidates
                    WHERE mapping.index_id = ?
                      AND mapping.data_file_path = candidates.data_file_path
                      AND mapping.index_file_path = candidates.index_file_path
                      AND mapping.generation = candidates.generation
                      AND mapping.last_verified_at = candidates.last_verified_at
                      AND mapping.created_at = candidates.created_at
                    RETURNING mapping.id, mapping.index_file_path
                ),
                removed_paths AS (
                    SELECT index_file_path, count(*) AS removed_count FROM removed GROUP BY index_file_path
                ),
                surviving_paths AS (
                    SELECT DISTINCT owner.index_file_path
                    FROM index_file_mappings AS owner
                    JOIN removed_paths AS path ON owner.index_file_path = path.index_file_path
                    LEFT JOIN removed AS deleted ON owner.id = deleted.id
                    WHERE owner.index_id = ? AND deleted.id IS NULL
                )
                SELECT path.index_file_path, path.removed_count, survivor.index_file_path IS NULL AS can_delete
                FROM removed_paths AS path
                LEFT JOIN surviving_paths AS survivor USING (index_file_path)
                """.formatted(values);
        try (var statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            for (var candidate : candidates) {
                statement.setString(parameter++, candidate.dataFilePath());
                statement.setString(parameter++, candidate.indexFilePath());
                statement.setInt(parameter++, candidate.generation());
                statement.setLong(parameter++, candidate.lastVerifiedAt());
                statement.setTimestamp(parameter++, Timestamp.from(candidate.createdAt()));
            }
            statement.setString(parameter++, indexId);
            statement.setString(parameter, indexId);
            try (var rows = statement.executeQuery()) {
                int removed = 0;
                Set<String> unreferenced = new LinkedHashSet<>();
                while (rows.next()) {
                    removed += rows.getInt("removed_count");
                    if (rows.getBoolean("can_delete")) unreferenced.add(rows.getString("index_file_path"));
                }
                return new OrphanRemoval(removed, unreferenced);
            }
        }
    }

    // --- Cache events ---

    void insertCacheEvent(String eventType, String indexId,
                          String catalog, String schema, String table, Instant now) {
        String sql = "INSERT INTO index_cache_events "
                + "(event_type, index_id, catalog_name, schema_name, table_name, created_at) "
                + "VALUES (?,?,?,?,?,?)";
        executeUpdate(sql, ps -> {
            ps.setString(1, eventType);
            ps.setString(2, indexId);
            ps.setString(3, catalog);
            ps.setString(4, schema);
            ps.setString(5, table);
            ps.setTimestamp(6, Timestamp.from(now));
        });
    }

    List<CacheEvent> pollCacheEvents(long afterEventId, int limit) {
        String sql = "SELECT event_id, event_type, index_id, catalog_name, schema_name, "
                + "table_name, created_at "
                + "FROM index_cache_events WHERE event_id > ? ORDER BY event_id LIMIT ?";
        return executeQuery(sql, ps -> {
            ps.setLong(1, afterEventId);
            ps.setInt(2, limit);
        }, rs -> {
            List<CacheEvent> events = new ArrayList<>();
            while (rs.next()) {
                events.add(new CacheEvent(
                        rs.getLong("event_id"),
                        rs.getString("event_type"),
                        rs.getString("index_id"),
                        rs.getString("catalog_name"),
                        rs.getString("schema_name"),
                        rs.getString("table_name"),
                        rs.getTimestamp("created_at").toInstant()));
            }
            return events;
        });
    }

    // --- Circuit breaker state ---

    Optional<BreakerState> getBreakerState(String indexId) {
        String sql = "SELECT index_id, failure_count, last_failure_at, last_success_at, state "
                + "FROM index_eval_breaker_state WHERE index_id=?";
        return executeQuery(sql, ps -> ps.setString(1, indexId), rs -> {
            if (!rs.next()) {
                return Optional.empty();
            }
            Timestamp failTs = rs.getTimestamp("last_failure_at");
            Timestamp succTs = rs.getTimestamp("last_success_at");
            return Optional.of(new BreakerState(
                    rs.getString("index_id"),
                    rs.getInt("failure_count"),
                    failTs != null ? failTs.toInstant() : null,
                    succTs != null ? succTs.toInstant() : null,
                    rs.getString("state")));
        });
    }

    void updateBreakerState(String indexId, int failureCount,
                            Instant lastFailureAt, Instant lastSuccessAt, String state) {
        String sql = "INSERT INTO index_eval_breaker_state "
                + "(index_id, failure_count, last_failure_at, last_success_at, state) "
                + "VALUES (?,?,?,?,?) "
                + "ON CONFLICT (index_id) DO UPDATE SET "
                + "failure_count=EXCLUDED.failure_count, last_failure_at=EXCLUDED.last_failure_at, "
                + "last_success_at=EXCLUDED.last_success_at, state=EXCLUDED.state";
        executeUpdate(sql, ps -> {
            ps.setString(1, indexId);
            ps.setInt(2, failureCount);
            ps.setTimestamp(3, lastFailureAt != null ? Timestamp.from(lastFailureAt) : null);
            ps.setTimestamp(4, lastSuccessAt != null ? Timestamp.from(lastSuccessAt) : null);
            ps.setString(5, state);
        });
    }
}
