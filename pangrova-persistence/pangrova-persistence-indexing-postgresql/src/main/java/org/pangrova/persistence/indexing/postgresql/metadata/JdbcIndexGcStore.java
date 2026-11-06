package org.pangrova.persistence.indexing.postgresql.metadata;

import javax.sql.DataSource;

import org.pangrova.persistence.indexing.api.metadata.BreakerState;
import org.pangrova.persistence.indexing.api.metadata.CacheEvent;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;

import java.sql.Timestamp;
import java.sql.SQLException;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
