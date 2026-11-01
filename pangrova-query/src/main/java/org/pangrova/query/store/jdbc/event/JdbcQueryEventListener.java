package org.pangrova.query.store.jdbc.event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.spi.observability.event.QueryState;
import org.pangrova.spi.observability.event.QueryCompletedEvent;
import org.pangrova.spi.observability.event.QueryStatistics;
import org.pangrova.spi.observability.event.SplitCompletedEvent;
import org.pangrova.spi.observability.event.QueryEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists {@link QueryCompletedEvent}s and {@link SplitCompletedEvent}s to queryable
 * JDBC tables, giving audit/history tooling a durable per-query record with stage/split
 * breakdown that survives coordinator restarts.
 *
 * <p>Schema ownership lives in migration {@code V102__query_event_store.sql}; this
 * class performs synchronous single-row inserts only. Completed events fire once per
 * query/split, so batching is unnecessary. Persistence failures are logged at ERROR
 * with an {@code audit_event_lost} marker (so audit-record loss is alertable) and then
 * swallowed — a persistence failure must never fail the query itself.</p>
 */
public final class JdbcQueryEventListener implements QueryEventListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQueryEventListener.class);

    private static final String SQL_INSERT_EVENT =
            "INSERT INTO query_events "
                    + "(query_id, sql_text, principal, final_state, create_time, end_time, "
                    + "wall_time_ms, cpu_time_ms, total_rows, total_bytes, failure_message, success) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_INSERT_SPLIT =
            "INSERT INTO query_split_profiles "
                    + "(query_id, stage_id, split_id, catalog_name, start_time, end_time, "
                    + "duration_ms, processed_rows, processed_bytes, failure_message, success) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_FIND_BY_ID =
            "SELECT query_id, sql_text, principal, final_state, create_time, end_time, "
                    + "wall_time_ms, cpu_time_ms, total_rows, total_bytes, failure_message "
                    + "FROM query_events WHERE query_id = ?";

    private static final String SQL_FIND_SPLITS_BY_QUERY =
            "SELECT query_id, stage_id, split_id, catalog_name, start_time, end_time, "
                    + "duration_ms, processed_rows, processed_bytes, failure_message, success "
                    + "FROM query_split_profiles WHERE query_id = ? ORDER BY stage_id, split_id";

    private final DataSource dataSource;

    public JdbcQueryEventListener(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_INSERT_EVENT)) {
            QueryStatistics stats = event.statistics();
            int paramIndex = 0;
            stmt.setString(++paramIndex, event.queryId());
            stmt.setString(++paramIndex, event.sql());
            stmt.setString(++paramIndex, event.principal());
            stmt.setString(++paramIndex, event.finalState().name());
            stmt.setTimestamp(++paramIndex, new Timestamp(event.createTimeEpochMillis()));
            stmt.setTimestamp(++paramIndex, new Timestamp(event.endTimeEpochMillis()));
            stmt.setLong(++paramIndex, stats.wallTimeMillis());
            stmt.setLong(++paramIndex, stats.cpuTimeMillis());
            stmt.setLong(++paramIndex, stats.totalRows());
            stmt.setLong(++paramIndex, stats.totalBytes());
            stmt.setString(++paramIndex, event.failureMessage().orElse(null));
            stmt.setBoolean(++paramIndex, event.succeeded());
            stmt.executeUpdate();
        } catch (SQLException e) {
            // F-OBS-001: audit/history record permanently lost — escalate to ERROR with an
            // explicit audit-loss marker so monitoring can alert (never fail the query for this).
            LOG.error("event=audit_event_lost kind=query queryId={} error={}",
                    event.queryId(), e.getMessage(), e);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_INSERT_SPLIT)) {
            int paramIndex = 0;
            stmt.setString(++paramIndex, event.queryId());
            stmt.setString(++paramIndex, event.stageId());
            stmt.setString(++paramIndex, event.splitId());
            stmt.setString(++paramIndex, event.catalogName());
            stmt.setTimestamp(++paramIndex, new Timestamp(event.startTimeEpochMillis()));
            stmt.setTimestamp(++paramIndex, new Timestamp(event.endTimeEpochMillis()));
            stmt.setLong(++paramIndex, event.durationMillis());
            stmt.setLong(++paramIndex, event.processedRows());
            stmt.setLong(++paramIndex, event.processedBytes());
            stmt.setString(++paramIndex, event.failureMessage().orElse(null));
            stmt.setBoolean(++paramIndex, event.succeeded());
            stmt.executeUpdate();
        } catch (SQLException e) {
            // F-OBS-001: profiling record permanently lost — escalate to ERROR with an explicit
            // audit-loss marker so monitoring can alert (never fail the query for this).
            LOG.error("event=audit_event_lost kind=split queryId={} splitId={} error={}",
                    event.queryId(), event.splitId(), e.getMessage(), e);
        }
    }

    /**
     * Reads back a persisted completed-query event by id.
     *
     * @param queryId the query identifier
     * @return the reconstructed event, or empty if not present
     */
    public Optional<QueryCompletedEvent> findByQueryId(String queryId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_FIND_BY_ID)) {
            stmt.setString(1, queryId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                QueryStatistics stats = new QueryStatistics(
                        rs.getLong("wall_time_ms"), rs.getLong("cpu_time_ms"),
                        rs.getLong("total_rows"), rs.getLong("total_bytes"));
                Timestamp createTs = rs.getTimestamp("create_time");
                Timestamp endTs = rs.getTimestamp("end_time");
                return Optional.of(new QueryCompletedEvent(
                        rs.getString("query_id"),
                        rs.getString("sql_text"),
                        rs.getString("principal"),
                        QueryState.valueOf(rs.getString("final_state")),
                        createTs != null ? createTs.getTime() : 0L,
                        endTs != null ? endTs.getTime() : 0L,
                        stats,
                        Optional.ofNullable(rs.getString("failure_message"))));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read query_events for " + queryId, e);
        }
    }

    /**
     * Reads back all persisted split profiles for a query, providing
     * the per-split breakdown needed for post-mortem replay.
     *
     * @param queryId the query identifier
     * @return list of split profile records, empty if none
     */
    public List<SplitCompletedEvent> findSplitsByQueryId(String queryId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL_FIND_SPLITS_BY_QUERY)) {
            stmt.setString(1, queryId);
            try (ResultSet rs = stmt.executeQuery()) {
                List<SplitCompletedEvent> splits = new ArrayList<>();
                while (rs.next()) {
                    Timestamp startTs = rs.getTimestamp("start_time");
                    Timestamp endTs = rs.getTimestamp("end_time");
                    splits.add(new SplitCompletedEvent(
                            rs.getString("query_id"),
                            rs.getString("stage_id"),
                            rs.getString("split_id"),
                            rs.getString("catalog_name"),
                            startTs != null ? startTs.getTime() : 0L,
                            endTs != null ? endTs.getTime() : 0L,
                            rs.getLong("processed_rows"),
                            rs.getLong("processed_bytes"),
                            Optional.ofNullable(rs.getString("failure_message"))));
                }
                return splits;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read split profiles for " + queryId, e);
        }
    }

    @Override
    public void close() {
        // No background resources to release; method present for lifecycle symmetry.
    }
}
