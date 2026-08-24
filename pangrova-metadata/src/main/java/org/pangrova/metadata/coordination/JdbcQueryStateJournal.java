package org.pangrova.metadata.coordination;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.ThreadUtils;
import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.QueryStateEvent;
import org.pangrova.spi.coordination.QueryStateJournal;
import org.pangrova.spi.coordination.StateJournalConfig;
import org.pangrova.spi.coordination.TaskStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-backed state journal with two-tier durability.
 *
 * <p>Sync methods execute a direct INSERT and block until committed.
 * Async methods enqueue events into a bounded queue; a background thread
 * flushes batches at configurable interval and batch size.</p>
 *
 * <p>Shutdown order (LIFO): flush remaining events → stop background thread → release DataSource.</p>
 */
@ThreadSafe
public final class JdbcQueryStateJournal implements QueryStateJournal, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQueryStateJournal.class);

    private static final String INSERT_QUERY_EVENT =
            "INSERT INTO query_state_journal (query_id, sql_text, session_json, state, timestamp_ms, coordinator_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?)";

    private static final String INSERT_TASK_EVENT =
            "INSERT INTO task_state_journal (query_id, task_id, worker_id, state, stage_id, report_sequence, "
                    + "timestamp_ms, coordinator_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String REPLAY_ACTIVE_QUERIES =
            "SELECT query_id, sql_text, session_json, state, timestamp_ms, coordinator_id "
                    + "FROM query_state_journal WHERE query_id IN ("
                    + "  SELECT DISTINCT query_id FROM query_state_journal "
                    + "  WHERE query_id NOT IN ("
                    + "    SELECT DISTINCT query_id FROM query_state_journal "
                    + "    WHERE state IN ('FINISHED','FAILED','CANCELLED')"
                    + "  )"
                    + ") ORDER BY id ASC";

    private static final String REPLAY_ACTIVE_TASKS =
            "SELECT query_id, task_id, worker_id, state, stage_id, report_sequence, timestamp_ms, coordinator_id "
                    + "FROM task_state_journal WHERE query_id IN ("
                    + "  SELECT DISTINCT query_id FROM query_state_journal "
                    + "  WHERE query_id NOT IN ("
                    + "    SELECT DISTINCT query_id FROM query_state_journal "
                    + "    WHERE state IN ('FINISHED','FAILED','CANCELLED')"
                    + "  )"
                    + ") ORDER BY id ASC";

    /** A-XNODE-01: latest event for one query — cross-coordinator state lookup fallback. */
    private static final String LATEST_QUERY_EVENT =
            "SELECT query_id, sql_text, session_json, state, timestamp_ms, coordinator_id "
                    + "FROM query_state_journal WHERE query_id = ? ORDER BY id DESC";

    private static final String PURGE_QUERY_EVENTS = "DELETE FROM query_state_journal WHERE query_id = ?";
    private static final String PURGE_TASK_EVENTS = "DELETE FROM task_state_journal WHERE query_id = ?";

    /** Bound on how long {@link #close()} waits for the flush thread to exit. */
    private static final long FLUSH_THREAD_JOIN_TIMEOUT_MS = 5000;

    /** Queue capacity is a fixed multiple of the configured batch size. */
    private static final int QUEUE_CAPACITY_BATCH_MULTIPLIER = 10;

    private final DataSource dataSource;
    private final StateJournalConfig config;
    private final LinkedBlockingQueue<Object> asyncQueue;
    private final Thread flushThread;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates the journal and starts its background flush thread.
     */
    public JdbcQueryStateJournal(DataSource dataSource, StateJournalConfig config) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.asyncQueue = new LinkedBlockingQueue<>(config.batchSize() * QUEUE_CAPACITY_BATCH_MULTIPLIER);
        this.flushThread = new Thread(this::flushLoop, "journal-flush");
        this.flushThread.setDaemon(true);
        this.flushThread.start();
        LOG.info("event=journal_started type=JDBC flushIntervalMs={} batchSize={}",
                config.flushIntervalMs(), config.batchSize());
    }

    @Override
    public void appendQueryEvent(QueryStateEvent event) {
        if (closed.get()) return;
        if (!asyncQueue.offer(event)) {
            LOG.warn("event=journal_queue_full dropped=query queryId={} state={}",
                    event.queryId(), event.state());
        }
    }

    @Override
    public void appendQueryEventSync(QueryStateEvent event) {
        if (closed.get()) return;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_QUERY_EVENT)) {
            setQueryParams(ps, event);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=journal_sync_write_failed type=query queryId={} state={} error={}",
                    event.queryId(), event.state(), e.getMessage(), e);
            throw new JournalWriteException("Failed to write query event synchronously", e);
        }
    }

    @Override
    public void appendTaskEvent(TaskStateEvent event) {
        if (closed.get()) return;
        if (!asyncQueue.offer(event)) {
            LOG.warn("event=journal_queue_full dropped=task taskId={} state={}",
                    event.taskId(), event.state());
        }
    }

    @Override
    public void appendTaskEventSync(TaskStateEvent event) {
        if (closed.get()) return;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_TASK_EVENT)) {
            setTaskParams(ps, event);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=journal_sync_write_failed type=task taskId={} state={} error={}",
                    event.taskId(), event.state(), e.getMessage(), e);
            throw new JournalWriteException("Failed to write task event synchronously", e);
        }
    }

    @Override
    public List<QueryStateEvent> replayActiveQueries() {
        List<QueryStateEvent> events = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(REPLAY_ACTIVE_QUERIES);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                events.add(new QueryStateEvent(
                        rs.getString("query_id"),
                        rs.getString("sql_text"),
                        rs.getString("session_json"),
                        rs.getString("state"),
                        rs.getLong("timestamp_ms"),
                        rs.getString("coordinator_id")));
            }
        } catch (SQLException e) {
            LOG.error("event=journal_replay_failed type=query error={}", e.getMessage(), e);
            throw new JournalWriteException("Failed to replay active queries", e);
        }
        LOG.info("event=journal_replay type=query count={}", events.size());
        return List.copyOf(events);
    }

    @Override
    public java.util.Optional<QueryStateEvent> latestQueryEvent(String queryId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LATEST_QUERY_EVENT)) {
            ps.setString(1, queryId);
            ps.setMaxRows(1);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return java.util.Optional.of(new QueryStateEvent(
                            rs.getString("query_id"),
                            rs.getString("sql_text"),
                            rs.getString("session_json"),
                            rs.getString("state"),
                            rs.getLong("timestamp_ms"),
                            rs.getString("coordinator_id")));
                }
                return java.util.Optional.empty();
            }
        } catch (SQLException e) {
            LOG.error("event=journal_latest_lookup_failed queryId={} error={}",
                    queryId, e.getMessage(), e);
            throw new JournalWriteException("Failed to look up latest query event", e);
        }
    }

    @Override
    public List<TaskStateEvent> replayActiveTasks() {
        List<TaskStateEvent> events = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(REPLAY_ACTIVE_TASKS);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                events.add(new TaskStateEvent(
                        rs.getString("query_id"),
                        rs.getString("task_id"),
                        rs.getString("worker_id"),
                        rs.getString("state"),
                        rs.getInt("stage_id"),
                        rs.getLong("report_sequence"),
                        rs.getLong("timestamp_ms"),
                        rs.getString("coordinator_id")));
            }
        } catch (SQLException e) {
            LOG.error("event=journal_replay_failed type=task error={}", e.getMessage(), e);
            throw new JournalWriteException("Failed to replay active tasks", e);
        }
        LOG.info("event=journal_replay type=task count={}", events.size());
        return List.copyOf(events);
    }

    @Override
    public void purgeQuery(String queryId) {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(PURGE_TASK_EVENTS)) {
                ps.setString(1, queryId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(PURGE_QUERY_EVENTS)) {
                ps.setString(1, queryId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            LOG.error("event=journal_purge_failed queryId={} error={}", queryId, e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        LOG.info("event=journal_closing");
        // Flush remaining events
        flushBatch();
        flushThread.interrupt();
        try {
            flushThread.join(FLUSH_THREAD_JOIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("event=journal_closed");
    }

    // ── Background flush loop ──

    private void flushLoop() {
        while (!closed.get()) {
            ThreadUtils.sleepQuietly(config.flushIntervalMs());
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            flushBatch();
        }
        // Final flush on shutdown
        flushBatch();
    }

    private void flushBatch() {
        List<Object> batch = new ArrayList<>(config.batchSize());
        asyncQueue.drainTo(batch, config.batchSize());
        if (batch.isEmpty()) return;

        try (Connection conn = dataSource.getConnection()) {
            executeFlush(conn, batch);
        } catch (SQLException e) {
            LOG.error("event=journal_flush_connection_failed error={}", e.getMessage(), e);
        }
    }

    private void executeFlush(Connection conn, List<Object> batch) throws SQLException {
        conn.setAutoCommit(false);
        try (PreparedStatement queryPs = conn.prepareStatement(INSERT_QUERY_EVENT);
             PreparedStatement taskPs = conn.prepareStatement(INSERT_TASK_EVENT)) {
            int[] counts = addBatchEvents(batch, queryPs, taskPs);
            if (counts[0] > 0) queryPs.executeBatch();
            if (counts[1] > 0) taskPs.executeBatch();
            conn.commit();
            LOG.debug("event=journal_flush queryEvents={} taskEvents={}", counts[0], counts[1]);
        } catch (SQLException e) {
            conn.rollback();
            LOG.error("event=journal_flush_failed batchSize={} error={}",
                    batch.size(), e.getMessage(), e);
        }
    }

    private int[] addBatchEvents(List<Object> batch, PreparedStatement queryPs,
                                 PreparedStatement taskPs) throws SQLException {
        int queryCount = 0;
        int taskCount = 0;
        for (Object event : batch) {
            if (event instanceof QueryStateEvent qe) {
                setQueryParams(queryPs, qe);
                queryPs.addBatch();
                queryCount++;
            } else if (event instanceof TaskStateEvent te) {
                setTaskParams(taskPs, te);
                taskPs.addBatch();
                taskCount++;
            }
        }
        return new int[]{queryCount, taskCount};
    }

    private static void setQueryParams(PreparedStatement ps, QueryStateEvent event) throws SQLException {
        ps.setString(1, event.queryId());
        ps.setString(2, event.sql());
        ps.setString(3, event.sessionJson());
        ps.setString(4, event.state());
        ps.setLong(5, event.timestampMs());
        ps.setString(6, event.coordinatorId());
    }

    private static void setTaskParams(PreparedStatement ps, TaskStateEvent event) throws SQLException {
        ps.setString(1, event.queryId());
        ps.setString(2, event.taskId());
        ps.setString(3, event.workerId());
        ps.setString(4, event.state());
        ps.setInt(5, event.stageId());
        ps.setLong(6, event.reportSequence());
        ps.setLong(7, event.timestampMs());
        ps.setString(8, event.coordinatorId());
    }
}
