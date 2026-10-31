package org.pangrova.query.store.jdbc.audit;

import org.pangrova.query.api.audit.QueryAuditStore;

import org.pangrova.query.api.audit.QueryAuditRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDBC-backed audit store that persists QueryAuditRecords to the {@code query_audit_records}
 * table using batch writes for throughput.
 *
 * <p>Records are buffered in an internal queue and flushed in batches of up to
 * {@value #BATCH_SIZE} records. A background scheduler triggers periodic flushes
 * to bound latency even under low write volume.</p>
 *
 * <p>Failure resilience (F-OBS-02): flush failures re-queue records and track
 * consecutive failure count. After {@value #MAX_CONSECUTIVE_FAILURES} consecutive
 * failures, new events are explicitly rejected until a successful flush resets
 * the counter. Capacity covers queued and in-flight records, so failed batches
 * retain their reserved capacity while producers continue.</p>
 *
 * <p>Async save (F-OBS-09): {@link #save(QueryAuditRecord)} never performs JDBC I/O
 * on the calling thread. Instead, it enqueues the record and schedules an async flush
 * when the buffer reaches {@value #BATCH_SIZE}.</p>
 */
public final class JdbcAuditStore implements QueryAuditStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcAuditStore.class);

    static final int BATCH_SIZE = 100;
    static final int MAX_CONSECUTIVE_FAILURES = 5;
    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    private static final long FLUSH_INTERVAL_MS = 1_000;
    private static final long SECONDS_PER_DAY = 24L * 60 * 60;
    private static final int MAX_SQL_LENGTH = 4096;

    private static final String SQL_INSERT =
            "INSERT INTO query_audit_records "
                    + "(query_id, user_id, sql_text, tenant_id, stage_count, task_count, "
                    + "cpu_time_ms, wall_time_ms, peak_memory_bytes, spilled_bytes, "
                    + "input_rows, output_rows, input_bytes, output_bytes, "
                    + "shuffle_read_bytes, shuffle_write_bytes, completed_at, success) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_FIND_BY_ID =
            "SELECT * FROM query_audit_records WHERE query_id = ?";

    private static final String SQL_DELETE_OLDER_THAN =
            "DELETE FROM query_audit_records WHERE completed_at < ?";

    private final DataSource dataSource;
    private final BlockingQueue<QueryAuditRecord> buffer;
    private final ScheduledExecutorService scheduler;
    private final Semaphore capacity;
    private final Object admissionLock = new Object();
    private final AtomicReference<CompletableFuture<Void>> flushOwner = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    public JdbcAuditStore(DataSource dataSource) {
        this(dataSource, DEFAULT_QUEUE_CAPACITY);
    }

    /**
     * Creates a store backed by the given data source with a bounded buffer capacity.
     */
    public JdbcAuditStore(DataSource dataSource, int queueCapacity) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.buffer = new LinkedBlockingQueue<>(queueCapacity);
        this.capacity = new Semaphore(queueCapacity);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "jdbc-audit-flush");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.scheduler = executor;
        this.scheduler.scheduleWithFixedDelay(
                this::flushBuffer, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void save(QueryAuditRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        synchronized (admissionLock) {
            if (closed.get()) {
                throw new IllegalStateException("JdbcAuditStore is closed");
            }
            if (consecutiveFailures.get() >= MAX_CONSECUTIVE_FAILURES) {
                throw new RejectedExecutionException("Audit flusher disabled after consecutive failures");
            }
            if (!capacity.tryAcquire()) {
                throw new RejectedExecutionException("Audit buffer capacity exhausted");
            }
            if (!buffer.offer(record)) {
                capacity.release();
                throw new IllegalStateException("Reserved audit buffer capacity was unavailable");
            }
            if (buffer.size() >= BATCH_SIZE) {
                scheduler.submit(this::flushBuffer);
            }
        }
    }

    @Override
    public Optional<QueryAuditRecord> findById(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_BY_ID)) {
            ps.setString(1, queryId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            LOG.error("event=audit_find_error query_id={} error={}", queryId, e.getMessage(), e);
            throw new RuntimeException("Failed to find audit record: " + queryId, e);
        }
    }

    @Override
    public long deleteOlderThan(int retentionDays) {
        Instant cutoff = Instant.now().minusSeconds((long) retentionDays * SECONDS_PER_DAY);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_DELETE_OLDER_THAN)) {
            ps.setTimestamp(1, Timestamp.from(cutoff));
            return ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=audit_cleanup_error retention_days={} error={}",
                    retentionDays, e.getMessage(), e);
            throw new RuntimeException("Failed to delete expired audit records", e);
        }
    }

    /**
     * Flushes pending records from the buffer to the database in batches.
     * Thread-safe: flushes are serialized without blocking save() on JDBC I/O.
     * On failure, records are re-queued for retry on the next flush cycle.
     */
    void flushBuffer() {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        if (!flushOwner.compareAndSet(null, completion)) {
            return;
        }
        try {
            flushBatch();
            completion.complete(null);
        } catch (RuntimeException | Error e) {
            completion.completeExceptionally(e);
            throw e;
        } finally {
            flushOwner.compareAndSet(completion, null);
        }
    }

    private void flushBatch() {
        List<QueryAuditRecord> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);
        if (batch.isEmpty()) {
            return;
        }
        try {
            writeBatch(batch);
            capacity.release(batch.size());
            consecutiveFailures.set(0);
        } catch (Exception e) {
            int failures = consecutiveFailures.incrementAndGet();
            LOG.warn("event=audit_flush_failed consecutive_failures={} batch_size={} error={}",
                    failures, batch.size(), e.getMessage());
            // Re-queue records for retry on next flush cycle
            for (QueryAuditRecord record : batch) {
                if (!buffer.offer(record)) {
                    throw new IllegalStateException("Reserved retry capacity was unavailable", e);
                }
            }
        }
    }

    /**
     * Returns the current consecutive failure count.
     * Exposed for testing failure-resilience behavior.
     */
    int consecutiveFailureCount() {
        return consecutiveFailures.get();
    }

    private void writeBatch(List<QueryAuditRecord> batch) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT)) {
            conn.setAutoCommit(false);
            for (QueryAuditRecord record : batch) {
                bindRecord(ps, record);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            LOG.debug("event=audit_batch_flushed count={}", batch.size());
        } catch (SQLException e) {
            LOG.error("event=audit_batch_write_error count={} error={}",
                    batch.size(), e.getMessage(), e);
            throw new RuntimeException("Failed to write audit batch of " + batch.size() + " records", e);
        }
    }

    private static void bindRecord(PreparedStatement ps, QueryAuditRecord record) throws SQLException {
        int paramIndex = 0;
        ps.setString(++paramIndex, record.queryId());
        ps.setString(++paramIndex, record.userId());
        ps.setString(++paramIndex, truncateSql(record.sql()));
        ps.setString(++paramIndex, record.tenantId());
        ps.setInt(++paramIndex, record.stageCount());
        ps.setInt(++paramIndex, record.taskCount());
        ps.setLong(++paramIndex, record.cpuTimeMs());
        ps.setLong(++paramIndex, record.wallTimeMs());
        ps.setLong(++paramIndex, record.peakMemoryBytes());
        ps.setLong(++paramIndex, record.spilledBytes());
        ps.setLong(++paramIndex, record.inputRows());
        ps.setLong(++paramIndex, record.outputRows());
        ps.setLong(++paramIndex, record.inputBytes());
        ps.setLong(++paramIndex, record.outputBytes());
        ps.setLong(++paramIndex, record.shuffleReadBytes());
        ps.setLong(++paramIndex, record.shuffleWriteBytes());
        ps.setTimestamp(++paramIndex, record.completedAt() != null
                ? Timestamp.from(record.completedAt()) : null);
        ps.setBoolean(++paramIndex, record.success());
    }

    private static String truncateSql(String sql) {
        if (sql == null) {
            return null;
        }
        return sql.length() > MAX_SQL_LENGTH ? sql.substring(0, MAX_SQL_LENGTH) : sql;
    }

    private static QueryAuditRecord mapRow(ResultSet rs) throws SQLException {
        Timestamp completedTs = rs.getTimestamp("completed_at");
        return new QueryAuditRecord(
                rs.getString("query_id"),
                rs.getString("user_id"),
                rs.getString("sql_text"),
                rs.getString("tenant_id"),
                rs.getInt("stage_count"),
                rs.getInt("task_count"),
                rs.getLong("cpu_time_ms"),
                rs.getLong("wall_time_ms"),
                rs.getLong("peak_memory_bytes"),
                rs.getLong("spilled_bytes"),
                rs.getLong("input_rows"),
                rs.getLong("output_rows"),
                rs.getLong("input_bytes"),
                rs.getLong("output_bytes"),
                rs.getLong("shuffle_read_bytes"),
                rs.getLong("shuffle_write_bytes"),
                completedTs != null ? completedTs.toInstant() : null,
                rs.getBoolean("success"));
    }

    @Override
    public void close() {
        synchronized (admissionLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            scheduler.shutdown();
        }
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        drainRemainingRecords();
    }

    private void drainRemainingRecords() {
        do {
            CompletableFuture<Void> inFlight = flushOwner.get();
            if (inFlight != null) {
                inFlight.join();
                continue;
            }
            flushBuffer();
            if (consecutiveFailures.get() > 0) {
                throw new IllegalStateException("Audit store closed with unpersisted records");
            }
        } while (!buffer.isEmpty() || flushOwner.get() != null);
    }
}
