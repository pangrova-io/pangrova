package org.pangrova.observability.audit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for JdbcAuditStore.
 * Uses H2 in-memory database with a controllable DataSource wrapper
 * that can inject failures on demand.
 *
 * Validates:
 * - F-OBS-02: single DB error does not permanently disable the flusher;
 *             retry and consecutive-failure tracking
 * - F-OBS-09: save() never performs synchronous JDBC on the calling thread;
 *             async flush, bounded queue, overflow policy
 */
class JdbcAuditStoreTest {

    private Connection sharedConnection;
    private FailableDataSource failableDataSource;
    private JdbcAuditStore store;

    @BeforeEach
    void setUp() throws Exception {
        // Use a single H2 in-memory connection to keep the schema alive
        sharedConnection = DriverManager.getConnection(
                "jdbc:h2:mem:audit_test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        try (Statement stmt = sharedConnection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE query_audit_records (
                        query_id            VARCHAR(64)   NOT NULL PRIMARY KEY,
                        user_id             VARCHAR(255),
                        sql_text            TEXT,
                        tenant_id           VARCHAR(255),
                        stage_count         INT           NOT NULL DEFAULT 0,
                        task_count          INT           NOT NULL DEFAULT 0,
                        cpu_time_ms         BIGINT        NOT NULL DEFAULT 0,
                        wall_time_ms        BIGINT        NOT NULL DEFAULT 0,
                        peak_memory_bytes   BIGINT        NOT NULL DEFAULT 0,
                        spilled_bytes       BIGINT        NOT NULL DEFAULT 0,
                        input_rows          BIGINT        NOT NULL DEFAULT 0,
                        output_rows         BIGINT        NOT NULL DEFAULT 0,
                        input_bytes         BIGINT        NOT NULL DEFAULT 0,
                        output_bytes        BIGINT        NOT NULL DEFAULT 0,
                        shuffle_read_bytes  BIGINT        NOT NULL DEFAULT 0,
                        shuffle_write_bytes BIGINT        NOT NULL DEFAULT 0,
                        completed_at        TIMESTAMP,
                        success             BOOLEAN       NOT NULL DEFAULT FALSE
                    )
                    """);
        }
        failableDataSource = new FailableDataSource(sharedConnection);
        store = new JdbcAuditStore(failableDataSource);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
        if (sharedConnection != null && !sharedConnection.isClosed()) {
            sharedConnection.close();
        }
    }

    // ---- F-OBS-02: Retry and failure resilience ----

    @Test
    void singleFlushFailureDoesNotDisableFlusher() {
        // Add a record
        store.save(buildRecord("q-1"));

        // Make DB fail for one flush
        failableDataSource.setFailing(true);
        store.flushBuffer();

        // Failure count should be 1, flusher is NOT disabled
        assertThat(store.consecutiveFailureCount()).isEqualTo(1);

        // Record should still be in buffer (re-queued)
        // Re-enable DB and flush again — should succeed
        failableDataSource.setFailing(false);
        store.flushBuffer();

        // Failure counter reset on success
        assertThat(store.consecutiveFailureCount()).isEqualTo(0);

        // Record was persisted
        assertThat(store.findById("q-1")).isPresent();
    }

    @Test
    void eventsAreRetriedOnNextFlushCycleAfterFailure() {
        store.save(buildRecord("q-retry-1"));
        store.save(buildRecord("q-retry-2"));

        // First flush fails — events should be re-queued
        failableDataSource.setFailing(true);
        store.flushBuffer();

        // Verify records are NOT in DB
        failableDataSource.setFailing(false);
        assertThat(store.findById("q-retry-1")).isEmpty();
        assertThat(store.findById("q-retry-2")).isEmpty();

        // Second flush succeeds — retried events are written
        store.flushBuffer();
        assertThat(store.findById("q-retry-1")).isPresent();
        assertThat(store.findById("q-retry-2")).isPresent();
        assertThat(store.consecutiveFailureCount()).isEqualTo(0);
    }

    @Test
    void flusherDisabledOnlyAfterMaxConsecutiveFailures() {
        store.save(buildRecord("q-disabled"));

        failableDataSource.setFailing(true);

        // Flush MAX_CONSECUTIVE_FAILURES times — all fail
        for (int i = 0; i < JdbcAuditStore.MAX_CONSECUTIVE_FAILURES; i++) {
            store.flushBuffer();
        }

        assertThat(store.consecutiveFailureCount())
                .isEqualTo(JdbcAuditStore.MAX_CONSECUTIVE_FAILURES);

        // Now save() should silently drop new events
        store.save(buildRecord("q-dropped"));

        // Re-enable and flush — the original event should still be retried
        failableDataSource.setFailing(false);
        store.flushBuffer();

        // After successful flush, counter resets
        assertThat(store.consecutiveFailureCount()).isEqualTo(0);

        // Original event was persisted, dropped one was not
        assertThat(store.findById("q-disabled")).isPresent();
        assertThat(store.findById("q-dropped")).isEmpty();
    }

    @Test
    void fewerThanMaxFailuresDoesNotDropEvents() {
        failableDataSource.setFailing(true);

        // Fail fewer than MAX times
        store.save(buildRecord("q-kept"));
        for (int i = 0; i < JdbcAuditStore.MAX_CONSECUTIVE_FAILURES - 1; i++) {
            store.flushBuffer();
        }

        assertThat(store.consecutiveFailureCount())
                .isEqualTo(JdbcAuditStore.MAX_CONSECUTIVE_FAILURES - 1);

        // save() should still accept events
        store.save(buildRecord("q-still-accepted"));

        // Re-enable and flush
        failableDataSource.setFailing(false);
        store.flushBuffer();

        assertThat(store.consecutiveFailureCount()).isEqualTo(0);
        assertThat(store.findById("q-kept")).isPresent();
        assertThat(store.findById("q-still-accepted")).isPresent();
    }

    @Test
    void successfulFlushResetsFailureCounterAndReEnables() {
        store.save(buildRecord("q-reset"));

        // Accumulate some failures
        failableDataSource.setFailing(true);
        store.flushBuffer();
        store.flushBuffer();
        store.flushBuffer();
        assertThat(store.consecutiveFailureCount()).isEqualTo(3);

        // Successful flush resets
        failableDataSource.setFailing(false);
        store.flushBuffer();
        assertThat(store.consecutiveFailureCount()).isEqualTo(0);
        assertThat(store.findById("q-reset")).isPresent();

        // New events accepted again
        store.save(buildRecord("q-after-recovery"));
        store.flushBuffer();
        assertThat(store.findById("q-after-recovery")).isPresent();
    }

    // ---- F-OBS-09: Async save — never block the calling thread ----

    @Test
    void saveDoesNotCallFlushSynchronouslyOnCallingThread() {
        // Use a latching DataSource that blocks on getConnection() so we can detect
        // if the calling thread enters JDBC code.
        AtomicBoolean callerBlockedInJdbc = new AtomicBoolean(false);
        CountDownLatch jdbcEntered = new CountDownLatch(1);

        BlockingDataSource blockingDs = new BlockingDataSource(failableDataSource, jdbcEntered);
        JdbcAuditStore asyncStore = new JdbcAuditStore(blockingDs);
        try {
            // Save enough records to trigger a flush (>= BATCH_SIZE)
            Thread callerThread = Thread.currentThread();
            blockingDs.setCallerThread(callerThread);

            for (int i = 0; i < JdbcAuditStore.BATCH_SIZE + 10; i++) {
                asyncStore.save(buildRecord("q-async-" + i));
            }

            // save() should have returned immediately without blocking
            // If it had called flushBuffer() synchronously, BlockingDataSource
            // would have recorded it
            assertThat(blockingDs.wasCallerThreadSeenInJdbc())
                    .as("save() must not perform JDBC I/O on the calling thread")
                    .isFalse();
        } finally {
            asyncStore.close();
        }
    }

    @Test
    void saveEnqueuesAndReturnsImmediately() {
        // Make the DB very slow — if save() were synchronous, this would take > 1 second
        SlowDataSource slowDs = new SlowDataSource(failableDataSource, 500);
        JdbcAuditStore asyncStore = new JdbcAuditStore(slowDs);
        try {
            long startNanos = System.nanoTime();
            for (int i = 0; i < JdbcAuditStore.BATCH_SIZE + 10; i++) {
                asyncStore.save(buildRecord("q-fast-" + i));
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            // save() calls should complete in < 100ms total even though DB is slow
            assertThat(elapsedMs)
                    .as("save() must return immediately without waiting for JDBC flush")
                    .isLessThan(200);
        } finally {
            asyncStore.close();
        }
    }

    @Test
    void backgroundSchedulerFlushesPeriodically() throws Exception {
        store.save(buildRecord("q-periodic"));

        // Don't call flushBuffer() — rely on the background scheduler
        // The scheduler flushes every 1 second, so wait a bit
        Thread.sleep(2_000);

        assertThat(store.findById("q-periodic"))
                .as("Background scheduler should have flushed the record")
                .isPresent();
    }

    @Test
    void boundedQueueDropsOnOverflow() {
        // Create store with tiny queue capacity
        JdbcAuditStore tinyStore = new JdbcAuditStore(failableDataSource, 5);
        try {
            // Disable DB so nothing gets flushed
            failableDataSource.setFailing(true);

            // Fill beyond capacity — excess should be silently dropped
            for (int i = 0; i < 20; i++) {
                tinyStore.save(buildRecord("q-overflow-" + i));
            }

            // No exception thrown — overflow is handled gracefully
            // Re-enable and flush what we have
            failableDataSource.setFailing(false);
            tinyStore.flushBuffer();

            // At most 5 records should be persisted (the queue capacity)
            int found = 0;
            for (int i = 0; i < 20; i++) {
                if (tinyStore.findById("q-overflow-" + i).isPresent()) {
                    found++;
                }
            }
            assertThat(found)
                    .as("Only records that fit in the bounded queue should be persisted")
                    .isLessThanOrEqualTo(5);
        } finally {
            tinyStore.close();
        }
    }

    @Test
    void saveThrowsAfterClose() {
        store.close();
        assertThatThrownBy(() -> store.save(buildRecord("q-closed")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void closeFlushesPendingRecords() {
        store.save(buildRecord("q-close-flush-1"));
        store.save(buildRecord("q-close-flush-2"));

        // close() should do a final flush
        store.close();

        // Create a new store to query (the old one is closed)
        JdbcAuditStore queryStore = new JdbcAuditStore(failableDataSource);
        try {
            assertThat(queryStore.findById("q-close-flush-1")).isPresent();
            assertThat(queryStore.findById("q-close-flush-2")).isPresent();
        } finally {
            queryStore.close();
        }
    }

    @Test
    void batchSizeTriggersAsyncFlush() throws Exception {
        // Save exactly BATCH_SIZE records — should trigger an async flush
        for (int i = 0; i < JdbcAuditStore.BATCH_SIZE; i++) {
            store.save(buildRecord("q-trigger-" + i));
        }

        // Wait for the async flush to complete
        Thread.sleep(500);

        int found = 0;
        for (int i = 0; i < JdbcAuditStore.BATCH_SIZE; i++) {
            if (store.findById("q-trigger-" + i).isPresent()) {
                found++;
            }
        }
        assertThat(found)
                .as("Async flush should persist records when batch size is reached")
                .isGreaterThan(0);
    }

    // -- Helpers --

    private static QueryAuditRecord buildRecord(String queryId) {
        return new QueryAuditRecord(
                queryId, "test-user", "SELECT 1", "test-tenant",
                1, 2, 50L, 100L, 512L, 0L,
                100L, 50L, 1024L, 256L, 0L, 0L,
                Instant.now(), true);
    }

    /**
     * A DataSource wrapper that can be toggled to throw SQLException on getConnection().
     * Uses a shared H2 in-memory connection underneath.
     */
    static final class FailableDataSource implements DataSource {

        private final Connection realConnection;
        private final AtomicBoolean failing = new AtomicBoolean(false);
        private final String url;

        FailableDataSource(Connection realConnection) throws SQLException {
            this.realConnection = realConnection;
            this.url = realConnection.getMetaData().getURL();
        }

        void setFailing(boolean shouldFail) {
            failing.set(shouldFail);
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (failing.get()) {
                throw new SQLException("Simulated transient DB error");
            }
            // Return a new connection to the same in-memory DB
            return DriverManager.getConnection(url);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }

    /**
     * DataSource wrapper that tracks which thread calls getConnection().
     * Used to verify that save() does not perform JDBC on the calling thread.
     */
    private static final class BlockingDataSource implements DataSource {

        private final DataSource delegate;
        private final CountDownLatch latch;
        private volatile Thread callerThread;
        private final AtomicBoolean callerSeenInJdbc = new AtomicBoolean(false);

        BlockingDataSource(DataSource delegate, CountDownLatch latch) {
            this.delegate = delegate;
            this.latch = latch;
        }

        void setCallerThread(Thread thread) {
            this.callerThread = thread;
        }

        boolean wasCallerThreadSeenInJdbc() {
            return callerSeenInJdbc.get();
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (Thread.currentThread() == callerThread) {
                callerSeenInJdbc.set(true);
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() { return null; }
        @Override
        public void setLogWriter(PrintWriter out) {}
        @Override
        public void setLoginTimeout(int seconds) {}
        @Override
        public int getLoginTimeout() { return 0; }
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }

    /**
     * DataSource wrapper that introduces artificial delay on getConnection().
     * Used to verify that save() does not block waiting for JDBC.
     */
    private static final class SlowDataSource implements DataSource {

        private final DataSource delegate;
        private final long delayMs;

        SlowDataSource(DataSource delegate, long delayMs) {
            this.delegate = delegate;
            this.delayMs = delayMs;
        }

        @Override
        public Connection getConnection() throws SQLException {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return delegate.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() { return null; }
        @Override
        public void setLogWriter(PrintWriter out) {}
        @Override
        public void setLoginTimeout(int seconds) {}
        @Override
        public int getLoginTimeout() { return 0; }
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
        }
        @Override
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
