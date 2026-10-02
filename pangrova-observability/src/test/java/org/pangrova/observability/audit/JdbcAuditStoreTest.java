package org.pangrova.observability.audit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.observability.audit.JdbcAuditStoreTestDataSources.BlockingDataSource;
import org.pangrova.observability.audit.JdbcAuditStoreTestDataSources.FailableDataSource;
import org.pangrova.observability.audit.JdbcAuditStoreTestDataSources.GatedDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PostgreSQL integration tests for JdbcAuditStore with a controllable
 * DataSource wrapper that can inject failures on demand.
 *
 * Validates:
 * - F-OBS-02: single DB error does not permanently disable the flusher;
 *             retry and consecutive-failure tracking
 * - F-OBS-09: save() never performs synchronous JDBC on the calling thread;
 *             async flush, bounded queue, overflow policy
 */
@Testcontainers
class JdbcAuditStoreTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_observability_test")
                    .withUsername("pangrova")
                    .withPassword("pangrova");

    private FailableDataSource failableDataSource;
    private JdbcAuditStore store;

    @BeforeEach
    void setUp() throws Exception {
        DataSource dataSource = isolatedDataSource();
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
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
        failableDataSource = new FailableDataSource(dataSource);
        store = new JdbcAuditStore(failableDataSource);
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
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

        assertThatThrownBy(() -> store.save(buildRecord("q-dropped")))
                .isInstanceOf(RejectedExecutionException.class)
                .hasMessageContaining("disabled");

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
        // Track whether save() itself enters JDBC on the caller thread.
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

            assertThat(blockingDs.wasCallerThreadSeenInJdbc())
                    .as("save() must not perform JDBC I/O on the calling thread")
                    .isFalse();
        } finally {
            asyncStore.close();
        }
    }

    @Test
    void saveEnqueuesAndReturnsImmediately() {
        CountDownLatch jdbcRelease = new CountDownLatch(1);
        GatedDataSource gatedDs = new GatedDataSource(failableDataSource, jdbcRelease, new CountDownLatch(1));
        JdbcAuditStore asyncStore = new JdbcAuditStore(gatedDs);
        try {
            long startNanos = System.nanoTime();
            for (int i = 0; i < JdbcAuditStore.BATCH_SIZE + 10; i++) {
                asyncStore.save(buildRecord("q-fast-" + i));
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            // save() must return before the explicit JDBC gate is released.
            assertThat(elapsedMs)
                    .as("save() must return immediately without waiting for JDBC flush")
                    .isLessThan(200);
        } finally {
            jdbcRelease.countDown();
            asyncStore.close();
        }
    }

    @Test
    void backgroundSchedulerFlushesPeriodically() throws Exception {
        store.save(buildRecord("q-periodic"));

        awaitPersisted(store, "q-periodic");

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

            for (int i = 0; i < 5; i++) {
                tinyStore.save(buildRecord("q-overflow-" + i));
            }
            assertThatThrownBy(() -> tinyStore.save(buildRecord("q-overflow-rejected")))
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessageContaining("capacity");
            var service = new QueryAuditService(tinyStore);
            assertThatThrownBy(() -> service.persist(buildRecord("q-service-rejected")))
                    .isInstanceOfSatisfying(PangrovaException.class, e -> assertThat(e.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.OBS_AUDIT_PERSISTENCE_FAILED))
                    .hasCauseInstanceOf(RejectedExecutionException.class);

            failableDataSource.setFailing(false);
            tinyStore.flushBuffer();

            // Every admitted record must persist; rejected records must not.
            int found = 0;
            for (int i = 0; i < 20; i++) {
                if (tinyStore.findById("q-overflow-" + i).isPresent()) {
                    found++;
                }
            }
            assertThat(found)
                    .as("Only records that fit in the bounded queue should be persisted")
                    .isEqualTo(5);
            assertThat(tinyStore.findById("q-overflow-rejected")).isEmpty();
        } finally {
            tinyStore.close();
        }
    }

    @Test
    void inFlightFailedBatchReservesCapacityAgainstConcurrentProducers() throws Exception {
        store.close();
        store = new JdbcAuditStore(failableDataSource, 2);
        store.save(buildRecord("q-reserved-1"));
        store.save(buildRecord("q-reserved-2"));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        failableDataSource.beforeFailure = () -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Failure release timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        };
        failableDataSource.setFailing(true);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var flush = executor.submit(store::flushBuffer);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> store.save(buildRecord("q-inflight-rejected")))
                    .isInstanceOf(RejectedExecutionException.class);
            release.countDown();
            flush.get(5, TimeUnit.SECONDS);
            failableDataSource.setFailing(false);
            store.flushBuffer();
            assertThat(store.findById("q-reserved-1")).isPresent();
            assertThat(store.findById("q-reserved-2")).isPresent();
            store.save(buildRecord("q-recovered-capacity"));
            store.flushBuffer();
            assertThat(store.findById("q-recovered-capacity")).isPresent();
        } finally {
            release.countDown();
            failableDataSource.setFailing(false);
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentClosePersistsEveryAcceptedSaveAndRejectsLaterAdmissions() throws Exception {
        var accepted = new ArrayList<String>();
        store.save(buildRecord("q-before-close"));
        accepted.add("q-before-close");
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var producer = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 250; i++) {
                    String id = "q-close-race-" + i;
                    try {
                        store.save(buildRecord(id));
                        accepted.add(id);
                    } catch (IllegalStateException e) {
                        assertThat(e).hasMessageContaining("closed");
                    }
                }
                return null;
            });
            var closer = executor.submit(() -> {
                start.await();
                store.close();
                return null;
            });
            start.countDown();
            producer.get(10, TimeUnit.SECONDS);
            closer.get(10, TimeUnit.SECONDS);
            for (String id : accepted) {
                assertThat(store.findById(id)).as("accepted audit %s", id).isPresent();
            }
            assertThatThrownBy(() -> store.save(buildRecord("q-after-close")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        } finally {
            start.countDown();
            executor.shutdownNow();
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

        store.close();

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
        for (int i = 0; i < JdbcAuditStore.BATCH_SIZE; i++) {
            store.save(buildRecord("q-trigger-" + i));
        }

        awaitPersisted(store, "q-trigger-0");

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

    private static void awaitPersisted(JdbcAuditStore store, String queryId) throws Exception {
        var observed = new CompletableFuture<Void>();
        var poller = Executors.newSingleThreadScheduledExecutor();
        try {
            poller.scheduleWithFixedDelay(() -> {
                try {
                    if (store.findById(queryId).isPresent()) {
                        observed.complete(null);
                    }
                } catch (RuntimeException e) {
                    observed.completeExceptionally(e);
                }
            }, 0, 20, TimeUnit.MILLISECONDS);
            observed.get(5, TimeUnit.SECONDS);
        } finally {
            poller.shutdownNow();
        }
    }

    private static QueryAuditRecord buildRecord(String queryId) {
        return new QueryAuditRecord(
                queryId, "test-user", "SELECT 1", "test-tenant",
                1, 2, 50L, 100L, 512L, 0L,
                100L, 50L, 1024L, 256L, 0L, 0L,
                Instant.now(), true);
    }


    private static DataSource isolatedDataSource() throws SQLException {
        String schema = "audit_" + UUID.randomUUID().toString().replace("-", "");
        PGSimpleDataSource admin = postgresDataSource(null);
        try (Connection connection = admin.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        return postgresDataSource(schema);
    }

    private static PGSimpleDataSource postgresDataSource(String schema) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        if (schema != null) {
            dataSource.setCurrentSchema(schema);
        }
        return dataSource;
    }

}
