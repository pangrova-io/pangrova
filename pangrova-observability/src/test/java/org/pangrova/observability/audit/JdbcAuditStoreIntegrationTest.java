package org.pangrova.observability.audit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for JdbcAuditStore using a real PostgreSQL instance via TestContainers.
 * Validates batch write behavior, findById, and retention cleanup.
 */
@Testcontainers
class JdbcAuditStoreIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> PG =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pangrova_test")
                    .withUsername("test")
                    .withPassword("test");

    private JdbcAuditStore store;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = createDataSource();
        createSchema(dataSource);
        store = new JdbcAuditStore(dataSource);
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void saveAndFindById() {
        QueryAuditRecord record = buildRecord("q-001", "alice", "SELECT 1", "tenant-a", true);
        store.save(record);
        store.flushBuffer();

        Optional<QueryAuditRecord> found = store.findById("q-001");
        assertThat(found).isPresent();
        assertThat(found.get().queryId()).isEqualTo("q-001");
        assertThat(found.get().userId()).isEqualTo("alice");
        assertThat(found.get().sql()).isEqualTo("SELECT 1");
        assertThat(found.get().tenantId()).isEqualTo("tenant-a");
        assertThat(found.get().success()).isTrue();
    }

    @Test
    void findByIdReturnsEmptyForMissingRecord() {
        assertThat(store.findById("nonexistent")).isEmpty();
    }

    @Test
    void batchFlushWritesMultipleRecords() {
        for (int i = 0; i < 50; i++) {
            store.save(buildRecord("q-batch-" + i, "user-" + i, "SELECT " + i, "t-1", true));
        }
        store.flushBuffer();

        for (int i = 0; i < 50; i++) {
            assertThat(store.findById("q-batch-" + i)).isPresent();
        }
    }

    @Test
    void batchSizeTriggersAutoFlush() throws Exception {
        List<QueryAuditRecord> records = new ArrayList<>();
        for (int i = 0; i < JdbcAuditStore.BATCH_SIZE; i++) {
            records.add(buildRecord("q-auto-" + i, "user", "SELECT 1", "t-1", true));
        }
        for (QueryAuditRecord r : records) {
            store.save(r);
        }
        awaitPersisted("q-auto-" + (JdbcAuditStore.BATCH_SIZE - 1));

        // At least some records should be persisted due to batch size trigger
        int found = 0;
        for (int i = 0; i < JdbcAuditStore.BATCH_SIZE; i++) {
            if (store.findById("q-auto-" + i).isPresent()) {
                found++;
            }
        }
        assertThat(found).isEqualTo(JdbcAuditStore.BATCH_SIZE);
    }

    @Test
    void jdbcDoesNotHoldMonitorAndCloseWaitsForActualFlushThenDrainsAllBatches() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var gated = new JdbcAuditStoreTestDataSources.GatedDataSource(dataSource, release, entered);
        var concurrent = new JdbcAuditStore(gated, 201);
        var executor = Executors.newFixedThreadPool(3, task -> new Thread(task, "audit-concurrency-test"));
        try {
            concurrent.save(buildRecord("q-gated-0", "user", "SELECT 1", "tenant", true));
            var flush = executor.submit(concurrent::flushBuffer);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(gated.heldMonitorCount()).isZero();
            executor.submit(concurrent::flushBuffer).get(1, TimeUnit.SECONDS);
            for (int i = 1; i <= 200; i++) {
                concurrent.save(buildRecord("q-gated-" + i, "user", "SELECT 1", "tenant", true));
            }
            assertThatThrownBy(() -> concurrent.save(
                    buildRecord("q-overflow", "user", "SELECT 1", "tenant", true)))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
            var close = executor.submit(concurrent::close);
            assertThatThrownBy(() -> close.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            flush.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            for (int i = 0; i <= 200; i++) {
                assertThat(store.findById("q-gated-" + i)).isPresent();
            }
            assertThat(store.findById("q-overflow")).isEmpty();
        } finally {
            release.countDown();
            executor.shutdownNow();
            concurrent.close();
        }
    }

    private void awaitPersisted(String queryId) throws Exception {
        CompletableFuture<Void> persisted = new CompletableFuture<>();
        var poller = new ScheduledThreadPoolExecutor(1, task -> new Thread(task, "audit-persistence-test"));
        poller.setRemoveOnCancelPolicy(true);
        try {
            poller.scheduleWithFixedDelay(() -> {
                try {
                    if (store.findById(queryId).isPresent()) {
                        persisted.complete(null);
                    }
                } catch (RuntimeException e) {
                    persisted.completeExceptionally(e);
                }
            }, 0, 20, TimeUnit.MILLISECONDS);
            persisted.get(5, TimeUnit.SECONDS);
        } finally {
            poller.shutdownNow();
        }
    }

    @Test
    void deleteOlderThanRemovesExpiredRecords() {
        Instant oldTime = Instant.now().minusSeconds(90 * 24 * 60 * 60L);
        QueryAuditRecord oldRecord = new QueryAuditRecord(
                "q-old", "user", "SELECT 1", "t-1", 1, 2,
                100L, 200L, 1024L, 0L, 1000L, 500L,
                2048L, 1024L, 0L, 0L, oldTime, true);
        QueryAuditRecord recentRecord = buildRecord("q-recent", "user", "SELECT 2", "t-1", true);

        store.save(oldRecord);
        store.save(recentRecord);
        store.flushBuffer();

        long deleted = store.deleteOlderThan(30);
        assertThat(deleted).isEqualTo(1);
        assertThat(store.findById("q-old")).isEmpty();
        assertThat(store.findById("q-recent")).isPresent();
    }

    @Test
    void saveHandlesNullSqlAndTenantId() {
        QueryAuditRecord record = new QueryAuditRecord(
                "q-null", "user", null, null, 0, 0,
                0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, Instant.now(), false);
        store.save(record);
        store.flushBuffer();

        Optional<QueryAuditRecord> found = store.findById("q-null");
        assertThat(found).isPresent();
        assertThat(found.get().sql()).isNull();
        assertThat(found.get().tenantId()).isNull();
    }

    @Test
    void savePreservesAllMetricFields() {
        QueryAuditRecord record = new QueryAuditRecord(
                "q-metrics", "analyst", "SELECT count(*) FROM orders", "prod-tenant",
                3, 12, 5000L, 8000L, 1_073_741_824L, 536_870_912L,
                10_000_000L, 1_000L, 4_294_967_296L, 1024L,
                2_147_483_648L, 1_073_741_824L, Instant.now(), true);
        store.save(record);
        store.flushBuffer();

        QueryAuditRecord found = store.findById("q-metrics").orElseThrow();
        assertThat(found.stageCount()).isEqualTo(3);
        assertThat(found.taskCount()).isEqualTo(12);
        assertThat(found.cpuTimeMs()).isEqualTo(5000L);
        assertThat(found.wallTimeMs()).isEqualTo(8000L);
        assertThat(found.peakMemoryBytes()).isEqualTo(1_073_741_824L);
        assertThat(found.spilledBytes()).isEqualTo(536_870_912L);
        assertThat(found.inputRows()).isEqualTo(10_000_000L);
        assertThat(found.outputRows()).isEqualTo(1_000L);
        assertThat(found.inputBytes()).isEqualTo(4_294_967_296L);
        assertThat(found.outputBytes()).isEqualTo(1024L);
        assertThat(found.shuffleReadBytes()).isEqualTo(2_147_483_648L);
        assertThat(found.shuffleWriteBytes()).isEqualTo(1_073_741_824L);
        assertThat(found.success()).isTrue();
    }

    private static QueryAuditRecord buildRecord(String queryId, String userId,
                                                 String sql, String tenantId, boolean success) {
        return new QueryAuditRecord(
                queryId, userId, sql, tenantId, 1, 4,
                100L, 200L, 1024L, 0L, 1000L, 500L,
                2048L, 1024L, 0L, 0L, Instant.now(), success);
    }

    private static DataSource createDataSource() {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        return ds;
    }

    private static void createSchema(DataSource dataSource) throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS query_audit_records (
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
    }
}
