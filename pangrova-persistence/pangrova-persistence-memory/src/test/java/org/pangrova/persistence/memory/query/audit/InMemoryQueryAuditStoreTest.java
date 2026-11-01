package org.pangrova.persistence.memory.query.audit;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.query.audit.QueryAuditRecord;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that InMemoryQueryAuditStore.save() logs key audit fields via structured logging.
 * Uses a real Logback ListAppender to capture log output (no mocks).
 *
 * Validates: Requirements 17.3
 */
class InMemoryQueryAuditStoreTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger auditLogger;

    @BeforeEach
    void setUp() {
        auditLogger = (Logger) LoggerFactory.getLogger("pangrova.audit.query");
        appender = new ListAppender<>();
        appender.start();
        auditLogger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        auditLogger.detachAppender(appender);
        appender.stop();
    }

    @Test
    void saveLogsQueryIdAndUserId() {
        var store = new InMemoryQueryAuditStore();
        var record = buildRecord("q-100", "alice", "SELECT 1", "t-1", true);

        store.save(record);

        assertThat(appender.list).hasSize(1);
        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("query=q-100");
        assertThat(msg).contains("user=alice");
    }

    @Test
    void saveLogsTenantIdAndSql() {
        var store = new InMemoryQueryAuditStore();
        var record = buildRecord("q-200", "bob", "INSERT INTO t VALUES(1)", "tenant-x", false);

        store.save(record);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("tenant=tenant-x");
        assertThat(msg).contains("sql=INSERT INTO t VALUES(1)");
        assertThat(msg).contains("success=false");
    }

    @Test
    void saveLogsResourceMetrics() {
        var store = new InMemoryQueryAuditStore();
        var record = new QueryAuditRecord(
                "q-300", "user-1", "SELECT *", "t-1",
                3, 6,
                500L, 1000L, 4096L, 128L,
                10000L, 5000L, 8192L, 2048L,
                0L, 0L,
                Instant.now(), true);

        store.save(record);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("cpu=500ms");
        assertThat(msg).contains("wall=1000ms");
        assertThat(msg).contains("peak_memory=4096");
        assertThat(msg).contains("spilled=128");
        assertThat(msg).contains("input_rows=10000");
        assertThat(msg).contains("output_rows=5000");
    }

    @Test
    void saveLogsStageAndTaskCount() {
        var store = new InMemoryQueryAuditStore();
        var record = buildRecord("q-400", "user-2", "SELECT 1", "t-2", true);

        store.save(record);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("stages=2");
        assertThat(msg).contains("tasks=4");
    }

    @Test
    void saveLogsAtInfoLevel() {
        var store = new InMemoryQueryAuditStore();
        store.save(buildRecord("q-500", "user-3", "SELECT 1", "t-3", true));

        assertThat(appender.list.get(0).getLevel()).isEqualTo(Level.INFO);
    }

    @Test
    void saveTruncatesLongSql() {
        var store = new InMemoryQueryAuditStore();
        String longSql = "SELECT " + "x".repeat(300);
        var record = buildRecord("q-600", "user-4", longSql, "t-4", true);

        store.save(record);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("...");
        assertThat(msg).doesNotContain(longSql);
    }

    @Test
    void saveHandlesNullSql() {
        var store = new InMemoryQueryAuditStore();
        var record = buildRecord("q-700", "user-5", null, "t-5", true);

        store.save(record);

        String msg = appender.list.get(0).getFormattedMessage();
        assertThat(msg).contains("sql=null");
    }

    @Test
    void findByIdAlwaysReturnsEmpty() {
        var store = new InMemoryQueryAuditStore();
        assertThat(store.findById("any")).isEmpty();
    }

    @Test
    void deleteOlderThanAlwaysReturnsZero() {
        var store = new InMemoryQueryAuditStore();
        assertThat(store.deleteOlderThan(30)).isZero();
    }

    // -- Helpers --

    private static QueryAuditRecord buildRecord(String queryId, String userId,
                                                  String sql, String tenantId,
                                                  boolean success) {
        return new QueryAuditRecord(
                queryId, userId, sql, tenantId,
                2, 4,
                100L, 200L, 1024L, 0L,
                500L, 100L, 2048L, 512L,
                0L, 0L,
                Instant.now(), success);
    }
}
