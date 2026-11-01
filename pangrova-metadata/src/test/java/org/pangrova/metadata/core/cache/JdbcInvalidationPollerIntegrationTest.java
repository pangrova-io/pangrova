package org.pangrova.metadata.core.cache;

import org.pangrova.metadata.core.cache.InvalidationEntry.InvalidationType;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link JdbcInvalidationPoller} using TestContainers PostgreSQL.
 *
 * <p>Validates commit-ordered ID polling against the production Flyway schema,
 * including event timestamps older than any finite overlap window.</p>
 *
 * <p>Validates: Requirements D.6</p>
 */
@DisplayName("JdbcInvalidationPoller PostgreSQL Integration")
class JdbcInvalidationPollerIntegrationTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("invalidation_order");
    }

    @Test
    @DisplayName("Out-of-order inserts are all eventually processed")
    void outOfOrderInsertsAllProcessed() throws Exception {
        RecordingMetadataCache cache = new RecordingMetadataCache();
        JdbcInvalidationPoller poller = new JdbcInvalidationPoller(
                dataSource, cache, 1);
        poller.initSchema();

        // Insert entries with out-of-order updated_at timestamps.
        // Simulate: entry A committed first (earlier updated_at), entry B committed
        // second but with an earlier updated_at due to transaction ordering.
        insertWithTimestamp("cat\u001Fsch\u001Ftbl_first", "TABLE",
                Instant.now().minusSeconds(10));
        insertWithTimestamp("cat\u001Fsch\u001Ftbl_late_commit", "TABLE",
                Instant.now().minusSeconds(15)); // earlier timestamp, later commit
        insertWithTimestamp("cat\u001Fsch\u001Ftbl_recent", "TABLE",
                Instant.now().minusSeconds(5));

        // First poll should pick up all entries (watermark starts at EPOCH)
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(3);
        assertThat(poller.getLastProcessedId()).isEqualTo(3);

        // Now insert another entry with a timestamp between existing entries
        // (simulating out-of-order commit)
        insertWithTimestamp("cat\u001Fsch\u001Ftbl_out_of_order", "TABLE",
                Instant.now().minusSeconds(3));

        // A late event with a much older transaction timestamp must still be read.
        insertWithTimestamp("cat\u001Fsch\u001Ftbl_old_transaction", "TABLE",
                Instant.now().minusSeconds(3600));
        cache.tableInvalidations.clear();
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(2);
        assertThat(cache.tableInvalidations.get(0).table()).isEqualTo("tbl_out_of_order");
        assertThat(cache.tableInvalidations.get(1).table()).isEqualTo("tbl_old_transaction");
        assertThat(poller.getLastProcessedId()).isEqualTo(5);

        poller.close();
    }

    @Test
    @DisplayName("Database server time (NOW()) is used for timestamps, not application time")
    void usesServerTimeNotApplicationTime() throws Exception {
        RecordingMetadataCache cache = new RecordingMetadataCache();
        JdbcInvalidationPoller poller = new JdbcInvalidationPoller(
                dataSource, cache, 1);
        poller.initSchema();

        // Write invalidation — uses NOW() in SQL
        poller.writeInvalidation("cat\u001Fsch\u001Ftbl1", InvalidationType.TABLE);

        // Verify the timestamp was set by the database (should be close to now)
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT updated_at, NOW() as server_time FROM invalidation_log")) {
            assertThat(rs.next()).isTrue();
            Timestamp updatedAt = rs.getTimestamp("updated_at");
            Timestamp serverTime = rs.getTimestamp("server_time");
            // updated_at should be within 5 seconds of server time
            long diffMs = Math.abs(serverTime.getTime() - updatedAt.getTime());
            assertThat(diffMs).isLessThan(5000);
        }

        poller.close();
    }

    @Test
    @DisplayName("Timestamp watermark advances correctly across multiple polls")
    void timestampWatermarkAdvancesCorrectly() throws Exception {
        RecordingMetadataCache cache = new RecordingMetadataCache();
        JdbcInvalidationPoller poller = new JdbcInvalidationPoller(
                dataSource, cache, 1);
        poller.initSchema();

        // Write and poll first batch
        poller.writeInvalidation("cat\u001Fsch\u001Ftbl1", InvalidationType.TABLE);
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(1);
        Instant firstWatermark = poller.getLastProcessedTimestamp();
        assertThat(firstWatermark).isAfter(Instant.EPOCH);

        // Write and poll second batch
        poller.writeInvalidation("cat\u001Fsch\u001Ftbl2", InvalidationType.TABLE);
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(2);
        Instant secondWatermark = poller.getLastProcessedTimestamp();
        assertThat(secondWatermark).isAfterOrEqualTo(firstWatermark);

        // Third poll should find nothing new
        cache.tableInvalidations.clear();
        poller.poll();
        assertThat(cache.tableInvalidations).isEmpty();

        poller.close();
    }

    @Test
    void concurrentWriterCannotCommitPastAnUncommittedAllocatedId() throws Exception {
        assertConcurrentAppendOrdering(false);
    }

    @Test
    void rolledBackAllocationLeavesOnlyAHarmlessSequenceGap() throws Exception {
        assertConcurrentAppendOrdering(true);
    }

    private void assertConcurrentAppendOrdering(boolean rollback) throws Exception {
        var cache = new RecordingMetadataCache();
        var executor = Executors.newSingleThreadExecutor();
        try (var poller = new JdbcInvalidationPoller(dataSource, cache, 1);
             var first = dataSource.getConnection()) {
            poller.initSchema();
            first.setAutoCommit(false);
            int firstPid;
            try (var statement = first.createStatement();
                 var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                result.next();
                firstPid = result.getInt(1);
            }
            try (var statement = first.createStatement()) {
                statement.execute("INSERT INTO invalidation_log(cache_key, invalidation_type) "
                        + "VALUES ('first', 'CATALOG')");
            }
            var second = executor.submit(() -> {
                insertWithTimestamp("second", "CATALOG", Instant.now().minusSeconds(3600));
                return true;
            });
            // Verify actual database blocking, not an assumed scheduling delay.
            await().atMost(Duration.ofSeconds(10)).until(() -> hasBlockedWriter(firstPid));
            assertThat(second.isDone()).isFalse();
            poller.poll();
            assertThat(cache.catalogInvalidations).isEmpty();
            assertThat(poller.getLastProcessedId()).isZero();
            if (rollback) {
                first.rollback();
            } else {
                first.commit();
            }
            assertThat(second.get(10, TimeUnit.SECONDS)).isTrue();
            poller.poll();
            if (rollback) {
                assertThat(cache.catalogInvalidations).containsExactly("second");
            } else {
                assertThat(cache.catalogInvalidations).containsExactly("first", "second");
            }
            assertThat(poller.getLastProcessedId()).isEqualTo(2);
            poller.poll();
            assertThat(cache.catalogInvalidations).hasSize(rollback ? 1 : 2);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private boolean hasBlockedWriter(int backendPid) throws Exception {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ?=ANY(pg_blocking_pids(pid)))")) {
            statement.setInt(1, backendPid);
            try (var result = statement.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private void insertWithTimestamp(String cacheKey, String type, Instant updatedAt) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                             + "VALUES (?, ?, NOW(), ?)")) {
            ps.setString(1, cacheKey);
            ps.setString(2, type);
            ps.setTimestamp(3, Timestamp.from(updatedAt));
            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert test entry", e);
        }
    }

}
