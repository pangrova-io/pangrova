package org.pangrova.metadata.cache;

import org.pangrova.metadata.cache.InvalidationEntry.InvalidationType;
import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.testing.ContainerTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Integration test for {@link JdbcInvalidationPoller} using TestContainers PostgreSQL.
 *
 * <p>Validates the timestamp-based high watermark behavior that prevents
 * out-of-order misses. Each coordinator polls independently; cache invalidation
 * is inherently idempotent so no cross-coordinator coordination is needed.</p>
 *
 * <p>Validates: Requirements D.6</p>
 */
@Testcontainers
@DisplayName("JdbcInvalidationPoller PostgreSQL Integration")
class JdbcInvalidationPollerIntegrationTest extends ContainerTestBase {

    @SuppressWarnings("resource")
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test");

    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        assumeTrue(isDockerReady(), "Docker not available");
        dataSource = new EmbeddedDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        truncateTable();
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

        // Second poll should pick up the new entry thanks to the overlap window
        cache.tableInvalidations.clear();
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(1);
        assertThat(cache.tableInvalidations.get(0).table()).isEqualTo("tbl_out_of_order");

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

        // Small delay to ensure distinct timestamps
        await().pollDelay(50, MILLISECONDS).until(() -> true);

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

    private void truncateTable() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS invalidation_log");
        } catch (Exception e) {
            throw new RuntimeException("Failed to truncate invalidation_log", e);
        }
    }
}
