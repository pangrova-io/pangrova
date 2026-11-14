package org.pangrova.persistence.catalog.postgresql.invalidation;

import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.cache.InvalidationEntry.InvalidationType;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.metadata.api.cache.InvalidationProgressRegistry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * PostgreSQL integration tests for {@link PostgresInvalidationPoller}.
 */
class JdbcInvalidationPollerTest {

    private DataSource dataSource;
    private RecordingMetadataCache cache;
    private PostgresInvalidationPoller poller;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        cache = new RecordingMetadataCache();
        poller = new PostgresInvalidationPoller(dataSource, cache, 1);
        poller.validateSchema();
    }

    @AfterEach
    void tearDown() {
        poller.close();
    }

    @Test
    void initSchemaValidatesProductionMigration() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM invalidation_log")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isZero();
        }
    }

    @Test
    void initSchemaIsIdempotent() {
        // Second call should not throw
        poller.validateSchema();
    }

    @Test
    void writeInvalidationInsertsRow() throws SQLException {
        poller.writeInvalidation("test_catalog\u001Ftest_schema\u001Ftest_table",
                InvalidationType.TABLE);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM invalidation_log")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("cache_key"))
                    .isEqualTo("test_catalog\u001Ftest_schema\u001Ftest_table");
            assertThat(rs.getString("invalidation_type")).isEqualTo("TABLE");
            assertThat(rs.getTimestamp("created_at")).isNotNull();
        }
    }

    @Test
    void pollProcessesTableInvalidation() {
        poller.writeInvalidation("cat\u001Fsch\u001Ftbl", InvalidationType.TABLE);

        poller.poll();

        assertThat(cache.tableInvalidations).hasSize(1);
        RecordingMetadataCache.TableKey key = cache.tableInvalidations.get(0);
        assertThat(key.catalog()).isEqualTo("cat");
        assertThat(key.schema()).isEqualTo("sch");
        assertThat(key.table()).isEqualTo("tbl");
        assertThat(poller.getLastProcessedId()).isEqualTo(1);
    }

    @Test
    void pollProcessesCatalogInvalidation() {
        poller.writeInvalidation("my_catalog", InvalidationType.CATALOG);

        poller.poll();

        assertThat(cache.catalogInvalidations).containsExactly("my_catalog");
        assertThat(poller.getLastProcessedId()).isEqualTo(1);
    }

    @Test
    void pollSkipsMalformedTableKey() {
        poller.writeInvalidation("bad_key_no_separators", InvalidationType.TABLE);

        poller.poll();

        // Malformed key is skipped — no table invalidation applied
        assertThat(cache.tableInvalidations).isEmpty();
        assertThat(cache.catalogInvalidations).isEmpty();
        // But the ID is still advanced so we don't re-process
        assertThat(poller.getLastProcessedId()).isEqualTo(1);
    }

    @Test
    void pollAdvancesHighWaterMark() {
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);
        poller.writeInvalidation("c2\u001Fs2\u001Ft2", InvalidationType.TABLE);

        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(2);
        assertThat(poller.getLastProcessedId()).isEqualTo(2);

        // Second poll should find nothing new
        cache.tableInvalidations.clear();
        poller.poll();
        assertThat(cache.tableInvalidations).isEmpty();
        assertThat(poller.getLastProcessedId()).isEqualTo(2);
    }

    @Test
    void pollPicksUpNewEntriesAfterPreviousPoll() {
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(1);

        // Write new entry after first poll
        poller.writeInvalidation("c2\u001Fs2\u001Ft2", InvalidationType.TABLE);
        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(2);
        assertThat(poller.getLastProcessedId()).isEqualTo(2);
    }

    @Test
    void pollHandlesMixedInvalidationTypes() {
        poller.writeInvalidation("cat\u001Fsch\u001Ftbl", InvalidationType.TABLE);
        poller.writeInvalidation("whole_catalog", InvalidationType.CATALOG);
        poller.writeInvalidation("cat2\u001Fsch2\u001Ftbl2", InvalidationType.TABLE);

        poller.poll();

        assertThat(cache.tableInvalidations).hasSize(2);
        assertThat(cache.catalogInvalidations).containsExactly("whole_catalog");
        assertThat(poller.getLastProcessedId()).isEqualTo(3);
    }

    @Test
    void writeInvalidationRejectsNullCacheKey() {
        assertThatThrownBy(() -> poller.writeInvalidation(null, InvalidationType.TABLE))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("cacheKey");
    }

    @Test
    void writeInvalidationRejectsNullType() {
        assertThatThrownBy(() -> poller.writeInvalidation("key", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("invalidationType");
    }

    @Test
    void constructorRejectsNullDataSource() {
        assertThatThrownBy(() -> new PostgresInvalidationPoller(null, cache))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dataSource");
    }

    @Test
    void constructorRejectsNullCache() {
        assertThatThrownBy(() -> new PostgresInvalidationPoller(dataSource, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("cache");
    }

    @Test
    void constructorRejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new PostgresInvalidationPoller(dataSource, cache, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollIntervalSeconds");
    }

    @Test
    void closeIsIdempotent() {
        poller.close();
        // Second close should not throw
        poller.close();
    }

    @Test
    void pollSurvivesAfterDatabaseError() throws SQLException {
        // Write a valid entry
        poller.writeInvalidation("c\u001Fs\u001Ft", InvalidationType.TABLE);

        // Hide the table to simulate a transient schema lookup failure without
        // recreating a competing schema or resetting the publication sequence.
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE invalidation_log RENAME TO unavailable_invalidation_log");
        }

        // Poll should not throw — it catches and logs
        poller.poll();
        assertThat(cache.tableInvalidations).isEmpty();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE unavailable_invalidation_log RENAME TO invalidation_log");
        }
        // Recover the canonical table and write another entry.
        poller.validateSchema();
        poller.writeInvalidation("c2\u001Fs2\u001Ft2", InvalidationType.TABLE);

        poller.poll();
        assertThat(cache.tableInvalidations).hasSize(2);
        assertThat(poller.getLastProcessedId()).isEqualTo(2);
    }

    @Test
    void refusesMissingCommitOrderGate() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE invalidation_log DISABLE TRIGGER invalidation_append_commit_gate");
        }
        assertThatThrownBy(poller::validateSchema)
                .isInstanceOf(org.pangrova.persistence.catalog.api.exception.MetastoreException.class)
                .hasRootCauseMessage("Canonical invalidation commit-order trigger is missing or disabled");
    }

    /** Inserts n distinct CATALOG invalidations. */
    private void insertCatalogInvalidations(int n) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO invalidation_log(cache_key, invalidation_type) "
                    + "SELECT 'catalog_' || n, 'CATALOG' FROM generate_series(1, " + n + ") AS n");
        }
    }

    /**
     * A single tick drains the whole backlog. Draining one page per tick capped
     * throughput at page-size/interval and let the watermark lag indefinitely.
     */
    @Test
    void onePollDrainsTheWholeBacklogWithoutDroppingEntries() throws SQLException {
        insertCatalogInvalidations(1001);

        poller.poll();

        assertThat(cache.catalogInvalidations).hasSize(1001);
        assertThat(poller.getLastProcessedId()).isEqualTo(1001);
        // A further tick must be a no-op rather than re-applying anything.
        poller.poll();
        assertThat(cache.catalogInvalidations).hasSize(1001);
    }

    /**
     * The drain loop stays bounded so a continuously-writing producer cannot
     * occupy the scheduler thread; the following tick resumes from the watermark.
     */
    @Test
    void onePollIsBoundedSoASteadyWriterCannotStarveTheScheduler() throws SQLException {
        final int bound = PostgresInvalidationPoller.MAX_PAGES_PER_POLL
                * PostgresInvalidationPoller.POLL_PAGE_SIZE;
        insertCatalogInvalidations(bound + 1);

        poller.poll();

        assertThat(cache.catalogInvalidations).hasSize(bound);
        assertThat(poller.getLastProcessedId()).isEqualTo(bound);
        poller.poll();
        assertThat(cache.catalogInvalidations).hasSize(bound + 1);
        assertThat(poller.getLastProcessedId()).isEqualTo(bound + 1);
    }

    /**
     * A restart must resume from this coordinator's durable watermark. Replaying
     * from zero re-evicted every cached entry and pinned the cleanup floor at zero
     * until the backlog drained.
     */
    @Test
    void startResumesFromThisCoordinatorsReportedWatermark() throws SQLException {
        insertCatalogInvalidations(3);
        poller.poll();
        long reported = poller.getLastProcessedId();
        assertThat(reported).isEqualTo(3);

        var registry = new RecordingInvalidationProgressRegistry();
        registry.register("local");
        registry.updateLastProcessedInvalidationId("local", reported);

        // A fresh poller stands in for the restarted process.
        var restarted = new PostgresInvalidationPoller(dataSource, new RecordingMetadataCache(),
                1, registry, Optional.of("local"));
        restarted.start();
        try {
            assertThat(restarted.getLastProcessedId()).isEqualTo(reported);
        } finally {
            restarted.close();
        }
    }

    @Test
    void unavailableWatermarkFailsStartAndCanRetryFromItsStoredPosition() throws SQLException {
        insertCatalogInvalidations(2);
        var unavailable = new java.util.concurrent.atomic.AtomicBoolean(true);
        var registry = new RecordingInvalidationProgressRegistry() {
            @Override public long lastProcessedInvalidationId(String id) {
                if (unavailable.get()) throw new IllegalStateException("registry offline");
                return super.lastProcessedInvalidationId(id);
            }
        };
        registry.register("local");
        registry.updateLastProcessedInvalidationId("local", 1);
        var resumedCache = new RecordingMetadataCache();
        try (var reader = new PostgresInvalidationPoller(dataSource, resumedCache, 1, registry, Optional.of("local"))) {
            assertThatThrownBy(reader::start).isInstanceOf(IllegalStateException.class).hasMessage("registry offline");
            assertThat(registry.lastProcessed("local")).isEqualTo(1);
            unavailable.set(false);
            reader.start();
            await().atMost(5, SECONDS).until(() -> reader.getLastProcessedId() == 2);
            assertThat(resumedCache.catalogInvalidations).hasSize(1);
        }
    }

    @Test
    void startWithoutAWatermarkBeginsAtZero() throws SQLException {
        insertCatalogInvalidations(2);
        var registry = new RecordingInvalidationProgressRegistry();
        registry.register("local");

        var freshCache = new RecordingMetadataCache();
        var fresh = new PostgresInvalidationPoller(dataSource, freshCache,
                1, registry, Optional.of("local"));
        fresh.start();
        try {
            await().atMost(5, SECONDS).until(() -> fresh.getLastProcessedId() == 2);
            assertThat(freshCache.catalogInvalidations).hasSize(2);
        } finally {
            fresh.close();
        }
    }

    @Test
    void startAndPollIntegration() throws InterruptedException {
        poller.writeInvalidation("cat\u001Fsch\u001Ftbl", InvalidationType.TABLE);

        poller.start();
        // Wait for at least one poll cycle
        await().atMost(3, SECONDS).until(() -> !cache.tableInvalidations.isEmpty());

        assertThat(cache.tableInvalidations).isNotEmpty();
        assertThat(poller.getLastProcessedId()).isPositive();
    }

    // ── cleanup() tests ─────────────────────────────────────────────

    @Test
    void cleanupDeletesOldProcessedEntries() throws SQLException {
        // Insert entries and backdate them to 48 hours ago
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);
        poller.writeInvalidation("c2\u001Fs2\u001Ft2", InvalidationType.TABLE);
        backdateAllEntries(48);

        // Poll to advance lastProcessedId
        poller.poll();
        assertThat(poller.getLastProcessedId()).isEqualTo(2);

        int deleted = poller.cleanup();

        assertThat(deleted).isEqualTo(2);
        assertThat(countRows()).isZero();
    }

    @Test
    void cleanupPreservesRecentEntries() throws SQLException {
        // Insert entries (they have current timestamps, within 24h TTL)
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);
        poller.writeInvalidation("c2\u001Fs2\u001Ft2", InvalidationType.TABLE);

        // Poll to advance lastProcessedId
        poller.poll();
        assertThat(poller.getLastProcessedId()).isEqualTo(2);

        int deleted = poller.cleanup();

        assertThat(deleted).as("Recent entries should not be deleted").isZero();
        assertThat(countRows()).isEqualTo(2);
    }

    @Test
    void cleanupRespectsLastProcessedIdSafetyBound() throws SQLException {
        // Insert 3 entries and backdate all to 48 hours ago
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);
        poller.writeInvalidation("c2\u001Fs2\u001Ft2", InvalidationType.TABLE);
        poller.writeInvalidation("c3\u001Fs3\u001Ft3", InvalidationType.TABLE);
        backdateAllEntries(48);

        // Poll all, then write a new entry that won't be polled
        poller.poll();
        assertThat(poller.getLastProcessedId()).isEqualTo(3);

        // Write a 4th entry (old timestamp but id=4 > lastProcessedId=3 won't matter
        // since cleanup uses id <= lastProcessedId AND created_at < cutoff)
        poller.writeInvalidation("c4\u001Fs4\u001Ft4", InvalidationType.TABLE);
        backdateAllEntries(48);

        // cleanup should delete entries 1-3 (old + id <= 3) but NOT entry 4 (id=4 > lastProcessedId=3)
        int deleted = poller.cleanup();

        assertThat(deleted).isEqualTo(3);
        assertThat(countRows()).isEqualTo(1);
    }

    @Test
    void cleanupSkipsWhenNoEntriesProcessed() {
        // Write entries but don't poll (lastProcessedId = 0)
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);

        int deleted = poller.cleanup();

        assertThat(deleted).as("cleanup must skip when lastProcessedId is 0").isZero();
    }

    @Test
    void cleanupIsIdempotent() throws SQLException {
        poller.writeInvalidation("c1\u001Fs1\u001Ft1", InvalidationType.TABLE);
        backdateAllEntries(48);
        poller.poll();

        int first = poller.cleanup();
        int second = poller.cleanup();

        assertThat(first).isEqualTo(1);
        assertThat(second).as("Second cleanup should find nothing to delete").isZero();
    }

    @Test
    void cleanupCannotPassAnotherRegisteredCoordinatorsProgress() throws SQLException {
        var registry = new RecordingInvalidationProgressRegistry();
        registry.register("local");
        registry.register("remote");
        poller.close();
        poller = new PostgresInvalidationPoller(dataSource, cache, 1, registry, Optional.of("local"));
        poller.writeInvalidation("first", InvalidationType.CATALOG);
        poller.writeInvalidation("second", InvalidationType.CATALOG);
        backdateAllEntries(48);
        poller.poll();
        assertThat(registry.lastProcessed("local")).isEqualTo(2);
        assertThat(poller.cleanup()).as("Unknown remote progress must retain the log").isZero();
        registry.updateLastProcessedInvalidationId("remote", 1);
        assertThat(poller.cleanup()).isEqualTo(1);
        assertThat(countRows()).isEqualTo(1);
        registry.updateLastProcessedInvalidationId("remote", 2);
        assertThat(poller.cleanup()).isEqualTo(1);
        assertThat(countRows()).isZero();
    }

    // ── Helpers ──

    private void backdateAllEntries(int hoursAgo) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "UPDATE invalidation_log SET created_at = "
                             + "CURRENT_TIMESTAMP - (? * INTERVAL '1 hour')")) {
            statement.setInt(1, hoursAgo);
            statement.executeUpdate();
        }
    }

    private int countRows() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM invalidation_log")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /** In-memory progress double: the poller only needs peer watermarks from the composition layer. */
    private static class RecordingInvalidationProgressRegistry implements InvalidationProgressRegistry {

        private final Map<String, Long> watermarks = new LinkedHashMap<>();

        void register(String coordinatorId) {
            watermarks.putIfAbsent(coordinatorId, -1L);
        }

        long lastProcessed(String coordinatorId) {
            return watermarks.getOrDefault(coordinatorId, -1L);
        }

        @Override
        public void updateLastProcessedInvalidationId(String coordinatorId, long invalidationId) {
            if (watermarks.containsKey(coordinatorId)) {
                watermarks.put(coordinatorId, invalidationId);
            }
        }

        @Override
        public long minimumReportedInvalidationId() {
            return watermarks.values().stream().mapToLong(Long::longValue).min().orElse(Long.MAX_VALUE);
        }

        @Override
        public long lastProcessedInvalidationId(String coordinatorId) {
            return lastProcessed(coordinatorId);
        }
    }
}
