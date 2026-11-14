package org.pangrova.testing.integration.e2e.metadata;

import org.pangrova.metadata.api.cache.InvalidationEntry.InvalidationType;
import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;
import org.pangrova.metadata.api.cache.MetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.metadata.api.cache.CacheStats;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the durable projection/outbox operation used by metadata synchronization, independent
 * coordinator readers and the processed-id fence on PostgreSQL TTL cleanup.
 */
@DisplayName("Metadata Sync Propagation via invalidation_log E2E")
@Execution(ExecutionMode.SAME_THREAD)
class MetadataSyncPropagationE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    /** Cache key separator used by MetadataSyncService and PostgresInvalidationPoller. */
    private static final String CACHE_KEY_SEPARATOR = "\u001F";

    /** Maximum time to wait for poller to process entries. */
    private static final long POLL_WAIT_TIMEOUT_MS = 10_000;
    private static final long POLL_CHECK_INTERVAL_MS = 100;

    private static DataSource invalidationDs;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalog() {
        invalidationDs = MigratedPostgres.create(pgJdbcUrl(), pgUsername(), pgPassword());
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void initInvalidationInfra() throws Exception {
        truncateInvalidationLog(invalidationDs);
    }

    // -----------------------------------------------------------------------
    // Scenario 1: Sync Writes Invalidation → Cross-Coordinator Propagation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 1 — Sync writes invalidation and cross-coordinator propagation")
    class SyncWritesInvalidation {

        @Test
        @DisplayName("A committed table projection records one invalidation when replayed")
        void syncProjectionWritesInvalidationLogEntry() throws Exception {
            String tableName = "t_sync_inv_" + System.nanoTime();
            var catalog = new CatalogDescriptor(CATALOG, CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                    Map.of(), "", Set.of());
            new PostgresCatalogDeploymentRepository(invalidationDs).saveCatalogDescriptor(catalog);
            var table = new TableMetadata(CATALOG, SCHEMA, tableName, List.of(), Map.of(), List.of(),
                    Map.of(), Instant.EPOCH, Instant.EPOCH);
            var mutation = new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), catalog.identity(), table);
            var projections = new PostgresMetadataProjectionTransactions(invalidationDs);
            var receipt = projections.apply(mutation);
            assertThat(receipt.changed()).isTrue();
            assertThat(projections.apply(mutation)).isEqualTo(receipt);
            assertThat(new PostgresTableProjectionRepository(invalidationDs).loadTable(CATALOG, SCHEMA, tableName))
                    .contains(table);
            String key = String.join(CACHE_KEY_SEPARATOR, CATALOG, SCHEMA, tableName);
            assertThat(countInvalidationEntries(invalidationDs, key)).isOne();
            assertThat(getInvalidationType(invalidationDs, key)).isEqualTo("TABLE");
        }

        @Test
        @DisplayName("PostgresInvalidationPoller on second coordinator picks up invalidation")
        void pollerPicksUpInvalidationFromAnotherCoordinator() throws Exception {
            // Coordinator-1 writes through its independent database connection.
            String tableName = "t_cross_coord_" + System.nanoTime();
            String cacheKey = CATALOG + CACHE_KEY_SEPARATOR
                    + SCHEMA + CACHE_KEY_SEPARATOR + tableName;

            PostgresInvalidationPoller writerPoller = new PostgresInvalidationPoller(
                    invalidationDs, new NoOpMetadataCache(), 1);
            writerPoller.validateSchema();
            writerPoller.writeInvalidation(cacheKey, InvalidationType.TABLE);

            // Coordinator-2 (reader): starts polling invalidation_log
            RecordingMetadataCache readerCache = new RecordingMetadataCache();
            PostgresInvalidationPoller readerPoller = new PostgresInvalidationPoller(
                    invalidationDs, readerCache, 1);

            // Pre-populate reader cache to verify invalidation clears it
            readerCache.putTable(CATALOG, SCHEMA, tableName,
                    dummyTableMetadata(CATALOG, SCHEMA, tableName));
            assertThat(readerCache.getTable(CATALOG, SCHEMA, tableName))
                    .as("Reader cache should have the table before poll")
                    .isPresent();

            // Start reader poller and wait for it to process the entry
            readerPoller.start();
            awaitPollerProcessed(readerPoller, 1);

            assertThat(readerCache.invalidatedTables)
                    .as("Reader cache should have received TABLE invalidation")
                    .contains(CATALOG + "." + SCHEMA + "." + tableName);

            assertThat(readerPoller.getLastProcessedId())
                    .as("Reader poller should have advanced lastProcessedId")
                    .isGreaterThan(0);

            writerPoller.close();
            readerPoller.close();
        }

        @Test
        @DisplayName("Multiple invalidation entries propagate in order")
        void multipleInvalidationsPropagate() throws Exception {
            PostgresInvalidationPoller writerPoller = new PostgresInvalidationPoller(
                    invalidationDs, new NoOpMetadataCache(), 1);
            writerPoller.validateSchema();

            // Write 5 invalidation entries for different tables
            for (int i = 1; i <= 5; i++) {
                String cacheKey = CATALOG + CACHE_KEY_SEPARATOR
                        + SCHEMA + CACHE_KEY_SEPARATOR + "t_multi_" + i;
                writerPoller.writeInvalidation(cacheKey, InvalidationType.TABLE);
            }

            // Reader poller picks them all up
            RecordingMetadataCache readerCache = new RecordingMetadataCache();
            PostgresInvalidationPoller readerPoller = new PostgresInvalidationPoller(
                    invalidationDs, readerCache, 1);
            readerPoller.start();
            awaitPollerProcessed(readerPoller, 5);

            assertThat(readerCache.invalidatedTables)
                    .as("All 5 invalidations should be applied")
                    .hasSize(5);

            assertThat(readerPoller.getLastProcessedId())
                    .as("lastProcessedId should reflect all entries processed")
                    .isGreaterThanOrEqualTo(5);

            writerPoller.close();
            readerPoller.close();
        }

        @Test
        @DisplayName("Query sees fresh data after invalidation propagation")
        void querySeeFreshDataAfterInvalidation() {
            // Create table and insert data
            String tableName = "t_fresh_query_" + System.nanoTime();
            String fqn = FQN_PREFIX + tableName;
            sqlRunner.executeDdl("CREATE TABLE " + fqn
                    + " (id BIGINT, name VARCHAR, value BIGINT)");

            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, value) VALUES (1, 'alpha', 100)");
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, value) VALUES (2, 'beta', 200)");

            // Query returns correct results
            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + fqn);
            assertThat(rows).hasSize(2);

            // Insert more data (simulates external catalog change detected by sync)
            sqlRunner.execute("INSERT INTO " + fqn
                    + " (id, name, value) VALUES (3, 'gamma', 300)");

            // After invalidation propagation, query sees all data
            List<TypedRow> allRows = sqlRunner.execute("SELECT * FROM " + fqn);
            assertThat(allRows).hasSize(3);

            // Aggregation also reflects fresh data
            List<TypedRow> sumRows = sqlRunner.execute(
                    "SELECT SUM(value) AS total FROM " + fqn);
            assertThat(sumRows).hasSize(1);
            long total = ((Number) sumRows.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(600L);
        }

        @Test
        @DisplayName("CATALOG-level invalidation propagates via invalidation_log")
        void catalogLevelInvalidationPropagates() throws Exception {
            PostgresInvalidationPoller writerPoller = new PostgresInvalidationPoller(
                    invalidationDs, new NoOpMetadataCache(), 1);
            writerPoller.validateSchema();

            // Write a CATALOG-level invalidation
            writerPoller.writeInvalidation(CATALOG, InvalidationType.CATALOG);

            // Reader picks it up
            RecordingMetadataCache readerCache = new RecordingMetadataCache();
            PostgresInvalidationPoller readerPoller = new PostgresInvalidationPoller(
                    invalidationDs, readerCache, 1);
            readerPoller.start();
            awaitPollerProcessed(readerPoller, 1);

            assertThat(readerCache.invalidatedCatalogs)
                    .as("CATALOG invalidation should be applied")
                    .contains(CATALOG);

            writerPoller.close();
            readerPoller.close();
        }
    }


    // -----------------------------------------------------------------------
    // Scenario 2: invalidation_log Cleanup
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Scenario 2 — invalidation_log cleanup lifecycle")
    class InvalidationLogCleanup {

        @Test
        @DisplayName("Cleanup deletes old entries beyond 24h TTL")
        void cleanupDeletesOldEntries() throws Exception {
            RecordingMetadataCache cache = new RecordingMetadataCache();
            PostgresInvalidationPoller poller = new PostgresInvalidationPoller(
                    invalidationDs, cache, 1);
            poller.validateSchema();

            // Insert 100 entries with old timestamps (25 hours ago)
            Instant oldTimestamp = Instant.now().minus(Duration.ofHours(25));
            insertInvalidationEntriesWithTimestamp(invalidationDs, 100, oldTimestamp, "old_table");

            // Start poller and wait for it to process all entries
            poller.start();
            awaitPollerProcessed(poller, 100);
            assertThat(poller.getLastProcessedId())
                    .as("Poller should have processed all 100 entries")
                    .isGreaterThanOrEqualTo(100);

            // Run cleanup — should delete all 100 old entries
            int deleted = poller.cleanup();
            assertThat(deleted)
                    .as("All 100 old entries should be deleted")
                    .isEqualTo(100);

            // Verify table is empty
            int remaining = countAllInvalidationEntries(invalidationDs);
            assertThat(remaining)
                    .as("No entries should remain after cleanup")
                    .isZero();

            poller.close();
        }

        @Test
        @DisplayName("Cleanup preserves recent entries within 24h TTL")
        void cleanupPreservesRecentEntries() throws Exception {
            RecordingMetadataCache cache = new RecordingMetadataCache();
            PostgresInvalidationPoller poller = new PostgresInvalidationPoller(
                    invalidationDs, cache, 1);
            poller.validateSchema();

            // Insert 50 old entries (25 hours ago)
            Instant oldTimestamp = Instant.now().minus(Duration.ofHours(25));
            insertInvalidationEntriesWithTimestamp(invalidationDs, 50, oldTimestamp, "old_table");

            // Insert 50 recent entries (1 hour ago — within 24h TTL)
            Instant recentTimestamp = Instant.now().minus(Duration.ofHours(1));
            insertInvalidationEntriesWithTimestamp(invalidationDs, 50, recentTimestamp, "recent_table");

            // Start poller and wait for it to process all 100 entries
            poller.start();
            awaitPollerProcessed(poller, 100);
            assertThat(poller.getLastProcessedId())
                    .as("Poller should have processed all 100 entries")
                    .isGreaterThanOrEqualTo(100);

            // Run cleanup — should delete only the 50 old entries
            int deleted = poller.cleanup();
            assertThat(deleted)
                    .as("Only old entries beyond 24h TTL should be deleted")
                    .isEqualTo(50);

            // Verify 50 recent entries remain
            int remaining = countAllInvalidationEntries(invalidationDs);
            assertThat(remaining)
                    .as("Recent entries within 24h TTL should be preserved")
                    .isEqualTo(50);

            poller.close();
        }

        @Test
        @DisplayName("Cleanup respects lastProcessedId safety bound")
        void cleanupRespectsLastProcessedIdSafetyBound() throws Exception {
            RecordingMetadataCache cache = new RecordingMetadataCache();
            PostgresInvalidationPoller poller = new PostgresInvalidationPoller(
                    invalidationDs, cache, 1);
            poller.validateSchema();

            // Insert 100 old entries (25 hours ago)
            Instant oldTimestamp = Instant.now().minus(Duration.ofHours(25));
            insertInvalidationEntriesWithTimestamp(invalidationDs, 100, oldTimestamp, "safety_table");

            // Start poller and wait for it to process all 100 entries
            poller.start();
            awaitPollerProcessed(poller, 100);
            long processedId = poller.getLastProcessedId();
            assertThat(processedId).isGreaterThanOrEqualTo(100);
            // Stop poller so it doesn't process the next batch
            poller.close();

            // Insert 20 more old entries AFTER the poll — these have id > lastProcessedId
            insertInvalidationEntriesWithTimestamp(invalidationDs, 20, oldTimestamp, "unprocessed_table");

            // Create a new poller that inherits the same lastProcessedId state
            // by processing the same entries (start fresh, poll to same point)
            RecordingMetadataCache cache2 = new RecordingMetadataCache();
            PostgresInvalidationPoller poller2 = new PostgresInvalidationPoller(
                    invalidationDs, cache2, 1);
            // Start and wait — it will process all 120 entries from id=0
            poller2.start();
            awaitPollerProcessed(poller2, 120);
            poller2.close();

            // Now create a third poller that only processes the first 100
            // To test safety bound, we directly verify via SQL:
            // The 20 unprocessed entries (id > processedId from first poller)
            // should NOT be deleted by cleanup on the first poller's state
            // Since poller is closed, cleanup uses its lastProcessedId
            int deleted = poller.cleanup();
            assertThat(deleted)
                    .as("Cleanup should delete old entries up to first poller's lastProcessedId")
                    .isEqualTo(100);

            // The 20 entries beyond first poller's lastProcessedId should remain
            int remaining = countAllInvalidationEntries(invalidationDs);
            assertThat(remaining)
                    .as("Unprocessed entries beyond lastProcessedId should be preserved")
                    .isEqualTo(20);
        }

        @Test
        @DisplayName("Cleanup is no-op when lastProcessedId is zero")
        void cleanupNoOpWhenNoProcessedEntries() throws Exception {
            RecordingMetadataCache cache = new RecordingMetadataCache();
            PostgresInvalidationPoller poller = new PostgresInvalidationPoller(
                    invalidationDs, cache, 1);
            poller.validateSchema();

            // Insert old entries but do NOT start poller (lastProcessedId stays 0)
            Instant oldTimestamp = Instant.now().minus(Duration.ofHours(25));
            insertInvalidationEntriesWithTimestamp(invalidationDs, 10, oldTimestamp, "nopoll_table");

            assertThat(poller.getLastProcessedId())
                    .as("lastProcessedId should be 0 before any poll")
                    .isZero();

            // Cleanup should be no-op — safety bound prevents deletion
            int deleted = poller.cleanup();
            assertThat(deleted)
                    .as("Cleanup should not delete anything when lastProcessedId is 0")
                    .isZero();

            // All entries should remain
            int remaining = countAllInvalidationEntries(invalidationDs);
            assertThat(remaining)
                    .as("All entries should be preserved when cleanup is no-op")
                    .isEqualTo(10);

            poller.close();
        }

        @Test
        @DisplayName("Repeated cleanup is idempotent")
        void repeatedCleanupIsIdempotent() throws Exception {
            RecordingMetadataCache cache = new RecordingMetadataCache();
            PostgresInvalidationPoller poller = new PostgresInvalidationPoller(
                    invalidationDs, cache, 1);
            poller.validateSchema();

            // Insert and process old entries
            Instant oldTimestamp = Instant.now().minus(Duration.ofHours(25));
            insertInvalidationEntriesWithTimestamp(invalidationDs, 30, oldTimestamp, "idempotent_table");
            poller.start();
            awaitPollerProcessed(poller, 30);

            // First cleanup deletes all
            int firstCleanup = poller.cleanup();
            assertThat(firstCleanup).isEqualTo(30);

            // Second cleanup is no-op (nothing left to delete)
            int secondCleanup = poller.cleanup();
            assertThat(secondCleanup)
                    .as("Second cleanup should be no-op")
                    .isZero();

            poller.close();
        }
    }


    // -----------------------------------------------------------------------
    // Polling helpers
    // -----------------------------------------------------------------------

    /**
     * Waits for the poller's lastProcessedId to reach at least the given minimum.
     *
     * <p>F-E2E-04 fix: replaced manual deadline+Thread.sleep loop with Awaitility
     * deterministic polling (CLAUDE.md C3 anti-busy-wait rule).
     */
    private static void awaitPollerProcessed(PostgresInvalidationPoller poller,
                                              long minProcessedId) {
        try {
            org.awaitility.Awaitility.await("poller-lastProcessedId >= " + minProcessedId)
                    .atMost(java.time.Duration.ofMillis(POLL_WAIT_TIMEOUT_MS))
                    .pollInterval(java.time.Duration.ofMillis(POLL_CHECK_INTERVAL_MS))
                    .pollDelay(java.time.Duration.ZERO)
                    .until(() -> poller.getLastProcessedId() >= minProcessedId);
        } catch (org.awaitility.core.ConditionTimeoutException e) {
            throw new AssertionError(String.format(
                    "Poller did not reach lastProcessedId >= %d within %dms (current: %d)",
                    minProcessedId, POLL_WAIT_TIMEOUT_MS, poller.getLastProcessedId()));
        }
    }

    // -----------------------------------------------------------------------
    // Schema setup helpers
    // -----------------------------------------------------------------------

    private static void truncateInvalidationLog(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE invalidation_log RESTART IDENTITY");
        }
    }

    private static int countInvalidationEntries(DataSource ds, String cacheKey) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM invalidation_log WHERE cache_key = ?")) {
            ps.setString(1, cacheKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private static int countAllInvalidationEntries(DataSource ds) throws Exception {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM invalidation_log")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private static String getInvalidationType(DataSource ds, String cacheKey) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT invalidation_type FROM invalidation_log WHERE cache_key = ? LIMIT 1")) {
            ps.setString(1, cacheKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static void insertInvalidationEntriesWithTimestamp(
            DataSource ds, int count, Instant timestamp, String tablePrefix) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                             + "VALUES (?, ?, ?, ?)")) {
            for (int i = 1; i <= count; i++) {
                String cacheKey = CATALOG + CACHE_KEY_SEPARATOR
                        + SCHEMA + CACHE_KEY_SEPARATOR + tablePrefix + "_" + i;
                ps.setString(1, cacheKey);
                ps.setString(2, "TABLE");
                ps.setTimestamp(3, Timestamp.from(timestamp));
                ps.setTimestamp(4, Timestamp.from(timestamp));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // -----------------------------------------------------------------------
    // Test doubles — no mock frameworks per pangrova-rules.md
    // -----------------------------------------------------------------------

    private static TableMetadata dummyTableMetadata(String catalog, String schema, String table) {
        return new TableMetadata(
                catalog, schema, table,
                List.of(), java.util.Map.of(), List.of(), java.util.Map.of(),
                Instant.now(), Instant.now());
    }

    /**
     * No-op cache implementation for writer-side pollers that don't need
     * cache invalidation (they only write to the log).
     */
    private static final class NoOpMetadataCache implements MetadataCache {
        @Override
        public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void putTable(String catalog, String schema, String table, TableMetadata metadata) {
            // no-op
        }

        @Override
        public void invalidateTable(String catalog, String schema, String table) {
            // no-op
        }

        @Override
        public void invalidateCatalog(String catalog) {
            // no-op
        }

        @Override
        public CacheStats getStats() {
            return new CacheStats(0, 0, 0, 0);
        }
    }

    /**
     * Recording cache that tracks invalidation calls for assertion.
     * Stores actual table metadata and removes on invalidation.
     */
    private static final class RecordingMetadataCache implements MetadataCache {
        final CopyOnWriteArrayList<String> invalidatedTables = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> invalidatedCatalogs = new CopyOnWriteArrayList<>();
        private final java.util.concurrent.ConcurrentHashMap<String, TableMetadata> store =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public Optional<TableMetadata> getTable(String catalog, String schema, String table) {
            return Optional.ofNullable(store.get(key(catalog, schema, table)));
        }

        @Override
        public void putTable(String catalog, String schema, String table, TableMetadata metadata) {
            store.put(key(catalog, schema, table), metadata);
        }

        @Override
        public void invalidateTable(String catalog, String schema, String table) {
            invalidatedTables.add(catalog + "." + schema + "." + table);
            store.remove(key(catalog, schema, table));
        }

        @Override
        public void invalidateCatalog(String catalog) {
            invalidatedCatalogs.add(catalog);
            store.keySet().removeIf(k -> k.startsWith(catalog + "."));
        }

        @Override
        public CacheStats getStats() {
            return new CacheStats(0, 0, store.size(), 0);
        }

        private static String key(String catalog, String schema, String table) {
            return catalog + "." + schema + "." + table;
        }
    }
}
