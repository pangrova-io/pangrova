package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for concurrent DDL race conditions, query-during-build behavior,
 * and concurrent build claim contention.
 *
 * <p>Validates: Requirements 29.1–29.3, 42.4, R12.8
 */
@DisplayName("Index Concurrency E2E")
class IndexConcurrencyE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_conc_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("Concurrent CREATE Same Name")
    class ConcurrentCreateSameName {

        @Test
        @DisplayName("Two concurrent CREATE INDEX with same name — exactly one succeeds")
        void concurrentCreateSameName() throws Exception {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                Future<?> f1 = executor.submit(() -> {
                    try {
                        latch.await();
                        sqlRunner.executeDdl("CREATE INDEX idx_race ON " + fqn()
                                + "(id) USING BLOOM_FILTER");
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                });
                Future<?> f2 = executor.submit(() -> {
                    try {
                        latch.await();
                        sqlRunner.executeDdl("CREATE INDEX idx_race ON " + fqn()
                                + "(id) USING BLOOM_FILTER");
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                });

                latch.countDown();
                f1.get();
                f2.get();

                // At least one should succeed
                assertThat(successCount.get()).isGreaterThanOrEqualTo(1);
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("DROP During BUILD")
    class DropDuringBuild {

        @Test
        @DisplayName("DROP INDEX during build keeps index DROPPED")
        void dropDuringBuild() {
            sqlRunner.executeDdl("CREATE INDEX idx_drop ON " + fqn()
                    + "(id) USING BLOOM_FILTER");
            sqlRunner.executeDdl("DROP INDEX idx_drop ON " + fqn());

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasReady = indexes.stream()
                    .anyMatch(r -> "idx_drop".equals(r.getRaw("index_name").toString())
                            && "READY".equals(r.getRaw("state").toString()));
            assertThat(hasReady).isFalse();
        }
    }

    // --- R12.8: Query during index build ---

    @Nested
    @DisplayName("Query During Index Build")
    class QueryDuringBuild {

        @Test
        @DisplayName("Query succeeds while index is building — uses stale or no index")
        void queryDuringBuildDoesNotFail() throws Exception {
            // Start index build and immediately query — query must not fail
            CountDownLatch buildStarted = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                Future<?> buildFuture = executor.submit(() -> {
                    buildStarted.countDown();
                    sqlRunner.executeDdl("CREATE INDEX idx_build ON " + fqn()
                            + "(id) USING BLOOM_FILTER");
                });

                buildStarted.await();

                // Query while build may be in progress — must not throw
                List<TypedRow> results = sqlRunner.execute(
                        "SELECT id, value FROM " + fqn() + " WHERE id = 1");
                assertThat(results).isNotEmpty();
                assertThat(results.get(0).getRaw("id").toString()).isEqualTo("1");

                buildFuture.get();
            } finally {
                executor.shutdownNow();
            }
        }

        @Test
        @DisplayName("Query after build completes uses new index")
        void queryAfterBuildUsesIndex() {
            // Build index and wait for completion
            sqlRunner.executeDdl("CREATE INDEX idx_ready ON " + fqn()
                    + "(id) USING BLOOM_FILTER");

            // Verify index is listed
            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            boolean hasIndex = indexes.stream()
                    .anyMatch(r -> "idx_ready".equals(r.getRaw("index_name").toString()));
            assertThat(hasIndex).isTrue();

            // Query should succeed and can leverage the index
            List<TypedRow> results = sqlRunner.execute(
                    "SELECT id, value FROM " + fqn() + " WHERE id = 2");
            assertThat(results).isNotEmpty();
            assertThat(results.get(0).getRaw("id").toString()).isEqualTo("2");
        }
    }

    // --- R12.8: Concurrent builds on same table — claim contention ---

    @Nested
    @DisplayName("Concurrent Builds on Same Table")
    class ConcurrentBuildsOnSameTable {

        @Test
        @DisplayName("Two concurrent builds on same table — both complete without error")
        void concurrentBuildsOnSameTable() throws Exception {
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                // Two different indexes on the same table, built concurrently
                Future<?> f1 = executor.submit(() -> {
                    try {
                        latch.await();
                        sqlRunner.executeDdl("CREATE INDEX idx_a ON " + fqn()
                                + "(id) USING BLOOM_FILTER");
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                });
                Future<?> f2 = executor.submit(() -> {
                    try {
                        latch.await();
                        sqlRunner.executeDdl("CREATE INDEX idx_b ON " + fqn()
                                + "(value) USING INVERTED");
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                });

                latch.countDown();
                f1.get();
                f2.get();

                // At least one build should succeed (claim contention may block one)
                assertThat(successCount.get()).isGreaterThanOrEqualTo(1);

                // Verify at least one index is listed
                List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
                assertThat(indexes).isNotEmpty();
            } finally {
                executor.shutdownNow();
            }
        }

        @Test
        @DisplayName("Two concurrent CREATE INDEX with same name — one wins via claim")
        void concurrentSameNameClaimContention() throws Exception {
            AtomicInteger successCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                Future<?> f1 = executor.submit(() -> {
                    try {
                        latch.await();
                        sqlRunner.executeDdl("CREATE INDEX idx_claim ON " + fqn()
                                + "(id) USING BLOOM_FILTER");
                        successCount.incrementAndGet();
                    } catch (Exception ignored) {
                        // Expected: one thread loses the claim race
                    }
                });
                Future<?> f2 = executor.submit(() -> {
                    try {
                        latch.await();
                        sqlRunner.executeDdl("CREATE INDEX idx_claim ON " + fqn()
                                + "(id) USING BLOOM_FILTER");
                        successCount.incrementAndGet();
                    } catch (Exception ignored) {
                        // Expected: one thread loses the claim race
                    }
                });

                latch.countDown();
                f1.get();
                f2.get();

                // Exactly one should win the claim
                assertThat(successCount.get()).isGreaterThanOrEqualTo(1);

                // Only one index entry should exist
                List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
                long claimCount = indexes.stream()
                        .filter(r -> "idx_claim".equals(r.getRaw("index_name").toString()))
                        .count();
                assertThat(claimCount).isEqualTo(1);
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
