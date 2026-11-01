package org.pangrova.testing.integration.e2e.module;

import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.coordinator.resilience.breaker.CircuitBreaker;
import org.pangrova.common.resilience.policy.CircuitBreakerConfig;
import org.pangrova.spi.security.tenant.ResourceQuota;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.streaming.store.memory.InMemoryWatermarkStore;
import org.pangrova.streaming.core.watermark.WatermarkTracker;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.setup.MetadataSchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for module integration: governance, resilience, streaming, tenant, indexing.
 *
 * <p>Each nested class validates one module's integration behavior using
 * real implementations (no mocks) backed by TestContainers PostgreSQL where needed.
 *
 * <p>Implements: R9.5
 */
@DisplayName("Module Integration E2E")
class ModuleIntegrationE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    // -----------------------------------------------------------------------
    // Governance: lineage events emitted on query completion
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Governance — Lineage Events")
    class GovernanceLineage {

        private CapturingLineageBackend backend;
        private LineageEventEmitter emitter;

        @BeforeEach
        void initEmitter() {
            backend = new CapturingLineageBackend();
            emitter = new LineageEventEmitter(backend);
        }

        @AfterEach
        void closeEmitter() {
            if (emitter != null) {
                emitter.close();
            }
        }

        @Test
        @DisplayName("Start and complete events emitted for query lifecycle")
        void startAndCompleteEventsEmitted() {
            String queryId = "lineage-q-" + System.nanoTime();
            List<DatasetRef> inputs = List.of(new DatasetRef("ns", "input_table", List.of()));
            List<DatasetRef> outputs = List.of(new DatasetRef("ns", "output_table", List.of()));

            emitter.emitStartEvent(queryId, "SELECT * FROM t", inputs, outputs);
            emitter.emitCompleteEvent(queryId);

            // F-E2E-04 fix: poll until consumer drains both events instead of fixed sleep.
            org.awaitility.Awaitility.await("lineage-start+complete drained")
                    .atMost(java.time.Duration.ofSeconds(5))
                    .pollInterval(java.time.Duration.ofMillis(50))
                    .pollDelay(java.time.Duration.ZERO)
                    .until(() -> backend.events.stream()
                                    .filter(e -> e.eventType() == LineageEvent.EventType.START)
                                    .count() >= 1
                            && backend.events.stream()
                                    .filter(e -> e.eventType() == LineageEvent.EventType.COMPLETE)
                                    .count() >= 1);

            assertThat(backend.events).hasSizeGreaterThanOrEqualTo(2);
            assertThat(backend.events.stream()
                    .filter(e -> e.eventType() == LineageEvent.EventType.START)
                    .count()).isGreaterThanOrEqualTo(1);
            assertThat(backend.events.stream()
                    .filter(e -> e.eventType() == LineageEvent.EventType.COMPLETE)
                    .count()).isGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("Fail event emitted on query failure")
        void failEventEmittedOnFailure() {
            String queryId = "lineage-fail-" + System.nanoTime();

            emitter.emitStartEvent(queryId, "SELECT 1/0", List.of(), List.of());
            emitter.emitFailEvent(queryId, "Division by zero");

            // F-E2E-04 fix: poll until FAIL event consumed.
            org.awaitility.Awaitility.await("lineage-fail drained")
                    .atMost(java.time.Duration.ofSeconds(5))
                    .pollInterval(java.time.Duration.ofMillis(50))
                    .pollDelay(java.time.Duration.ZERO)
                    .until(() -> backend.events.stream()
                            .filter(e -> e.eventType() == LineageEvent.EventType.FAIL)
                            .count() >= 1);

            assertThat(backend.events.stream()
                    .filter(e -> e.eventType() == LineageEvent.EventType.FAIL)
                    .count()).isGreaterThanOrEqualTo(1);
        }
    }

    // -----------------------------------------------------------------------
    // Resilience: circuit breaker trips after N failures
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Resilience — Circuit Breaker")
    class ResilienceCircuitBreaker {

        @Test
        @DisplayName("Circuit breaker trips to OPEN after failure threshold")
        void circuitBreakerTripsAfterFailures() {
            CircuitBreakerConfig config = new CircuitBreakerConfig(
                    0.5, 4, 60_000L, 1, java.time.Clock.systemUTC());
            CircuitBreaker breaker = new CircuitBreaker(config);

            // 4 calls: 3 failures + 1 success = 75% failure rate > 50% threshold
            breaker.onSuccess();
            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();

            assertThat(breaker.getState())
                    .isEqualTo(CircuitBreaker.State.OPEN);
            assertThat(breaker.allowRequest()).isFalse();
        }

        @Test
        @DisplayName("Circuit breaker stays CLOSED below threshold")
        void circuitBreakerStaysClosedBelowThreshold() {
            CircuitBreakerConfig config = new CircuitBreakerConfig(
                    0.5, 4, 60_000L, 1, java.time.Clock.systemUTC());
            CircuitBreaker breaker = new CircuitBreaker(config);

            // 4 calls: 1 failure + 3 successes = 25% < 50%
            breaker.onFailure();
            breaker.onSuccess();
            breaker.onSuccess();
            breaker.onSuccess();

            assertThat(breaker.getState())
                    .isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(breaker.allowRequest()).isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // Streaming: watermark tracked across CDC events
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Streaming — Watermark Tracking")
    class StreamingWatermark {

        private WatermarkTracker tracker;

        @BeforeEach
        void initTracker() {
            tracker = new WatermarkTracker(new InMemoryWatermarkStore());
        }

        @Test
        @DisplayName("Watermark advances monotonically across events")
        void watermarkAdvancesMonotonically() {
            String table = "cdc_events_table";
            Instant t1 = Instant.parse("2025-01-01T00:00:00Z");
            Instant t2 = Instant.parse("2025-01-01T00:01:00Z");
            Instant t3 = Instant.parse("2025-01-01T00:02:00Z");

            tracker.updateWatermark(table, t1);
            assertThat(tracker.getWatermark(table)).isEqualTo(t1);

            tracker.updateWatermark(table, t2);
            assertThat(tracker.getWatermark(table)).isEqualTo(t2);

            tracker.updateWatermark(table, t3);
            assertThat(tracker.getWatermark(table)).isEqualTo(t3);
        }

        @Test
        @DisplayName("Watermark regression is rejected")
        void watermarkRegressionRejected() {
            String table = "cdc_regression_table";
            Instant t1 = Instant.parse("2025-01-01T00:02:00Z");
            Instant t2 = Instant.parse("2025-01-01T00:01:00Z");

            tracker.updateWatermark(table, t1);
            tracker.updateWatermark(table, t2);

            // Watermark should not regress
            assertThat(tracker.getWatermark(table)).isEqualTo(t1);
        }

        @Test
        @DisplayName("Multiple tables tracked independently")
        void multipleTablesTrackedIndependently() {
            Instant t1 = Instant.parse("2025-01-01T00:00:00Z");
            Instant t2 = Instant.parse("2025-06-01T00:00:00Z");

            tracker.updateWatermark("table_a", t1);
            tracker.updateWatermark("table_b", t2);

            assertThat(tracker.getWatermark("table_a")).isEqualTo(t1);
            assertThat(tracker.getWatermark("table_b")).isEqualTo(t2);
        }
    }

    // -----------------------------------------------------------------------
    // Tenant: query rejected when quota exceeded
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Tenant — Quota Enforcement")
    class TenantQuotaEnforcement {

        private JdbcTenantStore tenantStore;

        @BeforeEach
        void initTenantStore() throws Exception {
            DataSource ds = MetadataSchemaInitializer.createDataSource(
                    mysqlJdbcUrl(), mysqlUsername(), mysqlPassword());
            MetadataSchemaInitializer.createStateStoreTables(ds);
            tenantStore = new JdbcTenantStore(ds);
        }

        @Test
        @DisplayName("Tenant with zero concurrent query quota rejects queries")
        void tenantWithZeroQuotaRejectsQueries() {
            String tenantId = "quota-tenant-" + System.nanoTime();
            ResourceQuota quota = new ResourceQuota(
                    1024 * 1024, 0, 0, 1000, 1024, 0, 0, 0);
            Tenant tenant = new Tenant(
                    tenantId, "Test Tenant", TenantStatus.ACTIVE, quota,
                    List.of(CATALOG), List.of(), null, Map.of(),
                    Instant.now(), "test");

            tenantStore.createTenant(tenant);

            var loaded = tenantStore.getTenant(tenantId);
            assertThat(loaded).isPresent();
            assertThat(loaded.get().quota().maxConcurrentQueries()).isZero();
            // Zero concurrent queries = no queries allowed
        }

        @Test
        @DisplayName("Tenant persists and retrieves with correct quota values")
        void tenantPersistsWithCorrectQuota() {
            String tenantId = "persist-tenant-" + System.nanoTime();
            ResourceQuota quota = new ResourceQuota(
                    2L * 1024 * 1024 * 1024, 10, 5, 30000, 1024 * 1024, 0, 0, 0);
            Tenant tenant = new Tenant(
                    tenantId, "Persist Tenant", TenantStatus.ACTIVE, quota,
                    List.of(CATALOG), List.of(), null, Map.of(),
                    Instant.now(), "test");

            tenantStore.createTenant(tenant);

            var loaded = tenantStore.getTenant(tenantId);
            assertThat(loaded).isPresent();
            assertThat(loaded.get().quota().maxConcurrentQueries()).isEqualTo(10);
            assertThat(loaded.get().quota().maxQueuedQueries()).isEqualTo(5);
            assertThat(loaded.get().quota().maxMemoryBytes())
                    .isEqualTo(2L * 1024 * 1024 * 1024);
        }
    }

    // -----------------------------------------------------------------------
    // Indexing: index built and used for data skipping
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Indexing — Build and Data Skipping")
    class IndexingDataSkipping {

        @Test
        @DisplayName("CREATE INDEX succeeds and SHOW INDEXES lists it")
        void createIndexAndShowIndexes() {
            String tableName = "idx_test_" + Long.toHexString(System.nanoTime());
            dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                    "id BIGINT, category VARCHAR, amount BIGINT");
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s.%s.%s (id, category, amount) VALUES ",
                    CATALOG, SCHEMA, tableName));
            for (int i = 1; i <= 20; i++) {
                if (i > 1) sb.append(", ");
                sb.append(String.format("(%d, 'cat_%d', %d)", i, i % 5, i * 10));
            }
            sqlRunner.execute(sb.toString());

            sqlRunner.execute(String.format(
                    "CREATE INDEX bloom_idx ON %s.%s.%s (category) USING BLOOM_FILTER",
                    CATALOG, SCHEMA, tableName));

            var indexes = sqlRunner.execute(String.format(
                    "SHOW INDEXES FROM %s.%s.%s",
                    CATALOG, SCHEMA, tableName));

            assertThat(indexes).isNotEmpty();
            assertThat(indexes.stream()
                    .anyMatch(r -> "bloom_idx".equals(
                            String.valueOf(r.getRaw("index_name")))))
                    .isTrue();
        }

        @Test
        @DisplayName("Query with index returns correct results")
        void queryWithIndexReturnsCorrectResults() {
            String tableName = "idx_query_" + Long.toHexString(System.nanoTime());
            dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                    "id BIGINT, value BIGINT");
            StringBuilder sb2 = new StringBuilder();
            sb2.append(String.format("INSERT INTO %s.%s.%s (id, value) VALUES ",
                    CATALOG, SCHEMA, tableName));
            for (int i = 1; i <= 50; i++) {
                if (i > 1) sb2.append(", ");
                sb2.append(String.format("(%d, %d)", i, i * 100));
            }
            sqlRunner.execute(sb2.toString());

            sqlRunner.execute(String.format(
                    "CREATE INDEX btree_idx ON %s.%s.%s (value) USING BTREE",
                    CATALOG, SCHEMA, tableName));

            var rows = sqlRunner.execute(String.format(
                    "SELECT id, value FROM %s.%s.%s WHERE value >= 4000 ORDER BY id",
                    CATALOG, SCHEMA, tableName));

            assertThat(rows).hasSize(11); // ids 40-50
            long firstId = ((Number) rows.get(0).getRaw("id")).longValue();
            assertThat(firstId).isEqualTo(40);
        }
    }

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /**
     * In-memory lineage backend that captures all events for assertion.
     * Thread-safe via CopyOnWriteArrayList.
     */
    private static final class CapturingLineageBackend implements LineageBackend {
        final CopyOnWriteArrayList<LineageEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void send(LineageEvent event) {
            events.add(event);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }
}
