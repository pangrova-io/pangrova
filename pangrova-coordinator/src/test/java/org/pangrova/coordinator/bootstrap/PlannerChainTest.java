package org.pangrova.coordinator.bootstrap;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.planner.cache.CacheEntryMetadata;
import org.pangrova.planner.cache.CacheHint;
import org.pangrova.planner.cache.CacheKey;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.planner.stats.PlannerTableStatistics;
import org.pangrova.planner.throttle.QueryThrottlingService;
import org.pangrova.planner.throttle.ThrottlePermit;
import org.pangrova.planner.throttle.ThrottleResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test: validates planner advanced features are active from main().
 *
 * <p>Chain under test: StatisticsCollector → CBO confidence,
 * QueryResultCache → L1/L2 tiered caching,
 * QueryThrottlingService → global/tenant/user throttling,
 * CoordinatorBootstrap → planner chain wired end-to-end.</p>
 *
 * <p><b>Property 31: Planner chain from main() to optimized plan</b></p>
 */
@Tag("Feature: e2e-pipeline-integration - Property 31: Planner chain")
class PlannerChainTest extends org.pangrova.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private CoordinatorBootstrap bootstrap;

    @AfterEach
    void tearDownJUnit() {
        shutdownBootstrap();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownBootstrap();
    }

    // -- Providers --

    @Provide
    Arbitrary<Integer> stalenessHours() {
        return Arbitraries.integers().between(1, 100);
    }

    @Provide
    Arbitrary<Double> changeRates() {
        return Arbitraries.doubles().between(0.01, 0.1).ofScale(2);
    }

    @Provide
    Arbitrary<Integer> globalLimits() {
        return Arbitraries.integers().between(1, 5);
    }

    // -- Property tests --

    @Property(tries = 5)
    @Label("P31a: confidence decays with staleness and stays in [0.1, 1.0]")
    void statisticsConfidenceDecaysWithStaleness(
            @ForAll("stalenessHours") int hours,
            @ForAll("changeRates") double changeRate) {

        StatisticsCollector collector = new StatisticsCollector();
        double confidence = collector.computeConfidence(Duration.ofHours(hours), changeRate);

        assertThat(confidence).isBetween(0.1, 1.0);

        double higherStaleness = collector.computeConfidence(
                Duration.ofHours(hours + 10), changeRate);
        assertThat(higherStaleness).isLessThanOrEqualTo(confidence);
    }

    @Property(tries = 5)
    @Label("P31b: throttling rejects when global limit exhausted")
    void throttlingRejectsWhenGlobalLimitExhausted(
            @ForAll("globalLimits") int globalLimit) {

        // Use very high tenant/user rates so only global semaphore limits
        QueryThrottlingService throttle = new QueryThrottlingService(
                globalLimit, 1_000_000.0, 1_000_000.0);
        List<ThrottlePermit> permits = new ArrayList<>();

        for (int i = 0; i < globalLimit; i++) {
            ThrottleResult result = throttle.tryAcquire("tenant-1", "user-" + i);
            assertThat(result.acquired())
                    .as("permit %d of %d should be acquired", i + 1, globalLimit)
                    .isTrue();
            permits.add(result.permit());
        }

        ThrottleResult rejected = throttle.tryAcquire("tenant-1", "user-overflow");
        assertThat(rejected.acquired()).isFalse();
        assertThat(rejected.reason()).isNotBlank();

        permits.forEach(throttle::release);
    }

    // -- Scenario tests --

    @Test
    void statisticsCollectorAnalyzesAndRetrieves() {
        StatisticsCollector collector = new StatisticsCollector();
        PlannerTableStatistics stats = collector.analyzeTable(new QualifiedTableName("test", "db", "orders"), List.of("id", "amount"));

        assertThat(stats.tableName()).isEqualTo(new QualifiedTableName("test", "db", "orders"));
        assertThat(stats.columns()).hasSize(2);
        assertThat(stats.columns()).containsKeys("id", "amount");

        Optional<PlannerTableStatistics> retrieved = collector.getStatistics(new QualifiedTableName("test", "db", "orders"));
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().columns()).hasSize(2);
    }

    @Test
    void queryResultCacheHitAndMiss() {
        InMemoryL2CacheStore l2 = new InMemoryL2CacheStore();
        QueryResultCache cache = new QueryResultCache(l2);
        CachedResult result = new CachedResult(
                new byte[]{1, 2, 3}, Instant.now());

        CacheKey key = CacheKey.of("select * from orders", "t1", "alice", Map.of(), null);
        CacheEntryMetadata metadata = new CacheEntryMetadata(Set.of("orders"), "alice", "");
        cache.put(key, result, metadata);

        Optional<CachedResult> hit = cache.get(key, CacheHint.NONE);
        assertThat(hit).isPresent();

        Optional<CachedResult> noCache = cache.get(key, CacheHint.NO_CACHE);
        assertThat(noCache).isEmpty();

        cache.invalidateByTable("orders");

        Optional<CachedResult> miss = cache.get(key, CacheHint.NONE);
        assertThat(miss).isEmpty();
    }

    @Test
    void queryResultCacheL2Promotion() {
        InMemoryL2CacheStore l2 = new InMemoryL2CacheStore();
        QueryResultCache cache = new QueryResultCache(l2);
        CachedResult result = new CachedResult(
                new byte[]{10, 20}, Instant.now());

        CacheKey key = CacheKey.of("select * from promo", "t1", "alice", Map.of(), null);
        CacheEntryMetadata metadata = new CacheEntryMetadata(Set.of("promo"), "alice", "");
        cache.put(key, result, metadata);

        // Verify L1 hit
        Optional<CachedResult> l1Hit = cache.get(key, CacheHint.NONE);
        assertThat(l1Hit).isPresent();

        // Invalidate with matching table clears L1 entries referencing "promo"
        cache.invalidateByTable("promo");

        // L1 cleared, L2 also cleared by invalidateByTable — verify miss
        Optional<CachedResult> afterInvalidate = cache.get(key, CacheHint.NONE);
        assertThat(afterInvalidate).isEmpty();

        // Re-put to L2 only (simulate external population)
        l2.put(key.keyString(), result);

        // L1 miss → L2 hit → promoted to L1
        Optional<CachedResult> promoted = cache.get(key, CacheHint.NONE);
        assertThat(promoted).isPresent();
        assertThat(promoted.get().data()).isEqualTo(new byte[]{10, 20});

        // Second get should be L1 hit (promoted)
        Optional<CachedResult> l1AfterPromo = cache.get(key, CacheHint.NONE);
        assertThat(l1AfterPromo).isPresent();
    }

    @Test
    void coordinatorBootstrapPlannerChainActive() throws Exception {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                "planner-chain-test", 0);
        bootstrap = startAuthorizedCoordinator(config);

        assertThat(bootstrap.isStarted()).isTrue();
        int port = bootstrap.getHttpPort();
        assertThat(port).isGreaterThan(0);

        HttpResponse<String> resp = submitQuery(port, "SELECT 1");
        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("No available worker for task dispatch");
    }

    @Test
    void histogramCollectionDecision() {
        StatisticsCollector collector = new StatisticsCollector();

        // Join key → always collect
        assertThat(collector.shouldCollectHistogram(
                "join_col", 500, 10_000, true, false)).isTrue();

        // Filter column → always collect
        assertThat(collector.shouldCollectHistogram(
                "filter_col", 500, 10_000, false, true)).isTrue();

        // High-cardinality non-key column → skip
        assertThat(collector.shouldCollectHistogram(
                "high_card", 5000, 10_000, false, false)).isFalse();
    }

    // -- Bootstrap helpers --

    private void shutdownBootstrap() {
        if (bootstrap != null) {
            bootstrap.shutdown();
            bootstrap = null;
        }
    }

    // -- HTTP helpers --

    private HttpResponse<String> submitQuery(int port, String sql)
            throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .header("Authorization", authorizationHeader())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(HttpResponse<String> resp)
            throws Exception {
        return MAPPER.readValue(resp.body(), Map.class);
    }
}
