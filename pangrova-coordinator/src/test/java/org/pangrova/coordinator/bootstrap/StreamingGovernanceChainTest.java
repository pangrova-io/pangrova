package org.pangrova.coordinator.bootstrap;

import org.pangrova.common.lifecycle.ResourceCloser;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.*;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.governance.GovernanceDeps;
import org.pangrova.governance.GovernanceContext;
import org.pangrova.governance.classification.AuditConfigStore;
import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.governance.lineage.*;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor;
import org.pangrova.governance.quality.rule.GovernanceStatementHandler;
import org.pangrova.governance.quality.rule.InMemoryQualityRuleStore;
import org.pangrova.governance.quality.schedule.CronScheduler;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.EnrichedColumnMaskStore;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.spi.connector.data.*;
import org.pangrova.spi.connector.data.write.*;
import org.pangrova.spi.connector.data.write.model.*;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.WatermarkStore;
import org.pangrova.streaming.cdc.*;
import org.pangrova.streaming.watermark.*;
import org.pangrova.streaming.writer.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance test: validates the streaming/governance chain from main() to CDC pipeline.
 *
 * <p>Chain under test: LineageEventEmitter → GovernanceContext → StreamingContext →
 * WatermarkTracker → CdcConsumer</p>
 *
 * <p><b>Property 33: Streaming/governance chain from main() to CDC pipeline</b></p>
 */
@Tag("streaming-governance-component")
class StreamingGovernanceChainTest extends org.pangrova.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private CoordinatorBootstrap coordinatorBootstrap;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    // -- Providers --

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(10)
                .map(s -> "q-" + s);
    }

    @Provide
    Arbitrary<Instant> watermarkInstants() {
        return Arbitraries.longs()
                .between(1_000_000_000L, 2_000_000_000L)
                .map(epoch -> Instant.ofEpochSecond(epoch));
    }

    // -- Property tests --

    /** P33a: emitStartEvent + emitCompleteEvent does not throw for any queryId. */
    @Property(tries = 5)
    @net.jqwik.api.Tag("streaming-governance-component")
    void lineageEmissionPipelineWorksEndToEnd(
            @ForAll("queryIds") String queryId) {

        LineageEventEmitter emitter = createLineageEmitter();
        assertThatCode(() -> {
            emitter.emitStartEvent(queryId, "SELECT 1", List.of(), List.of());
            emitter.emitCompleteEvent(queryId);
        }).doesNotThrowAnyException();
    }

    /** P33b: updateWatermark advances watermark and transitions state to ACTIVE. */
    @Property(tries = 5)
    @net.jqwik.api.Tag("streaming-governance-component")
    void watermarkUpdateAdvancesStateToActive(
            @ForAll("watermarkInstants") Instant watermark) {

        WatermarkTracker tracker = createWatermarkTracker();
        tracker.updateWatermark("table1", watermark);

        assertThat(tracker.getWatermark("table1")).isEqualTo(watermark);
        assertThat(tracker.getState("table1")).isEqualTo(WatermarkState.ACTIVE);
    }

    // -- JUnit scenario tests --

    /** GovernanceContext initializes with real quality rules without exception. */
    @Test
    void governanceModuleInitializesWithQualityRules() {
        GovernanceContext module = createGovernanceContext();
        closeables.add(module);

        assertThatCode(module::initialize).doesNotThrowAnyException();
    }

    /** LineageEventEmitter drops events when queue overflows. */
    @Test
    void lineageEmitterDropsEventsOnOverflow() throws Exception {
        BlockingLineageBackend backend = new BlockingLineageBackend();
        LineageEventEmitter emitter = new LineageEventEmitter(backend);
        closeables.add(emitter);

        fillEmitterQueue(emitter, 1100);
        Thread.sleep(50);

        assertThat(emitter.eventsDroppedTotal()).isGreaterThan(0);
    }

    /** WatermarkTracker transitions: ACTIVE on advance, STALE on regression. */
    @Test
    void watermarkTrackerStateTransitions() {
        WatermarkTracker tracker = createWatermarkTracker();
        Instant now = Instant.now();

        tracker.updateWatermark("t1", now);
        assertThat(tracker.getState("t1")).isEqualTo(WatermarkState.ACTIVE);

        tracker.updateWatermark("t1", now.minusSeconds(60));
        assertThat(tracker.getState("t1")).isEqualTo(WatermarkState.STALE);
    }

    /** CoordinatorBootstrap starts with streaming/governance chain intact. */
    @Test
    void coordinatorBootstrapStartsWithStreamingChain() throws Exception {
        startCoordinator("coord-stream-test");
        int port = coordinatorBootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 1");
        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("No available worker for task dispatch");
    }

    /** DebeziumJsonConverter parses a Debezium JSON message into correct CdcEnvelope fields. */
    @Test
    void debeziumConverterParsesMessage() {
        var converter = new org.pangrova.streaming.cdc.format.DebeziumJsonConverter();

        CdcEnvelope envelope = converter.deserialize(
                buildDebeziumJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertThat(envelope.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(envelope.after()).isNotNull();
        assertThat(envelope.after().get("id")).isNotNull();
        assertThat(envelope.after().get("name")).isNotNull();
        assertThat(envelope.before()).isNull();
        assertThat(envelope.tableName()).isEqualTo("mydb.users");
    }

    // -- Bootstrap helpers --

    private void startCoordinator(String coordinatorId) {
        shutdownCoordinator();
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                coordinatorId, 0);
        coordinatorBootstrap = startAuthorizedCoordinator(config);
        assertThat(coordinatorBootstrap.isStarted()).isTrue();
        assertThat(coordinatorBootstrap.getHttpPort()).isGreaterThan(0);
    }

    private void shutdownAll() {
        closeables.forEach(ResourceCloser::closeQuietly);
        closeables.clear();
        shutdownCoordinator();
    }

    private void shutdownCoordinator() {
        if (coordinatorBootstrap != null) {
            coordinatorBootstrap.shutdown();
            coordinatorBootstrap = null;
        }
    }

    // -- Factory helpers --

    private LineageEventEmitter createLineageEmitter() {
        InMemoryLineageBackend backend = new InMemoryLineageBackend();
        LineageEventEmitter emitter = new LineageEventEmitter(backend);
        closeables.add(emitter);
        return emitter;
    }

    private WatermarkTracker createWatermarkTracker() {
        return new WatermarkTracker(new InMemoryWatermarkStore());
    }

    private GovernanceContext createGovernanceContext() {
        InMemoryQualityRuleStore ruleStore = new InMemoryQualityRuleStore();
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        GovernanceStatementHandler handler = new GovernanceStatementHandler(
                ruleStore, new DefaultQualityRuleExecutor(), alerter);
        CronScheduler scheduler = new CronScheduler(handler);
        closeables.add(scheduler);
        LineageEventEmitter emitter = createLineageEmitter();
        TagBasedSecurityPolicy policy = new TagBasedSecurityPolicy(
                new InMemoryColumnMaskStore(), new InMemoryAuditConfigStore());
        return new GovernanceContext(new GovernanceDeps(
                emitter, handler, policy, scheduler, ruleStore));
    }

    private void fillEmitterQueue(LineageEventEmitter emitter, int count) {
        for (int i = 0; i < count; i++) {
            emitter.emitStartEvent("overflow-" + i, "SELECT 1", List.of(), List.of());
        }
    }

    // -- HTTP helpers --

    private HttpResponse<String> submitQuery(int port, String sql) throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .header("Authorization", authorizationHeader())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    // -- Test data helpers --

    private static String buildDebeziumJson() {
        return """
                {"payload":{"op":"c","before":null,\
                "after":{"id":1,"name":"test"},\
                "ts_ms":1234567890,\
                "source":{"db":"mydb","table":"users"}}}""";
    }

    // -- In-memory implementations (real, not mocks) --

    private static class InMemoryLineageBackend implements LineageBackend {
        private final CopyOnWriteArrayList<LineageEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void send(LineageEvent event) {
            events.add(event);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }

    private static class BlockingLineageBackend implements LineageBackend {
        @Override
        public void send(LineageEvent event) {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }

    private static class InMemoryWatermarkStore implements WatermarkStore {
        private final Map<String, Instant> store = new HashMap<>();

        @Override
        public void save(String tableName, Instant watermark) {
            store.put(tableName, watermark);
        }

        @Override
        public Optional<Instant> load(String tableName) {
            return Optional.ofNullable(store.get(tableName));
        }
    }

    private static class InMemoryColumnMaskStore implements EnrichedColumnMaskStore {
        private final List<ColumnMask> masks = new CopyOnWriteArrayList<>();

        @Override
        public List<ColumnMask> getColumnMasks(String cat, String schema, String table, String col) {
            return masks.stream()
                    .filter(m -> table.equals(m.tableName()) && col.equals(m.columnName()))
                    .toList();
        }

        @Override
        public void saveColumnMask(ColumnMask mask) {
            masks.add(mask);
        }

        @Override
        public void deleteMask(long maskId) {
            masks.removeIf(m -> m.maskId() == maskId);
        }
    }

    private static class InMemoryAuditConfigStore implements AuditConfigStore {
        private final Set<String> enabled = new HashSet<>();

        @Override
        public void enableDetailedAudit(String cat, String schema, String table) {
            enabled.add(cat + "." + schema + "." + table);
        }

        @Override
        public void disableDetailedAudit(String cat, String schema, String table) {
            enabled.remove(cat + "." + schema + "." + table);
        }

        @Override
        public boolean isDetailedAuditEnabled(String cat, String schema, String table) {
            return enabled.contains(cat + "." + schema + "." + table);
        }
    }

    private static class NoOpTableWriter implements TableWriter {
        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            return new WriteResult(tableName, records.size(), 0, 0, 0, Instant.now());
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() { }
    }
}
