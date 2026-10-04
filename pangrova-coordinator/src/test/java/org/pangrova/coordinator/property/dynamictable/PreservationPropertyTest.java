package org.pangrova.coordinator.property.dynamictable;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.DdlResult;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.DynamicTableDagManager;
import org.pangrova.coordinator.dynamictable.DynamicTableDdlContext;
import org.pangrova.coordinator.dynamictable.DynamicTableDdlExecutor;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.AggregateEvaluator;
import org.pangrova.coordinator.dynamictable.eval.AggregateEvaluator.AggregateFunction;
import org.pangrova.coordinator.dynamictable.eval.FilterProjectEvaluator;
import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuota;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinatorContext;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshLockCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.dynamictable.schedule.RefreshSchedulerContext;
import org.pangrova.coordinator.dynamictable.write.DynamicTableWriter;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.InMemoryRefreshHistoryStore;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.RefreshStrategy;
import org.pangrova.parser.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.mv.MaterializedViewDefinition;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRow;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Preservation property tests for Dynamic Table review findings bugfix.
 *
 * <p>These tests capture EXISTING correct behavior on UNFIXED code.
 * They MUST PASS before any fix is applied, establishing a regression baseline.</p>
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7</b></p>
 */
class PreservationPropertyTest {

    // ══════════════════════════════════════════════════════════════════
    // Property 1: CDC Success Path Unchanged (3.1)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 1: For all CDC-success refresh contexts, pipeline remains CDC
     * and no fallback is triggered.
     *
     * <p>When CDC detection succeeds, the system continues using CDC-based
     * detection without switching to SNAPSHOT.</p>
     *
     * <p><b>Validates: Requirements 3.1</b></p>
     */
    @Property(tries = 10)
    void cdcSuccessPath_pipelineRemainsCdc_noFallbackTriggered(
            @ForAll("cdcSuccessEventCounts") int eventCount) {

        InMemoryDynamicTableStateStore stateStore = new InMemoryDynamicTableStateStore();
        InMemoryRefreshHistoryStore historyStore = new InMemoryRefreshHistoryStore();
        RefreshLockManager lockManager = new RefreshLockManager(new InMemoryDistributedLease());
        DynamicTableConfig config = DynamicTableConfig.defaults();

        DynamicTableState state = cdcState("cdc_preserve_dt");
        stateStore.saveState(state);

        List<CdcEnvelope> events = generateInsertEvents("source", "key", eventCount);
        AggregateEvaluator evaluator = new AggregateEvaluator(
                List.of("key"), List.of(new AggregateFunction(
                AggregateFunction.Type.COUNT, "value", "cnt")),
                RefreshMemoryQuota.unlimited());

        RefreshPipeline cdcPipeline = new RefreshPipeline.CdcRefreshPipeline(
                s -> ChangeResult.cdcBatch(events), evaluator);

        RefreshCoordinatorContext deps = new RefreshCoordinatorContext(
                stateStore, historyStore, recordingWriter(eventCount));

        var coordinator = new RefreshCoordinator(
                deps, cdcPipeline, new RefreshLockCoordinator(lockManager), config);

        for (int i = 0; i < 3; i++) {
            coordinator.executeRefresh(state, TriggerType.SCHEDULED);
        }

        DynamicTableState updated = stateStore.loadState("cdc_preserve_dt").orElseThrow();
        assertThat(updated.effectiveRefreshMode())
                .as("CDC success path: effectiveRefreshMode must remain CDC")
                .isEqualTo(DynamicTableRefreshMode.CDC);
    }

    @Provide
    Arbitrary<Integer> cdcSuccessEventCounts() {
        return Arbitraries.integers().between(1, 50);
    }

    // ══════════════════════════════════════════════════════════════════
    // Property 2: SUM/COUNT Aggregation Correctness (3.2, 3.5)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 2: For all low-cardinality GROUP BY inputs within quota,
     * SUM/COUNT merge/retract produces correct aggregation.
     *
     * <p><b>Validates: Requirements 3.2, 3.5</b></p>
     */
    @Property(tries = 10)
    void sumCountAggregation_mergeRetract_producesCorrectResults(
            @ForAll("aggregationInputs") AggregationInput input) {

        AggregateEvaluator evaluator = new AggregateEvaluator(
                List.of("category"),
                List.of(
                        new AggregateFunction(AggregateFunction.Type.SUM, "amount", "total"),
                        new AggregateFunction(AggregateFunction.Type.COUNT, "amount", "cnt")),
                RefreshMemoryQuota.unlimited());

        DynamicTableState dtState = cdcState("agg_preserve_dt");

        // Phase 1: INSERT all rows
        List<CdcEnvelope> inserts = input.rows().stream()
                .map(r -> insertEnvelope("src", r.category(), r.amount()))
                .toList();
        IncrementalResult insertResult = evaluator.evaluate(dtState, ChangeResult.cdcBatch(inserts));

        // Compute expected aggregation
        Map<String, Long> expectedSum = new HashMap<>();
        Map<String, Long> expectedCount = new HashMap<>();
        for (TestRow row : input.rows()) {
            expectedSum.merge(row.category(), row.amount(), Long::sum);
            expectedCount.merge(row.category(), 1L, Long::sum);
        }

        for (Map<String, Object> record : insertResult.records()) {
            String cat = (String) record.get("category");
            assertThat(((Number) record.get("total")).longValue())
                    .as("SUM for category %s", cat)
                    .isEqualTo(expectedSum.get(cat));
            assertThat(((Number) record.get("cnt")).longValue())
                    .as("COUNT for category %s", cat)
                    .isEqualTo(expectedCount.get(cat));
        }

        // Phase 2: DELETE all rows → aggregation values should be zero
        List<CdcEnvelope> deletes = input.rows().stream()
                .map(r -> deleteEnvelope("src", r.category(), r.amount()))
                .toList();
        IncrementalResult deleteResult = evaluator.evaluate(dtState, ChangeResult.cdcBatch(deletes));

        // After full retract, all aggregation values in the result should be zero
        for (Map<String, Object> record : deleteResult.records()) {
            assertThat(((Number) record.get("total")).longValue())
                    .as("After full retract, SUM should be 0")
                    .isEqualTo(0L);
            assertThat(((Number) record.get("cnt")).longValue())
                    .as("After full retract, COUNT should be 0")
                    .isEqualTo(0L);
        }
    }

    @Provide
    Arbitrary<AggregationInput> aggregationInputs() {
        Arbitrary<String> categories = Arbitraries.of("A", "B", "C", "D");
        Arbitrary<Long> amounts = Arbitraries.longs().between(1, 10_000);
        Arbitrary<TestRow> rows = Combinators.combine(categories, amounts).as(TestRow::new);
        return rows.list().ofMinSize(1).ofMaxSize(30).map(AggregationInput::new);
    }

    // ══════════════════════════════════════════════════════════════════
    // Property 3: CdcBatch FilterProject Behavior (3.3)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 3: For all CdcBatch inputs, FilterProjectEvaluator output
     * matches filter predicate application.
     *
     * <p>Only INSERT and UPDATE rows passing the filter appear in output.
     * DELETE rows are always excluded.</p>
     *
     * <p><b>Validates: Requirements 3.3</b></p>
     */
    @Property(tries = 10)
    void cdcBatchFilterProject_outputMatchesFilterPredicate(
            @ForAll("filterProjectInputs") FilterProjectInput input) {

        long threshold = input.threshold();
        Predicate<Map<String, Object>> filter = row -> {
            Object val = row.get("value");
            if (val instanceof Number n) {
                return n.longValue() > threshold;
            }
            return false;
        };
        List<String> projectionColumns = List.of("name", "value");

        FilterProjectEvaluator evaluator = new FilterProjectEvaluator(filter, projectionColumns);
        DynamicTableState dtState = cdcState("fp_preserve_dt");

        IncrementalResult result = evaluator.evaluate(
                dtState, ChangeResult.cdcBatch(input.events()));

        // Compute expected output manually
        List<Map<String, Object>> expected = new ArrayList<>();
        for (CdcEnvelope env : input.events()) {
            if (env.operation() == CdcOperation.DELETE) {
                continue;
            }
            Map<String, Object> row = env.after() != null ? env.after().toMap() : null;
            if (row != null && filter.test(row)) {
                Map<String, Object> projected = new LinkedHashMap<>();
                for (String col : projectionColumns) {
                    projected.put(col, row.get(col));
                }
                expected.add(projected);
            }
        }

        assertThat(result.records())
                .as("FilterProject output must match manual filter+project")
                .hasSize(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertThat(result.records().get(i))
                    .as("Row %d must match expected projection", i)
                    .isEqualTo(expected.get(i));
        }
    }

    @Provide
    Arbitrary<FilterProjectInput> filterProjectInputs() {
        Arbitrary<Long> thresholds = Arbitraries.longs().between(10, 90);
        Arbitrary<List<CdcEnvelope>> eventLists = filterProjectEventArbitrary()
                .list().ofMinSize(1).ofMaxSize(30);
        return Combinators.combine(thresholds, eventLists).as(FilterProjectInput::new);
    }

    private Arbitrary<CdcEnvelope> filterProjectEventArbitrary() {
        Arbitrary<CdcOperation> ops = Arbitraries.of(
                CdcOperation.INSERT, CdcOperation.DELETE, CdcOperation.UPDATE);
        Arbitrary<String> names = Arbitraries.of("alice", "bob", "carol", "dave");
        Arbitrary<Long> values = Arbitraries.longs().between(1, 100);

        return Combinators.combine(ops, names, values).as((op, name, value) -> {
            CdcRow row = CdcRow.of(Map.of("name", name, "value", value));
            return switch (op) {
                case INSERT -> new CdcEnvelope(
                        CdcOperation.INSERT, "src", null, row, Instant.now(), 1, Map.of());
                case DELETE -> new CdcEnvelope(
                        CdcOperation.DELETE, "src", row, null, Instant.now(), 1, Map.of());
                case UPDATE -> new CdcEnvelope(
                        CdcOperation.UPDATE, "src", row, row, Instant.now(), 1, Map.of());
            };
        });
    }

    // ══════════════════════════════════════════════════════════════════
    // Property 4: ALTER Dispatch Correctness (3.4)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 4: For all valid ALTER actions, DynamicTableDdlExecutor
     * dispatches to the correct handler and returns appropriate DdlResult.
     *
     * <p><b>Validates: Requirements 3.4</b></p>
     */
    @Property(tries = 10)
    void alterDispatch_correctHandlerForEachAction(
            @ForAll("alterActions") AlterDynamicTableAction action) {

        InMemoryDynamicTableStateStore stateStore = new InMemoryDynamicTableStateStore();
        InMemoryRefreshHistoryStore historyStore = new InMemoryRefreshHistoryStore();
        MaterializedViewRegistry mvManager = new MaterializedViewRegistry();
        DynamicTableDagManager dagManager = new DynamicTableDagManager();
        DynamicTableConfig config = DynamicTableConfig.defaults();

        String dtName = "alter_test_dt";
        DynamicTableState state = activeState(dtName);
        stateStore.saveState(state);

        mvManager.register(new MaterializedViewDefinition(
                dtName, "SELECT * FROM src", RefreshStrategy.INCREMENTAL,
                false, Optional.empty(), Instant.now()));

        RefreshSchedulerContext schedulerDeps = new RefreshSchedulerContext(
                (s, t) -> RefreshResult.noOp(), stateStore, config);
        RefreshScheduler scheduler = new RefreshScheduler(schedulerDeps);

        try {
            IncrementalFeasibilityChecker checker = new IncrementalFeasibilityChecker();
            TestCdcLifecycleManager cdcManager = new TestCdcLifecycleManager();
            CountingRefreshExecutor refreshExecutor = new CountingRefreshExecutor();

            DynamicTableDdlContext deps = new DynamicTableDdlContext(
                    stateStore, historyStore, mvManager, scheduler,
                    dagManager, checker, cdcManager, refreshExecutor, config);
            DynamicTableDdlExecutor executor = new DynamicTableDdlExecutor(deps);

            AlterDynamicTableStatement stmt = new AlterDynamicTableStatement(
                    Identifier.of(dtName), action);

            DdlResult result = executor.alter(stmt, testSessionContext());

            if (action instanceof AlterDynamicTableAction.Resume) {
                // Resume on ACTIVE DT is rejected
                assertThat(result).isInstanceOf(DdlResult.Rejected.class);
            } else {
                assertThat(result)
                        .as("ALTER %s should produce Success", action.getClass().getSimpleName())
                        .isInstanceOf(DdlResult.Success.class);
            }

            if (action instanceof AlterDynamicTableAction.Suspend) {
                DynamicTableState updated = stateStore.loadState(dtName).orElseThrow();
                assertThat(updated.state()).isEqualTo(DynamicTableLifecycleState.PAUSED);
            }
            if (action instanceof AlterDynamicTableAction.SetTargetLag stl) {
                DynamicTableState updated = stateStore.loadState(dtName).orElseThrow();
                assertThat(updated.targetLag()).isEqualTo(stl.newLag());
            }
        } finally {
            scheduler.close();
        }
    }

    @Provide
    Arbitrary<AlterDynamicTableAction> alterActions() {
        return Arbitraries.of(
                new AlterDynamicTableAction.Suspend(),
                new AlterDynamicTableAction.Resume(),
                new AlterDynamicTableAction.Refresh(),
                new AlterDynamicTableAction.SetTargetLag(Duration.ofMinutes(10)));
    }

    // ══════════════════════════════════════════════════════════════════
    // Property 5: DAG Topological Order Consistency (3.6)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 5: For all valid DAG topologies, topologicalOrder() produces
     * consistent ordering where every upstream appears before its downstream.
     *
     * <p><b>Validates: Requirements 3.6</b></p>
     */
    @Property(tries = 10)
    void dagTopologicalOrder_producesConsistentOrdering(
            @ForAll("acyclicGraphs") AcyclicGraph graph) {

        DynamicTableDagManager dag = new DynamicTableDagManager();
        for (var entry : graph.edges().entrySet()) {
            dag.registerDependencies(entry.getKey(), entry.getValue());
        }

        List<String> order = dag.topologicalOrder();
        Map<String, Integer> position = new HashMap<>();
        for (int i = 0; i < order.size(); i++) {
            position.put(order.get(i), i);
        }

        for (var entry : graph.edges().entrySet()) {
            String downstream = entry.getKey();
            for (String upstream : entry.getValue()) {
                if (position.containsKey(upstream) && position.containsKey(downstream)) {
                    assertThat(position.get(upstream))
                            .as("upstream %s must appear before downstream %s",
                                    upstream, downstream)
                            .isLessThan(position.get(downstream));
                }
            }
        }

        Set<String> allNodes = new HashSet<>();
        allNodes.addAll(graph.edges().keySet());
        graph.edges().values().forEach(allNodes::addAll);
        assertThat(new HashSet<>(order))
                .as("All nodes must appear in topological order")
                .containsAll(allNodes);
    }

    @Provide
    Arbitrary<AcyclicGraph> acyclicGraphs() {
        return Arbitraries.integers().between(2, 8).flatMap(nodeCount -> {
            List<String> nodes = new ArrayList<>();
            for (int i = 0; i < nodeCount; i++) {
                nodes.add("node_" + i);
            }
            return Arbitraries.just(nodes).map(n -> {
                Map<String, Set<String>> edgeMap = new LinkedHashMap<>();
                Random rng = new Random();
                for (int i = 1; i < n.size(); i++) {
                    Set<String> deps = new HashSet<>();
                    for (int j = 0; j < i; j++) {
                        if (rng.nextBoolean()) {
                            deps.add(n.get(j));
                        }
                    }
                    if (!deps.isEmpty()) {
                        edgeMap.put(n.get(i), deps);
                    }
                }
                return new AcyclicGraph(edgeMap);
            });
        });
    }

    // ══════════════════════════════════════════════════════════════════
    // Property 6: MaterializedViewRegistry Internal API (3.7)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 6: For all Dynamic Table lifecycle operations,
     * MaterializedViewRegistry internal API functions correctly.
     *
     * <p>register(), getDefinition(), getAllDefinitions(), dropMaterializedView()
     * must work correctly for Dynamic Table query rewrite registration.</p>
     *
     * <p><b>Validates: Requirements 3.7</b></p>
     */
    @Property(tries = 10)
    void mvManagerInternalApi_functionsCorrectly(
            @ForAll("mvDefinitions") List<MvDefInput> definitions) {

        MaterializedViewRegistry manager = new MaterializedViewRegistry();

        // Register all definitions (last-write-wins for duplicate names)
        Map<String, String> lastQueryByName = new LinkedHashMap<>();
        for (MvDefInput def : definitions) {
            MaterializedViewDefinition mvDef = new MaterializedViewDefinition(
                    def.name(), def.query(), RefreshStrategy.INCREMENTAL,
                    false, Optional.empty(), Instant.now());
            manager.register(mvDef);
            lastQueryByName.put(def.name(), def.query());
        }

        // Verify getDefinition returns the last registered definition for each name
        for (var entry : lastQueryByName.entrySet()) {
            Optional<MaterializedViewDefinition> loaded = manager.getDefinition(entry.getKey());
            assertThat(loaded)
                    .as("getDefinition(%s) should be present", entry.getKey())
                    .isPresent();
            assertThat(loaded.get().name()).isEqualTo(entry.getKey());
            assertThat(loaded.get().query()).isEqualTo(entry.getValue());
            assertThat(loaded.get().refreshStrategy()).isEqualTo(RefreshStrategy.INCREMENTAL);
        }

        // Verify getAllDefinitions returns all unique names
        assertThat(manager.getAllDefinitions())
                .as("getAllDefinitions should return all unique definitions")
                .hasSize(lastQueryByName.size());

        // Drop one and verify it's gone
        if (!lastQueryByName.isEmpty()) {
            String toDrop = lastQueryByName.keySet().iterator().next();
            manager.dropMaterializedView(toDrop);
            assertThat(manager.getDefinition(toDrop))
                    .as("After drop, getDefinition(%s) should be empty", toDrop)
                    .isEmpty();
        }
    }

    @Provide
    Arbitrary<List<MvDefInput>> mvDefinitions() {
        Arbitrary<String> names = Arbitraries.integers().between(1, 10)
                .map(i -> "dt_mv_" + i);
        Arbitrary<String> queries = Arbitraries.of(
                "SELECT * FROM src",
                "SELECT a, SUM(b) FROM src GROUP BY a",
                "SELECT x, y FROM t WHERE x > 0");
        Arbitrary<MvDefInput> defs = Combinators.combine(names, queries).as(MvDefInput::new);
        return defs.list().ofMinSize(1).ofMaxSize(10);
    }

    // ══════════════════════════════════════════════════════════════════
    // Records
    // ══════════════════════════════════════════════════════════════════

    record TestRow(String category, long amount) {}
    record AggregationInput(List<TestRow> rows) {}
    record FilterProjectInput(long threshold, List<CdcEnvelope> events) {}
    record AcyclicGraph(Map<String, Set<String>> edges) {}
    record MvDefInput(String name, String query) {}

    // ══════════════════════════════════════════════════════════════════
    // Helper: State Factories
    // ══════════════════════════════════════════════════════════════════

    private static DynamicTableState cdcState(String dtName) {
        Instant now = Instant.now();
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.CDC,
                Duration.ofMinutes(5), "offset-100", "CDC_OFFSET",
                1, null, "admin", "coord-1", 1L,
                null, 0L, 0, now, now);
    }

    private static DynamicTableState activeState(String dtName) {
        Instant now = Instant.now();
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.CDC,
                Duration.ofMinutes(5), "offset-0", "CDC_OFFSET",
                1, null, "admin", "coord-1", 1L,
                null, 0L, 0, now, now);
    }

    // ══════════════════════════════════════════════════════════════════
    // Helper: CDC Event Generators
    // ══════════════════════════════════════════════════════════════════

    private static List<CdcEnvelope> generateInsertEvents(
            String source, String keyCol, int count) {
        List<CdcEnvelope> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(new CdcEnvelope(
                    CdcOperation.INSERT, source, null,
                    CdcRow.of(Map.of(keyCol, "k" + (i % 5), "value", (long) i)),
                    Instant.now(), 1, Map.of()));
        }
        return events;
    }

    private static CdcEnvelope insertEnvelope(String source, String category, long amount) {
        return new CdcEnvelope(
                CdcOperation.INSERT, source, null,
                CdcRow.of(Map.of("category", category, "amount", amount)),
                Instant.now(), 1, Map.of());
    }

    private static CdcEnvelope deleteEnvelope(String source, String category, long amount) {
        return new CdcEnvelope(
                CdcOperation.DELETE, source,
                CdcRow.of(Map.of("category", category, "amount", amount)),
                null, Instant.now(), 1, Map.of());
    }

    // ══════════════════════════════════════════════════════════════════
    // Helper: Writer and Cache
    // ══════════════════════════════════════════════════════════════════

    private static DynamicTableWriter recordingWriter(int expectedRows) {
        return new DynamicTableWriter(new RecordingTableWriter(expectedRows), noOpCache());
    }

    private static QueryResultCache noOpCache() {
        return new QueryResultCache(new NoOpL2CacheStore());
    }

    private static CoordinatorSessionContext testSessionContext() {
        UserIdentity identity = new UserIdentity(
                "test-user", Set.of("admin"), Map.of(), Instant.now());
        return new CoordinatorSessionContext("q-test", Map.of(), identity, "tenant-1");
    }

    // ══════════════════════════════════════════════════════════════════
    // Stub Implementations
    // ══════════════════════════════════════════════════════════════════

    static final class RecordingTableWriter implements TableWriter {
        private final int rowCount;

        RecordingTableWriter(int rowCount) { this.rowCount = rowCount; }

        @Override
        public org.pangrova.spi.connector.data.write.model.WriteResult write(
                String tableName, List<WriteRecord> records,
                WriteMode writeMode, WriteContext context) {
            return new org.pangrova.spi.connector.data.write.model.WriteResult(
                    tableName, rowCount, 1, 0, 0, Instant.now());
        }

        @Override
        public TableFormat format() { return TableFormat.ICEBERG; }

        @Override
        public void close() {}
    }

    static final class NoOpL2CacheStore implements L2CacheStore {
        @Override public Optional<CachedResult> get(String key) { return Optional.empty(); }
        @Override public void put(String key, CachedResult value) {}
        @Override public void remove(String key) {}
        @Override public void removeByTable(String tableName) {}
        @Override public void removeByUser(String userId) {}
    }

    static final class TestCdcLifecycleManager implements CdcLifecycleManager {
        @Override
        public boolean isAvailable(String tableName) { return true; }

        @Override
        public String resolveOffsetFromSnapshot(String tableName, String snapshotCheckpoint) {
            return "offset-0";
        }

        @Override
        public CdcRegistrationResult register(String consumerId, String tenantId,
                                               int maxConcurrentWrites, long maxPendingBytes) {
            return new CdcRegistrationResult("stream-1", maxConcurrentWrites,
                    maxPendingBytes, 5000, 30000);
        }

        @Override public void heartbeat(String streamId) {}
        @Override public void expireStaleStreams() {}
    }

    static final class CountingRefreshExecutor implements RefreshExecutor {
        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            return RefreshResult.noOp();
        }
    }
}
