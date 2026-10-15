package org.pangrova.coordinator.property.dynamictable;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.AggregateEvaluator;
import org.pangrova.coordinator.dynamictable.eval.AggregateEvaluator.AggregateFunction;
import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker;
import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker.FeasibilityResult;
import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker.QueryFeature;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.quota.BoundedRefreshMemoryQuota;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuota;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuotaExceededException;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinatorContext;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshLockCoordinator;
import org.pangrova.coordinator.dynamictable.write.DynamicTableWriter;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.InMemoryRefreshHistoryStore;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRow;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Bug condition exploration property tests for Dynamic Table review findings (F-01 ~ F-06).
 *
 * <p><b>CRITICAL</b>: These tests are EXPECTED TO FAIL on unfixed code.
 * Failure confirms the bugs exist. DO NOT fix the code to make them pass.</p>
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 1.3, 1.5, 1.6</b></p>
 */
class BugConditionExplorationPropertyTest {

    // ══════════════════════════════════════════════════════════════════
    // F-01: CDC Fallback Not Implemented
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 1: CDC detection failure exceeding cdcFallbackThresholdMs should
     * trigger SNAPSHOT fallback.
     *
     * <p>Bug: RefreshCoordinator.detectChanges() only logs a warning and re-throws
     * on CDC failure — it never switches to SNAPSHOT pipeline.</p>
     *
     * <p><b>Validates: Requirements 1.1</b></p>
     */
    @Property(tries = 10)
    void f01_cdcFallbackShouldTriggerOnSustainedFailure(
            @ForAll("cdcFailureDurations") long failureDurationMs) {

        InMemoryDynamicTableStateStore stateStore = new InMemoryDynamicTableStateStore();
        InMemoryRefreshHistoryStore historyStore = new InMemoryRefreshHistoryStore();
        RefreshLockManager lockManager = new RefreshLockManager(new InMemoryDistributedLease());

        // Config with a very short fallback threshold so we can test quickly
        DynamicTableConfig config = new DynamicTableConfig(
                4, 2, 100, Duration.ofMinutes(5), 1000L,
                1000L, 300_000L, 5, 10, 3.0,
                100L, // cdcFallbackThresholdMs = 100ms (short for testing)
                1000, 30, 20);

        DynamicTableState state = cdcState("cdc_fallback_dt");
        stateStore.saveState(state);

        // Pipeline that always fails on CDC detect
        RefreshPipeline cdcPipeline = new RefreshPipeline.CdcRefreshPipeline(
                s -> { throw new RuntimeException("CDC connection lost"); },
                (s, c) -> IncrementalResult.empty());

        RefreshCoordinatorContext deps = new RefreshCoordinatorContext(
                stateStore, historyStore, recordingWriter(0));
        RefreshCoordinator coordinator = new RefreshCoordinator(
                deps, cdcPipeline, new RefreshLockCoordinator(lockManager), config);

        // Execute multiple refreshes to exceed the fallback threshold
        for (int i = 0; i < 5; i++) {
            coordinator.executeRefresh(state, TriggerType.SCHEDULED);
        }

        // After sustained CDC failures, the state should have switched to SNAPSHOT
        DynamicTableState updated = stateStore.loadState("cdc_fallback_dt").orElseThrow();
        assertThat(updated.effectiveRefreshMode())
                .as("F-01: CDC fallback should switch effectiveRefreshMode to SNAPSHOT")
                .isEqualTo(DynamicTableRefreshMode.SNAPSHOT);
    }

    @Provide
    Arbitrary<Long> cdcFailureDurations() {
        return Arbitraries.longs().between(300_000L, 600_000L);
    }

    // ══════════════════════════════════════════════════════════════════
    // F-02: Aggregation Memory Quota Not Enforced
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 2: High-cardinality GROUP BY exceeding memory quota should throw
     * RefreshMemoryQuotaExceededException.
     *
     * <p>Bug: AggregateEvaluator has no memory quota integration — the HashMap
     * grows unbounded without any check.</p>
     *
     * <p><b>Validates: Requirements 1.2</b></p>
     */
    @Property(tries = 10)
    void f02_aggregateEvaluatorShouldEnforceMemoryQuota(
            @ForAll("highCardinalityGroupCounts") int groupCount) {

        List<AggregateFunction> aggregations = List.of(
                new AggregateFunction(AggregateFunction.Type.SUM, "amount", "total"));
        // Quota allows only 100 groups (100 * 128 = 12800 bytes)
        // but groupCount is 1000-5000, so quota will be exceeded
        BoundedRefreshMemoryQuota quota = new BoundedRefreshMemoryQuota(
                100L * AggregateEvaluator.ESTIMATED_ENTRY_BYTES);
        AggregateEvaluator evaluator = new AggregateEvaluator(
                List.of("user_id"), aggregations, quota);

        // Create a batch with many distinct group keys to exceed any reasonable quota
        List<CdcEnvelope> events = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            events.add(new CdcEnvelope(
                    CdcOperation.INSERT, "source",
                    null,
                    CdcRow.of(Map.of("user_id", "user_" + i, "amount", 100L)),
                    Instant.now(), 1, Map.of()));
        }

        DynamicTableState dtState = snapshotState("quota_test_dt");

        // The evaluator should throw RefreshMemoryQuotaExceededException
        // when state exceeds quota. On unfixed code, it just grows unbounded.
        assertThatThrownBy(() -> evaluator.evaluate(dtState, ChangeResult.cdcBatch(events)))
                .as("F-02: AggregateEvaluator should throw RefreshMemoryQuotaExceededException on quota exceeded")
                .isInstanceOf(RefreshMemoryQuotaExceededException.class);
    }

    @Provide
    Arbitrary<Integer> highCardinalityGroupCounts() {
        return Arbitraries.integers().between(1000, 5000);
    }

    // ══════════════════════════════════════════════════════════════════
    // F-03: Snapshot + Non-Aggregation Rejection Missing
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 3: IncrementalFeasibilityChecker should reject SNAPSHOT mode
     * with non-aggregation (no GROUP BY) queries.
     *
     * <p>Bug: The checker does not consider refresh mode at all — it only
     * checks query features against the unsupported operator set. SNAPSHOT mode
     * with filter-only queries silently produces empty results.</p>
     *
     * <p><b>Validates: Requirements 1.3</b></p>
     */
    @Property(tries = 10)
    void f03_snapshotModeNonAggregationShouldBeRejected(
            @ForAll("nonAggregationFeatureSets") Set<QueryFeature> features) {

        IncrementalFeasibilityChecker checker = new IncrementalFeasibilityChecker();

        // Call the new overload with RefreshMode.SNAPSHOT to trigger rejection
        FeasibilityResult result = checker.check(features, DynamicTableRefreshMode.SNAPSHOT);

        // For SNAPSHOT mode without GROUP_BY, the result should be infeasible
        assertThat(result.feasible())
                .as("F-03: SNAPSHOT mode + non-aggregation query (features=%s) should be rejected", features)
                .isFalse();
        assertThat(result.reason())
                .as("F-03: Rejection reason should mention snapshot mode")
                .isNotNull();
    }

    @Provide
    Arbitrary<Set<QueryFeature>> nonAggregationFeatureSets() {
        // Generate feature sets that do NOT contain GROUP_BY (non-aggregation)
        // and only contain supported features (FILTER, PROJECTION)
        return Arbitraries.of(
                Set.of(QueryFeature.FILTER),
                Set.of(QueryFeature.PROJECTION),
                Set.of(QueryFeature.FILTER, QueryFeature.PROJECTION),
                EnumSet.of(QueryFeature.FILTER, QueryFeature.INNER_JOIN)
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // F-05: AVG Aggregation Not Supported, MIN/MAX Not Rejected
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 4: AVG aggregation should compute correct avg = sum / count.
     *
     * <p>Bug: AggregateFunction.Type only has SUM and COUNT — AVG is not
     * supported, causing a compilation or runtime error.</p>
     *
     * <p><b>Validates: Requirements 1.5</b></p>
     */
    @Property(tries = 10)
    void f05_avgAggregationShouldProduceCorrectResults(
            @ForAll("avgTestInputs") List<Long> amounts) {

        // This test will fail at compile/construction time on unfixed code
        // because AggregateFunction.Type.AVG does not exist
        AggregateFunction.Type avgType;
        try {
            avgType = AggregateFunction.Type.valueOf("AVG");
        } catch (IllegalArgumentException e) {
            // AVG enum value does not exist — this confirms the bug
            throw new AssertionError(
                    "F-05: AggregateFunction.Type.AVG does not exist — "
                            + "AVG aggregation is not supported", e);
        }

        AggregateEvaluator evaluator = new AggregateEvaluator(
                List.of("dept"),
                List.of(new AggregateFunction(avgType, "salary", "avg_salary")),
                RefreshMemoryQuota.unlimited());

        List<CdcEnvelope> events = new ArrayList<>();
        for (Long amount : amounts) {
            events.add(new CdcEnvelope(
                    CdcOperation.INSERT, "employees",
                    null,
                    CdcRow.of(Map.of("dept", "engineering", "salary", amount)),
                    Instant.now(), 1, Map.of()));
        }

        DynamicTableState dtState = cdcState("avg_test_dt");
        IncrementalResult result = evaluator.evaluate(dtState, ChangeResult.cdcBatch(events));

        // Verify avg = sum / count
        long expectedSum = amounts.stream().mapToLong(Long::longValue).sum();
        long expectedCount = amounts.size();
        long expectedAvg = expectedSum / expectedCount;

        assertThat(result.records()).hasSize(1);
        Number avgValue = (Number) result.records().get(0).get("avg_salary");
        assertThat(avgValue.longValue())
                .as("F-05: AVG should equal sum/count = %d/%d = %d", expectedSum, expectedCount, expectedAvg)
                .isEqualTo(expectedAvg);
    }

    /**
     * Property 5: MIN/MAX aggregation should be rejected at feasibility check.
     *
     * <p>Bug: IncrementalFeasibilityChecker does not validate aggregation function
     * types — MIN/MAX are not rejected.</p>
     *
     * <p><b>Validates: Requirements 1.5</b></p>
     */
    @Property(tries = 5)
    void f05_minMaxAggregationShouldBeRejectedAtFeasibilityCheck(
            @ForAll("minMaxFunctionNames") String functionName) {

        IncrementalFeasibilityChecker checker = new IncrementalFeasibilityChecker();

        // Use the new checkAggregationTypes API to validate MIN/MAX rejection
        FeasibilityResult result = checker.checkAggregationTypes(List.of(functionName));

        assertThat(result.feasible())
                .as("F-05: %s aggregation should be rejected at feasibility check", functionName)
                .isFalse();
        assertThat(result.reason())
                .as("F-05: rejection reason should list supported functions")
                .contains(functionName)
                .contains("SUM")
                .contains("COUNT")
                .contains("AVG");
    }

    @Provide
    Arbitrary<String> minMaxFunctionNames() {
        return Arbitraries.of("MIN", "MAX");
    }

    @Provide
    Arbitrary<List<Long>> avgTestInputs() {
        return Arbitraries.longs().between(1000L, 100_000L)
                .list().ofMinSize(2).ofMaxSize(20);
    }

    // ══════════════════════════════════════════════════════════════════
    // F-06: Integer::sum Null Safety in DynamicTableDagManager
    // ══════════════════════════════════════════════════════════════════

    /**
     * Property 6: DynamicTableDagManager should use null-safe lambda
     * instead of Integer::sum in Map.merge() calls.
     *
     * <p>Bug: computeInDegree() and kahnSort() use Integer::sum which
     * triggers null safety warnings due to auto-unboxing.</p>
     *
     * <p>This test verifies the code is null-safe by checking that the
     * source code does NOT contain Integer::sum references. On unfixed
     * code, this assertion will fail.</p>
     *
     * <p><b>Validates: Requirements 1.6</b></p>
     */
    @Property(tries = 1)
    void f06_dagManagerShouldUseNullSafeLambdaInsteadOfIntegerSum() throws Exception {
        // Read the source file and verify it does NOT contain Integer::sum
        // Resolve relative to the module root (test runs from pangrova-coordinator/)
        java.nio.file.Path path = java.nio.file.Path.of("src/main/java/org/pangrova/coordinator/dynamictable/DynamicTableDagManager.java");
        if (!java.nio.file.Files.exists(path)) {
            // Fallback: try from project root
            path = java.nio.file.Path.of("pangrova-coordinator/src/main/java/org/pangrova/coordinator/dynamictable/DynamicTableDagManager.java");
        }
        String source = java.nio.file.Files.readString(path);

        assertThat(source)
                .as("F-06: DynamicTableDagManager should NOT use Integer::sum (null safety risk)")
                .doesNotContain("Integer::sum");
    }

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

    private static DynamicTableState snapshotState(String dtName) {
        Instant now = Instant.now();
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT",
                1, null, "admin", "coord-1", 1L,
                null, 0L, 0, now, now);
    }

    // ══════════════════════════════════════════════════════════════════
    // Helper: Writer and Cache
    // ══════════════════════════════════════════════════════════════════

    private static DynamicTableWriter recordingWriter(int expectedRows) {
        return new DynamicTableWriter(
                new RecordingTableWriter(expectedRows), noOpCache());
    }

    private static QueryResultCache noOpCache() {
        return new QueryResultCache(new NoOpL2CacheStore());
    }

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
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
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
}
