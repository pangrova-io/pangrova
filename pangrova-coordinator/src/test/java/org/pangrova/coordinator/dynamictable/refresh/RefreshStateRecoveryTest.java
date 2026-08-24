package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.DynamicTableDagManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.refresh.recovery.RecoveryResult;
import org.pangrova.coordinator.dynamictable.refresh.recovery.RefreshStateRecovery;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaEvolutionChecker;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.dynamictable.schedule.RefreshSchedulerContext;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RefreshStateRecovery}.
 *
 * <p>Uses real {@link InMemoryDynamicTableStateStore} and hand-written
 * test implementations — no mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 7, 13</b></p>
 */
class RefreshStateRecoveryTest {

    private InMemoryDynamicTableStateStore stateStore;
    private DynamicTableDagManager dagManager;
    private RefreshScheduler scheduler;
    private SchemaEvolutionChecker schemaChecker;
    private RefreshStateRecovery recovery;
    private CountingRefreshExecutor executor;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        dagManager = new DynamicTableDagManager();
        schemaChecker = new SchemaEvolutionChecker();
        executor = new CountingRefreshExecutor();
        DynamicTableConfig config = DynamicTableConfig.defaults();
        var deps = new RefreshSchedulerContext(executor, stateStore, config);
        scheduler = new RefreshScheduler(deps);
        recovery = new RefreshStateRecovery(stateStore, dagManager, scheduler, schemaChecker);
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    // ── Normal recovery ─────────────────────────────────────────────

    @Test
    void recover_activeDtsRegisteredToScheduler() {
        stateStore.saveState(activeState("dt1", "t1"));
        stateStore.saveState(activeState("dt2", "t1"));

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.recovered()).isEqualTo(2);
        assertThat(result.skipped()).isZero();
        assertThat(result.suspended()).isZero();
        assertThat(scheduler.isRegistered("dt1")).isTrue();
        assertThat(scheduler.isRegistered("dt2")).isTrue();
    }

    @Test
    void recover_droppedAndSuspendedStatesIgnored() {
        stateStore.saveState(activeState("dt_active", "t1"));
        stateStore.saveState(droppedState("dt_dropped", "t1"));
        stateStore.saveState(suspendedState("dt_suspended", "t1"));

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.recovered()).isEqualTo(1);
        assertThat(result.total()).isEqualTo(1);
        assertThat(scheduler.isRegistered("dt_active")).isTrue();
        assertThat(scheduler.isRegistered("dt_dropped")).isFalse();
        assertThat(scheduler.isRegistered("dt_suspended")).isFalse();
    }

    @Test
    void recover_rebuildsDagFromDependencyResolver() {
        stateStore.saveState(activeState("dt_child", "t1"));

        Function<String, Set<String>> deps = name ->
                "dt_child".equals(name) ? Set.of("source_table") : Set.of();

        RecoveryResult result = recovery.recover(noSchemaChange(), deps);

        assertThat(result.recovered()).isEqualTo(1);
        assertThat(dagManager.getDirectDownstreams("source_table"))
                .contains("dt_child");
    }

    // ── Invalid checkpoint skip ─────────────────────────────────────

    @Test
    void recover_nullCheckpointSkipped() {
        stateStore.saveState(stateWithCheckpoint("dt_null_cp", "t1", null));

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.recovered()).isZero();
        assertThat(scheduler.isRegistered("dt_null_cp")).isFalse();
    }

    @Test
    void recover_blankCheckpointSkipped() {
        stateStore.saveState(stateWithCheckpoint("dt_blank_cp", "t1", "  "));

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.skipped()).isEqualTo(1);
        assertThat(scheduler.isRegistered("dt_blank_cp")).isFalse();
    }

    // ── Schema incompatible suspend ─────────────────────────────────

    @Test
    void recover_schemaIncompatibleSuspendsState() {
        DynamicTableState state = stateWithSchema("dt_schema", "t1", "id:INT,name:STRING");
        stateStore.saveState(state);

        // Current schema has column removed → incompatible
        Function<String, Map<String, String>> schemaProvider = name ->
                Map.of("id", "INT");

        RecoveryResult result = recovery.recover(schemaProvider, noDependencies());

        assertThat(result.suspended()).isEqualTo(1);
        assertThat(result.recovered()).isZero();
        DynamicTableState updated = stateStore.loadState("dt_schema").orElseThrow();
        assertThat(updated.state()).isEqualTo(DynamicTableLifecycleState.SUSPENDED);
        assertThat(scheduler.isRegistered("dt_schema")).isFalse();
    }

    @Test
    void recover_schemaCompatibleRecoveredNormally() {
        DynamicTableState state = stateWithSchema("dt_compat", "t1", "id:INT");
        stateStore.saveState(state);

        // Current schema adds a column → compatible
        Function<String, Map<String, String>> schemaProvider = name ->
                Map.of("id", "INT", "extra", "STRING");

        RecoveryResult result = recovery.recover(schemaProvider, noDependencies());

        assertThat(result.recovered()).isEqualTo(1);
        assertThat(result.suspended()).isZero();
        assertThat(scheduler.isRegistered("dt_compat")).isTrue();
    }

    @Test
    void recover_noRecordedSchemaSkipsCheck() {
        stateStore.saveState(activeState("dt_no_schema", "t1"));

        // Schema provider returns data, but recorded schema is null → skip check
        Function<String, Map<String, String>> schemaProvider = name ->
                Map.of("id", "INT");

        RecoveryResult result = recovery.recover(schemaProvider, noDependencies());

        assertThat(result.recovered()).isEqualTo(1);
    }

    // ── FAILING state recovery (R7 AC3) ─────────────────────────────

    @Test
    void recover_failingStateResetToActiveAndRegistered() {
        stateStore.saveState(failingState("dt_failing", "t1", 3));

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.recovered()).isEqualTo(1);
        DynamicTableState updated = stateStore.loadState("dt_failing").orElseThrow();
        assertThat(updated.state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
        assertThat(scheduler.isRegistered("dt_failing")).isTrue();
    }

    @Test
    void recover_failingStateWithNullCheckpointSkipped() {
        DynamicTableState failing = new DynamicTableState(
                "dt_fail_nocp", "t1", DynamicTableLifecycleState.FAILING, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), null, null, 1, null,
                "admin", "coord-1", 1L, null, 0, 5,
                Instant.now(), Instant.now());
        stateStore.saveState(failing);

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.recovered()).isZero();
    }

    // ── Mixed states ────────────────────────────────────────────────

    @Test
    void recover_mixedStatesProcessedCorrectly() {
        stateStore.saveState(activeState("dt_ok", "t1"));
        stateStore.saveState(stateWithCheckpoint("dt_bad_cp", "t1", null));
        stateStore.saveState(stateWithSchema("dt_bad_schema", "t1", "id:INT,name:STRING"));
        stateStore.saveState(failingState("dt_failing", "t1", 2));
        stateStore.saveState(droppedState("dt_dropped", "t1"));

        // Schema provider: remove column for dt_bad_schema
        Function<String, Map<String, String>> schemaProvider = name ->
                "dt_bad_schema".equals(name) ? Map.of("id", "INT") : Map.of();

        RecoveryResult result = recovery.recover(schemaProvider, noDependencies());

        assertThat(result.recovered()).isEqualTo(2); // dt_ok + dt_failing
        assertThat(result.skipped()).isEqualTo(1);   // dt_bad_cp
        assertThat(result.suspended()).isEqualTo(1); // dt_bad_schema
        assertThat(result.total()).isEqualTo(4);
    }

    // ── Empty store ─────────────────────────────────────────────────

    @Test
    void recover_emptyStoreReturnsZeroCounts() {
        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.recovered()).isZero();
        assertThat(result.skipped()).isZero();
        assertThat(result.suspended()).isZero();
    }

    // ── parseSimpleSchema ───────────────────────────────────────────

    @Test
    void parseSimpleSchema_parsesCorrectly() {
        Map<String, String> schema = RefreshStateRecovery.parseSimpleSchema("id:INT,name:STRING");
        assertThat(schema).containsEntry("id", "INT").containsEntry("name", "STRING");
    }

    @Test
    void parseSimpleSchema_emptyStringReturnsEmpty() {
        assertThat(RefreshStateRecovery.parseSimpleSchema("")).isEmpty();
    }

    // ── State factories ─────────────────────────────────────────────

    private static DynamicTableState activeState(String dtName, String tenantId) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState failingState(String dtName, String tenantId, int failures) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.FAILING, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, failures,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState droppedState(String dtName, String tenantId) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.DROPPED, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState suspendedState(String dtName, String tenantId) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.SUSPENDED, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState stateWithCheckpoint(String dtName, String tenantId,
                                                         String checkpoint) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), checkpoint, "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState stateWithSchema(String dtName, String tenantId,
                                                     String schemaSnapshot) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, schemaSnapshot,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private static Function<String, Map<String, String>> noSchemaChange() {
        return name -> Map.of();
    }

    private static Function<String, Set<String>> noDependencies() {
        return name -> Set.of();
    }

    // ── Test implementations ────────────────────────────────────────

    static final class CountingRefreshExecutor implements RefreshExecutor {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            return RefreshResult.noOp();
        }

        int callCount() { return count.get(); }
    }
}
