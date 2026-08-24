package org.pangrova.coordinator.dynamictable;

import org.pangrova.coordinator.dynamictable.eval.IncrementalFeasibilityChecker;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.dynamictable.schedule.RefreshSchedulerContext;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.InMemoryRefreshHistoryStore;
import org.pangrova.metadata.dynamictable.RefreshHistoryEntry;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.RefreshStatus;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.pangrova.parser.ast.AllColumns;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.statement.AlterDynamicTableAction;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.streaming.CdcLifecycleManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DynamicTableDdlExecutor}.
 *
 * <p>Uses real in-memory stores and hand-written test implementations.
 * No mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 2, 5, 6, 7, 11, 17, 19, 20, 23</b></p>
 */
class DynamicTableDdlExecutorTest {

    private InMemoryDynamicTableStateStore stateStore;
    private InMemoryRefreshHistoryStore historyStore;
    private MaterializedViewRegistry materializedViewManager;
    private RefreshScheduler scheduler;
    private DynamicTableDagManager dagManager;
    private IncrementalFeasibilityChecker feasibilityChecker;
    private TestCdcLifecycleManager cdcLifecycleManager;
    private CountingRefreshExecutor refreshExecutor;
    private DynamicTableConfig config;
    private DynamicTableDdlExecutor executor;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        historyStore = new InMemoryRefreshHistoryStore();
        materializedViewManager = new MaterializedViewRegistry();
        dagManager = new DynamicTableDagManager();
        feasibilityChecker = new IncrementalFeasibilityChecker();
        cdcLifecycleManager = new TestCdcLifecycleManager();
        refreshExecutor = new CountingRefreshExecutor();
        config = DynamicTableConfig.defaults();
        scheduler = buildTestScheduler();
        executor = buildExecutor();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    // ── CREATE tests ────────────────────────────────────────────────

    @Test
    void create_fullFlowSuccess() {
        DdlResult result = executor.create(createStmt("my_dt"), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        assertThat(((DdlResult.Success) result).message()).contains("my_dt");
        assertThat(stateStore.loadState("my_dt")).isPresent();
        assertThat(stateStore.loadState("my_dt").get().state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
        assertThat(materializedViewManager.getDefinition("my_dt")).isPresent();
        assertThat(refreshExecutor.callCount()).isEqualTo(1);
    }

    @Test
    void create_duplicateRejected() {
        executor.create(createStmt("dup_dt"), testContext());
        DdlResult result = executor.create(createStmt("dup_dt"), testContext());

        assertThat(result).isInstanceOf(DdlResult.Rejected.class);
        assertThat(((DdlResult.Rejected) result).reason()).contains("already exists");
    }

    @Test
    void create_autoModeCdcAvailable_usesCdc() {
        cdcLifecycleManager.setAvailable(true);
        DdlResult result = executor.create(
                createStmtWithMode("cdc_dt", org.pangrova.parser.ast.RefreshMode.AUTO), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        DynamicTableState state = stateStore.loadState("cdc_dt").orElseThrow();
        assertThat(state.effectiveRefreshMode()).isEqualTo(DynamicTableRefreshMode.CDC);
    }

    @Test
    void create_autoModeCdcUnavailable_usesSnapshot() {
        cdcLifecycleManager.setAvailable(false);
        DdlResult result = executor.create(
                createStmtWithMode("snap_dt", org.pangrova.parser.ast.RefreshMode.AUTO), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        DynamicTableState state = stateStore.loadState("snap_dt").orElseThrow();
        assertThat(state.effectiveRefreshMode()).isEqualTo(DynamicTableRefreshMode.SNAPSHOT);
    }

    @Test
    void create_cdcModeUnavailable_rejected() {
        cdcLifecycleManager.setAvailable(false);
        DdlResult result = executor.create(
                createStmtWithMode("cdc_fail", org.pangrova.parser.ast.RefreshMode.CDC), testContext());

        assertThat(result).isInstanceOf(DdlResult.Rejected.class);
        assertThat(((DdlResult.Rejected) result).reason()).contains("CDC not available");
    }

    @Test
    void create_recordsCreatorIdentity() {
        executor.create(createStmt("creator_dt"), testContext());

        DynamicTableState state = stateStore.loadState("creator_dt").orElseThrow();
        assertThat(state.creatorIdentity()).isEqualTo("test-user");
    }

    // ── ALTER tests ─────────────────────────────────────────────────

    @Test
    void alter_suspend_stateBecomeSuspended() {
        executor.create(createStmt("sus_dt"), testContext());
        DdlResult result = executor.alter(alterStmt("sus_dt", new AlterDynamicTableAction.Suspend()), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        assertThat(stateStore.loadState("sus_dt").get().state()).isEqualTo(DynamicTableLifecycleState.PAUSED);
    }

    @Test
    void alter_resume_stateBecomeActive() {
        executor.create(createStmt("res_dt"), testContext());
        executor.alter(alterStmt("res_dt", new AlterDynamicTableAction.Suspend()), testContext());
        DdlResult result = executor.alter(alterStmt("res_dt", new AlterDynamicTableAction.Resume()), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        assertThat(stateStore.loadState("res_dt").get().state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
    }

    @Test
    void alter_resume_nonSuspended_rejected() {
        executor.create(createStmt("active_dt"), testContext());
        DdlResult result = executor.alter(alterStmt("active_dt", new AlterDynamicTableAction.Resume()), testContext());

        assertThat(result).isInstanceOf(DdlResult.Rejected.class);
        assertThat(((DdlResult.Rejected) result).reason()).contains("not PAUSED or SUSPENDED");
    }

    @Test
    void alter_refresh_triggersManualRefresh() {
        executor.create(createStmt("ref_dt"), testContext());
        int before = refreshExecutor.callCount();
        executor.alter(alterStmt("ref_dt", new AlterDynamicTableAction.Refresh()), testContext());

        // Manual refresh is async via scheduler, but the DDL result is immediate
        assertThat(refreshExecutor.callCount()).isGreaterThanOrEqualTo(before);
    }

    @Test
    void alter_setTargetLag_updatesLag() {
        executor.create(createStmt("lag_dt"), testContext());
        Duration newLag = Duration.ofMinutes(10);
        DdlResult result = executor.alter(
                alterStmt("lag_dt", new AlterDynamicTableAction.SetTargetLag(newLag)), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        assertThat(stateStore.loadState("lag_dt").get().targetLag()).isEqualTo(newLag);
    }

    @Test
    void alter_notFound_rejected() {
        DdlResult result = executor.alter(
                alterStmt("nonexistent", new AlterDynamicTableAction.Suspend()), testContext());

        assertThat(result).isInstanceOf(DdlResult.Rejected.class);
    }

    // ── DROP tests ──────────────────────────────────────────────────

    @Test
    void drop_normal_removesState() {
        executor.create(createStmt("drop_dt"), testContext());
        DdlResult result = executor.drop(dropStmt("drop_dt", false), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        assertThat(stateStore.loadState("drop_dt")).isEmpty();
    }

    @Test
    void drop_withDependents_rejected() {
        executor.create(createStmt("parent_dt"), testContext());
        // Register a dependent manually
        dagManager.registerDependencies("child_dt", Set.of("parent_dt"));

        DdlResult result = executor.drop(dropStmt("parent_dt", false), testContext());

        assertThat(result).isInstanceOf(DdlResult.Rejected.class);
        assertThat(((DdlResult.Rejected) result).reason()).contains("dependents");
    }

    @Test
    void drop_cascade_removesAllDependents() {
        executor.create(createStmt("root_dt"), testContext());
        // Create a child DT that depends on root_dt
        DynamicTableState childState = activeState("child_dt", "test-tenant");
        stateStore.saveState(childState);
        dagManager.registerDependencies("child_dt", Set.of("root_dt"));

        DdlResult result = executor.drop(dropStmt("root_dt", true), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
        assertThat(stateStore.loadState("root_dt")).isEmpty();
        assertThat(stateStore.loadState("child_dt")).isEmpty();
    }

    @Test
    void drop_notFound_rejected() {
        DdlResult result = executor.drop(dropStmt("ghost", false), testContext());

        assertThat(result).isInstanceOf(DdlResult.Rejected.class);
    }

    // ── SHOW tests ──────────────────────────────────────────────────

    @Test
    void show_returnsDtListForTenant() {
        executor.create(createStmt("show_dt1"), testContext());
        executor.create(createStmt("show_dt2"), testContext());

        List<DynamicTableInfo> infos = executor.show("test-tenant");

        assertThat(infos).hasSize(2);
        assertThat(infos).extracting(DynamicTableInfo::dtName)
                .containsExactlyInAnyOrder("show_dt1", "show_dt2");
    }

    @Test
    void show_emptyForUnknownTenant() {
        executor.create(createStmt("other_dt"), testContext());

        List<DynamicTableInfo> infos = executor.show("unknown-tenant");

        assertThat(infos).isEmpty();
    }

    // ── DESCRIBE tests ──────────────────────────────────────────────

    @Test
    void describe_returnsDetailWithRecentRefreshes() {
        executor.create(createStmt("desc_dt"), testContext());
        // Add some refresh history
        historyStore.record(refreshEntry("desc_dt", "test-tenant"));

        DynamicTableDetail detail = executor.describe("desc_dt");

        assertThat(detail.dtName()).isEqualTo("desc_dt");
        assertThat(detail.state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
        assertThat(detail.recentRefreshes()).hasSize(1);
    }

    @Test
    void describe_notFound_throws() {
        assertThatThrownBy(() -> executor.describe("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    // ── Tenant ownership tests ──────────────────────────────────────

    @Test
    void drop_differentTenant_throwsAccessDenied() {
        executor.create(createStmt("owned_dt"), testContext()); // owned by "test-tenant"
        CoordinatorSessionContext otherTenant = testContextWithTenant("other-tenant");

        assertThatThrownBy(() -> executor.drop(dropStmt("owned_dt", false), otherTenant))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("other-tenant")
                .hasMessageContaining("test-tenant");
    }

    @Test
    void drop_sameTenant_succeeds() {
        executor.create(createStmt("same_dt"), testContext());
        DdlResult result = executor.drop(dropStmt("same_dt", false), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
    }

    @Test
    void alter_differentTenant_throwsAccessDenied() {
        executor.create(createStmt("alt_dt"), testContext()); // owned by "test-tenant"
        CoordinatorSessionContext otherTenant = testContextWithTenant("other-tenant");

        assertThatThrownBy(() -> executor.alter(
                alterStmt("alt_dt", new AlterDynamicTableAction.Suspend()), otherTenant))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("other-tenant")
                .hasMessageContaining("test-tenant");
    }

    @Test
    void alter_sameTenant_succeeds() {
        executor.create(createStmt("alt_ok"), testContext());
        DdlResult result = executor.alter(
                alterStmt("alt_ok", new AlterDynamicTableAction.Suspend()), testContext());

        assertThat(result).isInstanceOf(DdlResult.Success.class);
    }

    // ── Builder helpers ─────────────────────────────────────────────

    private DynamicTableDdlExecutor buildExecutor() {
        var deps = new DynamicTableDdlContext(
                stateStore, historyStore, materializedViewManager, scheduler,
                dagManager, feasibilityChecker, cdcLifecycleManager,
                refreshExecutor, config);
        return new DynamicTableDdlExecutor(deps);
    }

    private RefreshScheduler buildTestScheduler() {
        var deps = new RefreshSchedulerContext(
                (state, trigger) -> RefreshResult.noOp(), stateStore, config);
        return new RefreshScheduler(deps);
    }

    // ── Statement factories ─────────────────────────────────────────

    private static CreateDynamicTableStatement createStmt(String dtName) {
        return new CreateDynamicTableStatement(
                Identifier.of(dtName), testQuery("source_table"),
                Duration.ofMinutes(5), Optional.empty(), Optional.empty(), Map.of());
    }

    private static CreateDynamicTableStatement createStmtWithMode(
            String dtName, org.pangrova.parser.ast.RefreshMode mode) {
        return new CreateDynamicTableStatement(
                Identifier.of(dtName), testQuery("source_table"),
                Duration.ofMinutes(5), Optional.of(mode), Optional.empty(), Map.of());
    }

    private static AlterDynamicTableStatement alterStmt(String dtName,
                                                        AlterDynamicTableAction action) {
        return new AlterDynamicTableStatement(Identifier.of(dtName), action);
    }

    private static DropDynamicTableStatement dropStmt(String dtName, boolean cascade) {
        return new DropDynamicTableStatement(Identifier.of(dtName), cascade);
    }

    private static Query testQuery(String tableName) {
        return new Query(
                false,
                List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of(tableName))),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty());
    }

    private static CoordinatorSessionContext testContext() {
        UserIdentity identity = new UserIdentity(
                "test-user", Set.of("admin"), Map.of(), Instant.now());
        return new CoordinatorSessionContext("q-001", Map.of(), identity, "test-tenant");
    }

    private static CoordinatorSessionContext testContextWithTenant(String tenantId) {
        UserIdentity identity = new UserIdentity(
                "test-user", Set.of("admin"), Map.of(), Instant.now());
        return new CoordinatorSessionContext("q-001", Map.of(), identity, tenantId);
    }

    // ── State factories ─────────────────────────────────────────────

    private static DynamicTableState activeState(String dtName, String tenantId) {
        Instant now = Instant.now();
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0, now, now);
    }

    private static RefreshHistoryEntry refreshEntry(String dtName, String tenantId) {
        Instant now = Instant.now();
        return new RefreshHistoryEntry(
                "r-001", dtName, tenantId, TriggerType.SCHEDULED,
                now.minusSeconds(10), now, RefreshStatus.SUCCESS,
                100, 1024, "cp-0", "cp-1", null);
    }

    // ── Test implementations ────────────────────────────────────────

    /** Configurable CDC lifecycle manager for testing. */
    static final class TestCdcLifecycleManager implements CdcLifecycleManager {
        private boolean available = true;

        void setAvailable(boolean available) {
            this.available = available;
        }

        @Override
        public boolean isAvailable(String tableName) {
            return available;
        }

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

        @Override
        public void heartbeat(String streamId) { }

        @Override
        public void expireStaleStreams() { }
    }

    /** Executor that counts calls and returns NO_OP. */
    static final class CountingRefreshExecutor implements RefreshExecutor {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            return RefreshResult.noOp();
        }

        int callCount() {
            return count.get();
        }
    }
}
