package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.DynamicTableDagManager;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.refresh.recovery.RecoveryResult;
import org.pangrova.coordinator.dynamictable.refresh.recovery.RefreshStateRecovery;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaEvolutionChecker;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.dynamictable.schedule.RefreshSchedulerContext;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for BUG-D15 fix: {@link RefreshStateRecovery#recoverStates} must
 * acquire a per-DT lock via {@link RefreshLockManager#tryAcquire} before
 * modifying state, and skip DTs locked by another coordinator.
 */
class RefreshStateRecoveryLockTest {

    private InMemoryDynamicTableStateStore stateStore;
    private DynamicTableDagManager dagManager;
    private RefreshScheduler scheduler;
    private SchemaEvolutionChecker schemaChecker;
    private InMemoryDistributedLease lease;
    private RefreshLockManager lockManager;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        dagManager = new DynamicTableDagManager();
        schemaChecker = new SchemaEvolutionChecker();
        lease = new InMemoryDistributedLease();
        lockManager = new RefreshLockManager(lease);

        DynamicTableConfig config = DynamicTableConfig.defaults();
        var executor = new CountingRefreshExecutor();
        var deps = new RefreshSchedulerContext(executor, stateStore, config);
        scheduler = new RefreshScheduler(deps);
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    @Test
    @DisplayName("recovery acquires per-DT lock and recovers when lock is available (BUG-D15)")
    void recoveryAcquiresLockAndRecovers() {
        stateStore.saveState(activeState("dt1", "t1"));
        stateStore.saveState(activeState("dt2", "t1"));

        RefreshStateRecovery recovery = new RefreshStateRecovery(
                stateStore, dagManager, scheduler, schemaChecker,
                Duration.ofMinutes(5), lockManager, "coord-1");

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.recovered()).isEqualTo(2);
        assertThat(result.skipped()).isZero();
    }

    @Test
    @DisplayName("recovery skips DT locked by another coordinator (BUG-D15)")
    void recoverySkipsDtLockedByAnotherCoordinator() {
        stateStore.saveState(activeState("dt_locked", "t1"));
        stateStore.saveState(activeState("dt_free", "t1"));

        // Pre-acquire lock for dt_locked by another coordinator
        lockManager.tryAcquire("dt_locked", "coord-other", 1, Duration.ofMinutes(5));

        RefreshStateRecovery recovery = new RefreshStateRecovery(
                stateStore, dagManager, scheduler, schemaChecker,
                Duration.ofMinutes(5), lockManager, "coord-1");

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        // dt_locked should be skipped, dt_free should be recovered
        assertThat(result.recovered()).isEqualTo(1);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(scheduler.isRegistered("dt_free")).isTrue();
        assertThat(scheduler.isRegistered("dt_locked")).isFalse();
    }

    @Test
    @DisplayName("recovery releases lock after processing each DT (BUG-D15)")
    void recoveryReleasesLockAfterProcessing() {
        stateStore.saveState(activeState("dt1", "t1"));

        RefreshStateRecovery recovery = new RefreshStateRecovery(
                stateStore, dagManager, scheduler, schemaChecker,
                Duration.ofMinutes(5), lockManager, "coord-1");

        recovery.recover(noSchemaChange(), noDependencies());

        // Lock should be released — another coordinator should be able to acquire it
        boolean acquired = lockManager.tryAcquire("dt1", "coord-other", 1, Duration.ofMinutes(1));
        assertThat(acquired).isTrue();
    }

    @Test
    @DisplayName("recovery without lockManager (backward compat) still works")
    void recoveryWithoutLockManagerStillWorks() {
        stateStore.saveState(activeState("dt1", "t1"));

        // Use the 2-arg constructor (no lockManager)
        RefreshStateRecovery recovery = new RefreshStateRecovery(
                stateStore, dagManager, scheduler, schemaChecker);

        RecoveryResult result = recovery.recover(noSchemaChange(), noDependencies());

        assertThat(result.recovered()).isEqualTo(1);
    }

    // ── State factories ─────────────────────────────────────────────

    private static DynamicTableState activeState(String dtName, String tenantId) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static Function<String, Map<String, String>> noSchemaChange() {
        return name -> Map.of();
    }

    private static Function<String, Set<String>> noDependencies() {
        return name -> Set.of();
    }

    static final class CountingRefreshExecutor implements RefreshExecutor {
        private final AtomicInteger count = new AtomicInteger(0);
        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            return RefreshResult.noOp();
        }
    }
}
