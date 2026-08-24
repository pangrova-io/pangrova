package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.DynamicTableDagManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.refresh.recovery.RecoveryResult;
import org.pangrova.coordinator.dynamictable.refresh.recovery.RefreshStateRecovery;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaEvolutionChecker;
import org.pangrova.coordinator.dynamictable.schedule.RefreshScheduler;
import org.pangrova.coordinator.dynamictable.schedule.RefreshSchedulerContext;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for HA failover with Dynamic Table refresh scheduling.
 *
 * <p>Uses {@link org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease} and real
 * {@link HighAvailabilityCoordinationService} — no mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 13</b></p>
 */
class HighAvailabilityFailoverDynamicTableTest {

    private InMemoryDynamicTableStateStore stateStore;
    private DynamicTableConfig config;
    private org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease sharedLease;
    private HighAvailabilityCoordinationService node1;
    private HighAvailabilityCoordinationService node2;
    private RefreshScheduler scheduler1;
    private RefreshScheduler scheduler2;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        config = DynamicTableConfig.defaults();
        sharedLease = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
    }

    @AfterEach
    void tearDown() {
        ResourceCloser.closeQuietly(scheduler1);
        ResourceCloser.closeQuietly(scheduler2);
        ResourceCloser.closeQuietly(node1);
        ResourceCloser.closeQuietly(node2);
    }

    @Test
    void failover_newLeaderRecoversRefreshScheduling() throws Exception {
        stateStore.saveState(activeState("dt1", "t1"));
        stateStore.saveState(activeState("dt2", "t1"));

        var exec1 = new TestExec();
        scheduler1 = buildScheduler(exec1);
        var recovery1 = buildRecovery(scheduler1);

        HighAvailabilityCoordinationService.CoordinationConfig fc =
                new HighAvailabilityCoordinationService.CoordinationConfig(200, 50);
        node1 = new HighAvailabilityCoordinationService(sharedLease, fc);
        node1.start("node-1");
        assertThat(node1.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

        recovery1.recover(n -> Map.of(), n -> Set.of());
        assertThat(scheduler1.isRegistered("dt1")).isTrue();
        assertThat(scheduler1.isRegistered("dt2")).isTrue();

        // Old leader stops
        scheduler1.stopAll();
        node1.close();
        Thread.sleep(100);

        // New leader takes over
        var exec2 = new TestExec();
        scheduler2 = buildScheduler(exec2);
        var recovery2 = buildRecovery(scheduler2);

        node2 = new HighAvailabilityCoordinationService(sharedLease, fc);
        node2.start("node-2");
        assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

        RecoveryResult res = recovery2.recover(n -> Map.of(), n -> Set.of());
        assertThat(res.recovered()).isEqualTo(2);
        assertThat(scheduler2.isRegistered("dt1")).isTrue();
        assertThat(scheduler2.isRegistered("dt2")).isTrue();
        assertThat(scheduler1.registeredCount()).isZero();
    }

    @Test
    void standbyDetectsLeaderFailureAndRecovers() throws Exception {
        stateStore.saveState(activeState("dt1", "t1"));

        HighAvailabilityCoordinationService.CoordinationConfig fc =
                new HighAvailabilityCoordinationService.CoordinationConfig(200, 50);

        node1 = new HighAvailabilityCoordinationService(sharedLease, fc);
        node1.start("node-1");
        assertThat(node1.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

        node2 = new HighAvailabilityCoordinationService(sharedLease, fc);
        node2.start("node-2");
        assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.STANDBY);

        node1.close();
        Thread.sleep(500);

        assertThat(node2.getLeaderId()).isEqualTo("node-2");
        assertThat(node2.getRole()).isEqualTo(HighAvailabilityCoordinationService.Role.LEADER);

        var exec = new TestExec();
        scheduler2 = buildScheduler(exec);
        RecoveryResult res = buildRecovery(scheduler2).recover(n -> Map.of(), n -> Set.of());
        assertThat(res.recovered()).isEqualTo(1);
        assertThat(scheduler2.isRegistered("dt1")).isTrue();
    }

    @Test
    void oldLeaderDemotion_stopAllReleasesResources() {
        scheduler1 = buildScheduler(new TestExec());
        scheduler1.register(activeState("dt1", "t1"));
        assertThat(scheduler1.isRegistered("dt1")).isTrue();

        scheduler1.stopAll();

        assertThat(scheduler1.registeredCount()).isZero();
        assertThat(scheduler1.globalActiveCount()).isZero();
    }

    @Test
    void failover_failingStatesRecoveredOnNewLeader() {
        stateStore.saveState(failingState("dt_failing", "t1", 3));
        stateStore.saveState(activeState("dt_active", "t1"));

        scheduler1 = buildScheduler(new TestExec());
        RecoveryResult res = buildRecovery(scheduler1).recover(n -> Map.of(), n -> Set.of());

        assertThat(res.recovered()).isEqualTo(2);
        DynamicTableState upd = stateStore.loadState("dt_failing").orElseThrow();
        assertThat(upd.state()).isEqualTo(DynamicTableLifecycleState.ACTIVE);
        assertThat(scheduler1.isRegistered("dt_failing")).isTrue();
        assertThat(scheduler1.isRegistered("dt_active")).isTrue();
    }

    @Test
    void failoverCallback_stopsOldAndRecoversNew() {
        stateStore.saveState(activeState("dt1", "t1"));

        scheduler1 = buildScheduler(new TestExec());
        scheduler1.register(activeState("dt1", "t1"));
        assertThat(scheduler1.isRegistered("dt1")).isTrue();

        scheduler2 = buildScheduler(new TestExec());
        var recovery = buildRecovery(scheduler2);

        // Simulate failover callback
        scheduler1.stopAll();
        RecoveryResult res = recovery.recover(n -> Map.of(), n -> Set.of());

        assertThat(scheduler1.registeredCount()).isZero();
        assertThat(res.recovered()).isEqualTo(1);
        assertThat(scheduler2.isRegistered("dt1")).isTrue();
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private RefreshScheduler buildScheduler(TestExec exec) {
        return new RefreshScheduler(new RefreshSchedulerContext(exec, stateStore, config));
    }

    private RefreshStateRecovery buildRecovery(RefreshScheduler s) {
        return new RefreshStateRecovery(
                stateStore, new DynamicTableDagManager(), s, new SchemaEvolutionChecker());
    }

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

    static final class TestExec implements RefreshExecutor {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            return RefreshResult.noOp();
        }

        int callCount() { return count.get(); }
    }
}
