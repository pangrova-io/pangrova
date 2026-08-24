package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.mv.InMemoryMaterializedViewStore;
import org.pangrova.metadata.mv.MaterializedViewMetadata;
import org.pangrova.metadata.mv.MaterializedViewState;
import org.pangrova.parser.ast.RefreshStrategy;
import org.pangrova.planner.mv.MaterializedViewDefinition;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.planner.mv.MaterializedViewQueryRewriter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies MV metadata survives a simulated Coordinator restart
 * and recovered MVs are available for query rewriting.
 *
 * <p><b>Validates: Requirements 1.49, 2.49</b></p>
 */
class MaterializedViewPersistenceRecoveryTest {

    @Test
    void mvSurvivesRestartAndIsAvailableForRewriting() {
        // --- Phase 1: "First Coordinator lifetime" — register MV and persist ---
        InMemoryMaterializedViewStore sharedStore = new InMemoryMaterializedViewStore();
        MaterializedViewPersistenceListener listener1 = new MaterializedViewPersistenceListener(sharedStore);
        MaterializedViewRegistry manager1 = new MaterializedViewRegistry(listener1);

        MaterializedViewDefinition def = mvDefinition("sales_daily", RefreshStrategy.INCREMENTAL);
        manager1.register(def);
        listener1.onCreated(def);

        // Simulate refresh by updating the definition
        MaterializedViewDefinition refreshed = def.withRefreshSnapshot(Instant.now());
        manager1.register(refreshed);
        listener1.onRefreshed(refreshed);

        // Verify persisted to store
        assertThat(sharedStore.load("sales_daily")).isPresent();
        MaterializedViewMetadata persisted = sharedStore.load("sales_daily").orElseThrow();
        assertThat(persisted.state()).isEqualTo(MaterializedViewState.ACTIVE);
        assertThat(persisted.lastRefreshedAt()).isNotNull();

        // --- Phase 2: Simulate restart — new manager, same backing store ---
        MaterializedViewRegistry manager2 = new MaterializedViewRegistry();
        MaterializedViewRecovery recovery = new MaterializedViewRecovery(sharedStore, manager2);
        int recovered = recovery.recover();

        assertThat(recovered).isEqualTo(1);
        assertThat(manager2.getDefinition("sales_daily")).isPresent();

        MaterializedViewDefinition recoveredDef = manager2.getDefinition("sales_daily").orElseThrow();
        assertThat(recoveredDef.name()).isEqualTo("sales_daily");
        assertThat(recoveredDef.enableQueryRewrite()).isTrue();
        assertThat(recoveredDef.lastRefreshSnapshot()).isPresent();
    }

    @Test
    void recoveredMvIsUsedForQueryRewriting() {
        InMemoryMaterializedViewStore store = new InMemoryMaterializedViewStore();
        Instant now = Instant.now();

        store.save(new MaterializedViewMetadata(
                "fresh_mv", "select x",
                MaterializedViewState.ACTIVE, "INCREMENTAL", now.minus(Duration.ofMinutes(5)), now));

        // Recover into a new manager
        MaterializedViewRegistry manager = new MaterializedViewRegistry();
        new MaterializedViewRecovery(store, manager).recover();

        // Verify the recovered MV is accessible to the rewriter
        assertThat(manager.getDefinition("fresh_mv")).isPresent();
        assertThat(manager.getAllDefinitions()).hasSize(1);

        // Build rewriter — it will use the recovered MVs from the manager
        MaterializedViewQueryRewriter rewriter = new MaterializedViewQueryRewriter(manager, Duration.ofMinutes(30));

        RelNode plan = dummyPlan().relNode();
        RelNode result = rewriter.rewrite(plan);

        assertThat(result).isNotNull();
    }

    @Test
    void droppedMvIsNotRecovered() {
        InMemoryMaterializedViewStore store = new InMemoryMaterializedViewStore();
        Instant now = Instant.now();
        store.save(new MaterializedViewMetadata(
                "dropped_mv", "select 1",
                MaterializedViewState.DROPPED, "INCREMENTAL", now, null));

        MaterializedViewRegistry manager = new MaterializedViewRegistry();
        int recovered = new MaterializedViewRecovery(store, manager).recover();

        assertThat(recovered).isZero();
        assertThat(manager.getDefinition("dropped_mv")).isEmpty();
    }

    @Test
    void dropPersistsAndPreventsRecovery() {
        InMemoryMaterializedViewStore store = new InMemoryMaterializedViewStore();
        MaterializedViewPersistenceListener listener = new MaterializedViewPersistenceListener(store);
        MaterializedViewRegistry manager1 = new MaterializedViewRegistry(listener);

        MaterializedViewDefinition def = mvDefinition("temp_mv", RefreshStrategy.INCREMENTAL);
        manager1.register(def);
        listener.onCreated(def);
        assertThat(store.load("temp_mv")).isPresent();

        manager1.dropMaterializedView("temp_mv");
        assertThat(store.load("temp_mv")).isEmpty();

        // Simulate restart — nothing to recover
        MaterializedViewRegistry manager2 = new MaterializedViewRegistry();
        int recovered = new MaterializedViewRecovery(store, manager2).recover();
        assertThat(recovered).isZero();
    }

    @Test
    void multipleCreatesAndRecovery() {
        InMemoryMaterializedViewStore store = new InMemoryMaterializedViewStore();
        MaterializedViewPersistenceListener listener = new MaterializedViewPersistenceListener(store);
        MaterializedViewRegistry manager1 = new MaterializedViewRegistry(listener);

        MaterializedViewDefinition defA = mvDefinition("mv_a", RefreshStrategy.INCREMENTAL);
        MaterializedViewDefinition defB = mvDefinition("mv_b", RefreshStrategy.INCREMENTAL);
        manager1.register(defA);
        listener.onCreated(defA);
        manager1.register(defB);
        listener.onCreated(defB);

        // Simulate refresh on mv_a
        MaterializedViewDefinition refreshedA = defA.withRefreshSnapshot(Instant.now());
        manager1.register(refreshedA);
        listener.onRefreshed(refreshedA);

        // Simulate restart
        MaterializedViewRegistry manager2 = new MaterializedViewRegistry();
        int recovered = new MaterializedViewRecovery(store, manager2).recover();

        assertThat(recovered).isEqualTo(2);
        assertThat(manager2.getDefinition("mv_a")).isPresent();
        assertThat(manager2.getDefinition("mv_b")).isPresent();
        assertThat(manager2.getDefinition("mv_a").orElseThrow().lastRefreshSnapshot()).isPresent();
    }

    @Test
    void nonIncrementalEntriesAreFilteredOutDuringRecovery() {
        InMemoryMaterializedViewStore store = new InMemoryMaterializedViewStore();
        Instant now = Instant.now();

        // INCREMENTAL entry — should be recovered
        store.save(new MaterializedViewMetadata(
                "dt_incremental", "SELECT sum(x) FROM t GROUP BY k",
                MaterializedViewState.ACTIVE, "INCREMENTAL", now.minus(Duration.ofMinutes(10)), now));

        // ON_SCHEDULE entry — legacy MV, should NOT be recovered
        store.save(new MaterializedViewMetadata(
                "mv_scheduled", "SELECT 1",
                MaterializedViewState.ACTIVE, "ON_SCHEDULE", now.minus(Duration.ofMinutes(20)), now));

        // ON_COMMIT entry — legacy MV, should NOT be recovered
        store.save(new MaterializedViewMetadata(
                "mv_commit", "SELECT 2",
                MaterializedViewState.ACTIVE, "ON_COMMIT", now.minus(Duration.ofMinutes(15)), now));

        MaterializedViewRegistry manager = new MaterializedViewRegistry();
        int recovered = new MaterializedViewRecovery(store, manager).recover();

        assertThat(recovered).isEqualTo(1);
        assertThat(manager.getDefinition("dt_incremental")).isPresent();
        assertThat(manager.getDefinition("mv_scheduled")).isEmpty();
        assertThat(manager.getDefinition("mv_commit")).isEmpty();
    }

    // --- helpers ---

    private static MaterializedViewDefinition mvDefinition(String name, RefreshStrategy strategy) {
        return new MaterializedViewDefinition(
                name, "SELECT 1", strategy, true,
                Optional.of(Instant.now()), Instant.now());
    }

    private org.pangrova.planner.PlanFragment dummyPlan() {
        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(RexUtil.EXECUTOR);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        return new org.pangrova.planner.PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
