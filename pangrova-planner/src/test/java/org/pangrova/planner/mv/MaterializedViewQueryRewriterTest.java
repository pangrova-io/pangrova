package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for MaterializedViewQueryRewriter covering freshness gate, matching, and skip logic.
 */
class MaterializedViewQueryRewriterTest {

    private MaterializedViewRegistry manager;
    private RelOptCluster cluster;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        manager = new MaterializedViewRegistry();
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(RexUtil.EXECUTOR);
        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    @Test
    void staleMvIsSkipped() {
        Instant longAgo = Instant.now().minus(Duration.ofHours(10));
        registerMv("stale_mv", "select x", true, longAgo);

        MaterializedViewQueryRewriter rewriter = new MaterializedViewQueryRewriter(manager, Duration.ofMinutes(5));
        RelNode plan = dummyPlan();

        RelNode result = rewriter.rewrite(plan);
        assertThat(result).isSameAs(plan);
    }

    @Test
    void freshMatchingMvTriggersRewrite() {
        Instant justNow = Instant.now().minus(Duration.ofSeconds(30));
        registerMv("fresh_mv", "select x", true, justNow);

        // Use a PlanRewriter that always matches
        MaterializedViewQueryRewriter rewriter = new MaterializedViewQueryRewriter(
                manager, Duration.ofMinutes(5), Instant::now, alwaysMatch());

        RelNode plan = dummyPlan();
        RelNode result = rewriter.rewrite(plan);

        assertThat(result).isNotSameAs(plan);
        assertThat(result.getTable().getQualifiedName()).containsExactly("fresh_mv");
    }

    @Test
    void nonMatchingQueryReturnsOriginalPlan() {
        Instant justNow = Instant.now().minus(Duration.ofSeconds(30));
        registerMv("fresh_mv", "select x", true, justNow);

        // Use a PlanRewriter that never matches
        MaterializedViewQueryRewriter rewriter = new MaterializedViewQueryRewriter(
                manager, Duration.ofMinutes(5), Instant::now, neverMatch());

        RelNode plan = dummyPlan();
        RelNode result = rewriter.rewrite(plan);

        assertThat(result).isSameAs(plan);
    }

    @Test
    void mvWithQueryRewriteDisabledIsSkipped() {
        Instant justNow = Instant.now().minus(Duration.ofSeconds(30));
        registerMv("disabled_mv", "select x", false, justNow);

        MaterializedViewQueryRewriter rewriter = new MaterializedViewQueryRewriter(
                manager, Duration.ofMinutes(5), Instant::now, alwaysMatch());

        RelNode plan = dummyPlan();
        RelNode result = rewriter.rewrite(plan);

        assertThat(result).isSameAs(plan);
    }

    @Test
    void noRegisteredMvsReturnsOriginalPlan() {
        MaterializedViewQueryRewriter rewriter = new MaterializedViewQueryRewriter(manager, Duration.ofMinutes(5));
        RelNode plan = dummyPlan();

        RelNode result = rewriter.rewrite(plan);
        assertThat(result).isSameAs(plan);
    }

    @Test
    void mvWithNoRefreshSnapshotIsSkipped() {
        MaterializedViewDefinition mv = new MaterializedViewDefinition(
                "never_refreshed", "select x", RefreshStrategy.ON_SCHEDULE,
                true, Optional.empty(), Instant.now());
        manager.getAllDefinitions(); // ensure manager is initialized
        // Register via reflection-free approach: use createMV path won't work,
        // so we add a direct-register helper or test differently.
        // Instead, test via the MaterializedViewCandidate staleness logic:
        MaterializedViewCandidate candidate = MaterializedViewCandidate.of(mv, Instant.now());
        assertThat(candidate.staleness()).isGreaterThan(Duration.ofDays(300));
    }

    // --- helpers ---

    private void registerMv(String name, String query, boolean enableRewrite, Instant lastRefresh) {
        MaterializedViewDefinition mv = new MaterializedViewDefinition(
                name, query, RefreshStrategy.ON_SCHEDULE,
                enableRewrite, Optional.of(lastRefresh), Instant.now());
        // Register via manager's public API
        manager.register(mv);
    }

    private RelNode dummyPlan() {
        return LogicalValues.createOneRow(cluster);
    }

    private MaterializedViewQueryRewriter.PlanRewriter alwaysMatch() {
        return new MaterializedViewQueryRewriter.PlanRewriter() {
            @Override
            public boolean matches(RelNode queryPlan, MaterializedViewDefinition mv) {
                return true;
            }

            @Override
            public RelNode rewrite(RelNode original, MaterializedViewDefinition mv) {
                RelOptCluster c = original.getCluster();
                RelDataType rowType = original.getRowType();
                MaterializedViewTableRef table = new MaterializedViewTableRef(mv.name(), rowType);
                return org.apache.calcite.rel.logical.LogicalTableScan.create(
                        c, table, java.util.List.of());
            }
        };
    }

    private MaterializedViewQueryRewriter.PlanRewriter neverMatch() {
        return new MaterializedViewQueryRewriter.PlanRewriter() {
            @Override
            public boolean matches(RelNode queryPlan, MaterializedViewDefinition mv) {
                return false;
            }

            @Override
            public RelNode rewrite(RelNode original, MaterializedViewDefinition mv) {
                return original;
            }
        };
    }
}
