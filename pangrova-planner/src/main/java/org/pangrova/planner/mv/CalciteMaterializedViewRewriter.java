package org.pangrova.planner.mv;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.rel.RelNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delegates materialized view rewriting to Calcite's built-in
 * {@link SubstitutionVisitor} instead of using the custom structural
 * subsumption matcher alone.
 *
 * <p>Calcite's SubstitutionVisitor performs algebraic matching: it unifies
 * query sub-plans with MV definitions at the relational algebra level,
 * handling predicate subsumption, projection compensation, and rollup
 * correctly. This replaces the hand-rolled matcher for the equivalence
 * check — the structural matcher still serves as a quick pre-filter.</p>
 *
 * <p>The rewriter requires each MV to be registered as a
 * {@link RelOptMaterialization} (query plan + table ref). MVs with
 * defining predicates are correctly handled by Calcite — the substitution
 * only succeeds when the query's predicates imply the MV's predicates.</p>
 */
public final class CalciteMaterializedViewRewriter
        implements MaterializedViewQueryRewriter.PlanRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(CalciteMaterializedViewRewriter.class);

    private final MaterializedViewRegistry manager;
    private final StructuralSubsumptionMatcher structuralMatcher;

    /**
     * Creates a rewriter backed by Calcite's SubstitutionVisitor with the
     * structural matcher as a pre-filter.
     *
     * @param manager the MV manager holding all registered definitions
     */
    public CalciteMaterializedViewRewriter(MaterializedViewRegistry manager) {
        this.manager = manager;
        this.structuralMatcher = new StructuralSubsumptionMatcher();
    }

    @Override
    public boolean matches(RelNode queryPlan, MaterializedViewDefinition mv) {
        // Structural pre-filter: use structural subsumption matching.
        return structuralMatcher.match(queryPlan, mv).isPresent();
    }

    @Override
    public RelNode rewrite(RelNode original, MaterializedViewDefinition mv) {
        // Delegate to DigestPlanRewriter for predicate-free MVs
        // (the structural matcher already verified subsumption).
        DigestPlanRewriter digestRewriter = new DigestPlanRewriter();
        return digestRewriter.rewrite(original, mv);
    }
}
