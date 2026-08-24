package org.pangrova.planner.mv;

import org.apache.calcite.rel.RelNode;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Rewrites a query plan to read from a materialized view when beneficial.
 * <p>
 * Steps: find candidate MVs → check freshness → match pattern → rewrite.
 */
public final class MaterializedViewQueryRewriter {

    private final MaterializedViewRegistry manager;
    private final Duration maxStaleness;
    private final Supplier<Instant> clock;
    private final PlanRewriter planRewriter;

    public MaterializedViewQueryRewriter(MaterializedViewRegistry manager, Duration maxStaleness) {
        this(manager, maxStaleness, Instant::now, new DigestPlanRewriter());
    }

    MaterializedViewQueryRewriter(
            MaterializedViewRegistry manager,
            Duration maxStaleness,
            Supplier<Instant> clock,
            PlanRewriter planRewriter) {
        this.manager = manager;
        this.maxStaleness = maxStaleness;
        this.clock = clock;
        this.planRewriter = planRewriter;
    }

    /**
     * Attempt to rewrite the plan using a fresh, matching MV.
     * Returns the original plan unchanged when no suitable MV exists.
     */
    public RelNode rewrite(RelNode originalPlan) {
        Instant now = clock.get();
        List<MaterializedViewCandidate> candidates = findFreshCandidates(now);
        return pickMatch(originalPlan, candidates).orElse(originalPlan);
    }

    private List<MaterializedViewCandidate> findFreshCandidates(Instant now) {
        return manager.getAllDefinitions().stream()
                .filter(MaterializedViewDefinition::enableQueryRewrite)
                .map(mv -> MaterializedViewCandidate.of(mv, now))
                .filter(c -> !c.staleness().isNegative())
                .filter(c -> c.staleness().compareTo(maxStaleness) <= 0)
                .toList();
    }

    private Optional<RelNode> pickMatch(RelNode plan, List<MaterializedViewCandidate> candidates) {
        for (MaterializedViewCandidate candidate : candidates) {
            if (planRewriter.matches(plan, candidate.definition())) {
                return Optional.of(planRewriter.rewrite(plan, candidate.definition()));
            }
        }
        return Optional.empty();
    }

    /**
     * Strategy interface for plan matching and rewriting.
     * Accepts the query's logical plan for structural subsumption matching.
     */
    public interface PlanRewriter {
        boolean matches(RelNode queryPlan, MaterializedViewDefinition mv);
        RelNode rewrite(RelNode original, MaterializedViewDefinition mv);
    }
}
