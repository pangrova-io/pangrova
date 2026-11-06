package org.pangrova.planner.core.mv;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites an optimized plan to read a published materialized view generation.
 *
 * <p>Calcite performs the algebraic substitution through
 * {@link org.apache.calcite.plan.RelOptMaterializations}. The adapter supplies
 * real {@link RelOptTable}s for published physical generations and compiles the
 * stored definition SQL with the same catalog. A candidate is used only when:
 * its definition is on the deterministic SPJG whitelist, its generation is
 * within the configured staleness bound, and the rewritten plan scans only the
 * original sources or the exact physical generation. Otherwise the original
 * plan is returned unchanged.
 */
public final class CalciteMaterializedViewRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(CalciteMaterializedViewRewriter.class);

    private final MaterializedViewRewriteCatalog catalog;
    private final BiFunction<RelOptCluster, String, RelNode> definitionCompiler;
    private final Function<List<String>, RelOptTable> tableResolver;
    private final Supplier<Instant> clock;

    /**
     * Creates a kernel-backed rewriter.
     *
     * @param catalog published view candidates with authorization applied
     * @param definitionCompiler compiles definition SQL in the query cluster without rewrite
     * @param tableResolver resolves a physical generation table from the query catalog
     * @param clock planning clock
     */
    public CalciteMaterializedViewRewriter(MaterializedViewRewriteCatalog catalog,
            BiFunction<RelOptCluster, String, RelNode> definitionCompiler,
            Function<List<String>, RelOptTable> tableResolver,
            Supplier<Instant> clock) {
        this.catalog = java.util.Objects.requireNonNull(catalog, "catalog");
        this.definitionCompiler = java.util.Objects.requireNonNull(definitionCompiler,
                "definitionCompiler");
        this.tableResolver = java.util.Objects.requireNonNull(tableResolver, "tableResolver");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    /**
     * Returns a plan that reads a published generation when a safe substitution exists.
     *
     * @param original optimized plan
     * @return rewritten plan, or the original plan when no candidate applies
     */
    public RelNode rewrite(RelNode original) {
        return rewriteWithDecision(original).plan();
    }

    /**
     * Returns the rewrite result together with a diagnostic decision.
     *
     * <p>The decision explains a skip for {@code EXPLAIN}; it never weakens a
     * guard. Every rejected candidate contributes its reason, and the first
     * substitution that passes both post-checks wins.
     *
     * @param original optimized plan
     * @return rewritten plan and decision
     */
    public RewriteResult rewriteWithDecision(RelNode original) {
        Set<String> referenced = RelOptUtil.findTables(original).stream()
                .map(table -> String.join(".", table.getQualifiedName()).toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        List<MaterializedViewRewriteCatalog.PublishedView> candidates =
                catalog.candidates(Set.copyOf(referenced));
        if (candidates.isEmpty()) {
            return new RewriteResult(original,
                    MaterializedViewRewriteDecision.skipped("no_published_candidate"));
        }
        List<RelOptMaterialization> materializations = new ArrayList<>();
        Set<String> physicalTables = new LinkedHashSet<>();
        List<String> skipReasons = new ArrayList<>();
        for (MaterializedViewRewriteCatalog.PublishedView view : candidates) {
            materialization(original, view, skipReasons).ifPresent(materialization -> {
                materializations.add(materialization);
                physicalTables.add((view.physicalCatalog() + "." + view.physicalSchema()
                        + "." + view.physicalTable()).toLowerCase(Locale.ROOT));
            });
        }
        if (materializations.isEmpty()) {
            return new RewriteResult(original, MaterializedViewRewriteDecision.skipped(
                    skipReasons.isEmpty() ? "no_eligible_candidate" : skipReasons.get(0)));
        }
        List<Pair<RelNode, List<RelOptMaterialization>>> rewrites =
                org.apache.calcite.plan.RelOptMaterializations.useMaterializedViews(
                        original, materializations);
        for (Pair<RelNode, List<RelOptMaterialization>> rewrite : rewrites) {
            RelNode candidate = rewrite.left;
            if (!sameOutputShape(original, candidate)
                    || !scansOnlyAllowed(candidate, original, physicalTables)) {
                LOG.warn("event=materialized_view_rewrite_skipped reason=post_check_failed");
                continue;
            }
            List<String> used = rewrite.right.stream()
                    .map(materialization -> String.join(".", materialization.qualifiedTableName))
                    .toList();
            LOG.info("event=materialized_view_rewrite_applied views={}", used);
            return new RewriteResult(candidate, MaterializedViewRewriteDecision.applied(used));
        }
        return new RewriteResult(original,
                MaterializedViewRewriteDecision.skipped("no_equivalent_substitution"));
    }

    private java.util.Optional<RelOptMaterialization> materialization(RelNode query,
            MaterializedViewRewriteCatalog.PublishedView view, List<String> skipReasons) {
        Instant now = clock.get();
        if (view.publishedAt().plus(view.maxStaleness()).isBefore(now)) {
            LOG.info("event=materialized_view_rewrite_skipped view={} reason=stale publishedAt={}",
                    view.qualifiedName(), view.publishedAt());
            skipReasons.add("stale");
            return java.util.Optional.empty();
        }
        RelNode definition;
        try {
            definition = definitionCompiler.apply(query.getCluster(), view.definitionSql());
        } catch (RuntimeException failure) {
            LOG.info("event=materialized_view_rewrite_skipped view={} reason=definition_unplannable error={}",
                    view.qualifiedName(), failure.getMessage());
            skipReasons.add("definition_unplannable");
            return java.util.Optional.empty();
        }
        java.util.Optional<String> ineligible = MaterializedViewPlanEligibility
                .ineligibleReason(definition);
        if (ineligible.isPresent()) {
            LOG.info("event=materialized_view_rewrite_skipped view={} reason={}",
                    view.qualifiedName(), ineligible.get());
            skipReasons.add("ineligible");
            return java.util.Optional.empty();
        }
        RelOptTable table = tableResolver.apply(List.of(view.physicalCatalog(),
                view.physicalSchema(), view.physicalTable()));
        if (table == null) {
            LOG.warn("event=materialized_view_rewrite_skipped view={} reason=physical_table_unresolved",
                    view.qualifiedName());
            skipReasons.add("physical_table_unresolved");
            return java.util.Optional.empty();
        }
        RelNode tableRel = LogicalTableScan.create(query.getCluster(), table, List.of());
        return java.util.Optional.of(new RelOptMaterialization(tableRel, definition, null,
                List.of(view.qualifiedName())));
    }

    /**
     * Result of one rewrite attempt.
     *
     * @param plan resulting plan; the original plan when not applied
     * @param decision whether and why a substitution happened
     */
    public record RewriteResult(RelNode plan, MaterializedViewRewriteDecision decision) {
        public RewriteResult {
            if (plan == null || decision == null) {
                throw new IllegalArgumentException("plan and decision must not be null");
            }
        }
    }

    private static boolean sameOutputShape(RelNode original, RelNode candidate) {
        return original.getRowType().getFieldCount() == candidate.getRowType().getFieldCount()
                && RelOptUtil.areRowTypesEqual(original.getRowType(), candidate.getRowType(), false);
    }

    private static boolean scansOnlyAllowed(RelNode candidate, RelNode original,
            Set<String> physicalTables) {
        Set<String> allowed = RelOptUtil.findTables(original).stream()
                .map(table -> String.join(".", table.getQualifiedName()).toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        allowed.addAll(physicalTables);
        return RelOptUtil.findTables(candidate).stream()
                .map(table -> String.join(".", table.getQualifiedName()).toLowerCase(Locale.ROOT))
                .allMatch(allowed::contains);
    }
}
