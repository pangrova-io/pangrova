package org.pangrova.planner;

import org.pangrova.planner.mv.DynamicTableResolution;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-parse planning support for Dynamic Tables: SQL rewriting, merge-on-read
 * hint collection, and advisory CTE materialization analysis.
 */
final class DynamicTablePlanningSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTablePlanningSupport.class);

    private final PangrovaParserFactory parserFactory;
    private final DynamicTableSqlRewriter dynamicTableSqlRewriter;

    DynamicTablePlanningSupport(PangrovaParserFactory parserFactory,
                                DynamicTableSqlRewriter dynamicTableSqlRewriter) {
        this.parserFactory = parserFactory;
        this.dynamicTableSqlRewriter = dynamicTableSqlRewriter;
    }

    /**
     * Pre-parse rewrite: delegates to {@link DynamicTableSqlRewriter} when wired.
     */
    DynamicTableSqlRewriter.RewriteWithAuthority rewriteIfWired(String sql) {
        if (dynamicTableSqlRewriter == null) {
            return new DynamicTableSqlRewriter.RewriteWithAuthority(sql, java.util.Map.of());
        }
        return dynamicTableSqlRewriter.rewriteWithAuthority(sql);
    }

    /**
     * Collects merge-on-read hints by resolving DT references in the SQL.
     *
     * <p>When a DT has pending deltas (lastRefreshSnapshot &lt; currentSnapshot),
     * the resolution is {@link DynamicTableResolution.MergeOnRead}. These hints
     * are passed to the coordinator's split manager so it can generate incremental
     * splits for the delta leg of the UNION ALL plan.</p>
     *
     * @param sql the original SQL before DT rewrite
     * @return list of merge-on-read hints, empty if no DTs need merge-on-read
     */
    List<DynamicTableResolution.MergeOnRead> collectMergeOnReadHints(String sql) {
        if (dynamicTableSqlRewriter == null) {
            return List.of();
        }
        DynamicTableRewriter rewriter = dynamicTableSqlRewriter.getDynamicTableRewriter();
        if (rewriter == null) {
            return List.of();
        }
        if (!rewriter.usesRegistry()) {
            return List.of(); // durable full-refresh publications have no delta leg
        }
        List<String> tableNames = dynamicTableSqlRewriter.extractTableNames(sql);
        List<DynamicTableResolution.MergeOnRead> hints = new ArrayList<>();
        for (String tableName : tableNames) {
            DynamicTableResolution resolution = rewriter.resolve(tableName);
            if (resolution instanceof DynamicTableResolution.MergeOnRead mor) {
                hints.add(mor);
            }
        }
        return hints;
    }

    /**
     * Analyzes CTE queries for materialization opportunities.
     * When a CTE is referenced multiple times and the cost model indicates
     * materialization would be cheaper than re-computation, logs an INFO
     * message for observability and future optimization.
     */
    void logCteMaterializationHints(String sql) {
        try {
            org.pangrova.parser.ast.Statement ast = parserFactory.parseToAst(sql);
            if (!(ast instanceof org.pangrova.parser.ast.CteQuery cteQuery)) {
                return;
            }
            List<org.pangrova.planner.rule.CTEReference> refs =
                    org.pangrova.planner.rule.CteReferenceCounter.count(cteQuery);
            for (var ref : refs) {
                if (org.pangrova.planner.rule.CTEMaterializationRule.shouldMaterialize(
                        ref.cteCost(), ref.referenceCount())) {
                    LOG.info("event=cte_materialization_candidate cte={} references={} "
                                    + "inlineCost={} materializeCost={}",
                            ref.cteName(), ref.referenceCount(),
                            org.pangrova.planner.rule.CTEMaterializationRule.estimateInlineCost(
                                    ref.cteCost(), ref.referenceCount()),
                            org.pangrova.planner.rule.CTEMaterializationRule.estimateMaterializationCost(
                                    ref.cteCost(), -1));
                }
            }
        } catch (Exception e) {
            // F-PLAN-02: CTE materialization analysis is advisory, but a failure here means the
            // optimization hint is silently unavailable — surface it at WARN (with the exception) so
            // the regression is observable instead of hidden at DEBUG.
            LOG.warn("event=cte_analysis_failed reason={}", e.getMessage(), e);
        }
    }
}
