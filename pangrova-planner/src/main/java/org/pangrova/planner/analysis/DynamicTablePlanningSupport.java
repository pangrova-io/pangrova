package org.pangrova.planner.analysis;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableAstRewriter;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Pre-parse planning support for Dynamic Tables: SQL rewriting and advisory
 * CTE materialization analysis.
 */
final class DynamicTablePlanningSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTablePlanningSupport.class);

    private final PangrovaParserFactory parserFactory;
    private final DynamicTableSqlRewriter dynamicTableSqlRewriter;
    private final DynamicTableAstRewriter dynamicTableAstRewriter;

    DynamicTablePlanningSupport(PangrovaParserFactory parserFactory,
                                DynamicTableSqlRewriter dynamicTableSqlRewriter,
                                DynamicTableAstRewriter dynamicTableAstRewriter) {
        this.parserFactory = parserFactory;
        this.dynamicTableSqlRewriter = dynamicTableSqlRewriter;
        this.dynamicTableAstRewriter = dynamicTableAstRewriter;
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

    /** AST rewrite for statements dispatched without a SQL text round-trip. */
    DynamicTableAstRewriter.RewriteWithAuthority rewriteAstIfWired(Statement statement) {
        if (dynamicTableAstRewriter == null) {
            return new DynamicTableAstRewriter.RewriteWithAuthority(statement, java.util.Map.of());
        }
        return dynamicTableAstRewriter.rewrite(statement);
    }

    /**
     * Analyzes CTE queries for materialization opportunities.
     * When a CTE is referenced multiple times and the cost model indicates
     * materialization would be cheaper than re-computation, logs an INFO
     * message for observability and future optimization.
     */
    void logCteMaterializationHints(String sql) {
        try {
            org.pangrova.parser.api.ast.Statement ast = parserFactory.parseToAst(sql);
            if (!(ast instanceof org.pangrova.parser.api.ast.CteQuery cteQuery)) {
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
