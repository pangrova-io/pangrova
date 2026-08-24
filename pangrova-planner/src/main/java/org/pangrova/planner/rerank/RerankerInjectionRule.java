package org.pangrova.planner.rerank;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.SortItem;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.parser.ast.expression.RerankClause;
import org.pangrova.parser.ast.expression.RerankClause.CascadeStrategy;
import org.pangrova.parser.ast.expression.RerankClause.CrossEncoderStrategy;
import org.pangrova.parser.ast.expression.RerankClause.LinearCombinationStrategy;
import org.pangrova.parser.ast.expression.RerankClause.RerankStrategy;
import org.pangrova.parser.ast.expression.RerankClause.RrfStrategy;
import org.pangrova.planner.rerank.RerankerPlanNode.FullTextSource;
import org.pangrova.planner.rerank.RerankerPlanNode.RetrievalSource;
import org.pangrova.planner.rerank.RerankerPlanNode.VectorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Planner rule that injects a {@link RerankerPlanNode} into the query plan when
 * reranking is needed.
 *
 * <p>Injection triggers:
 * <ul>
 *   <li>Explicit RERANK BY clause in the query</li>
 *   <li>Implicit hybrid search: vector distance ORDER BY combined with MATCH() in WHERE,
 *       without explicit RERANK BY — defaults to RRF(k=60)</li>
 * </ul>
 *
 * <p>The reranker node is positioned below projections so that scoring columns
 * (distance, BM25) remain available during reranking even if the final SELECT
 * does not include them.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class RerankerInjectionRule {

    private static final Logger LOG = LoggerFactory.getLogger(RerankerInjectionRule.class);

    private static final int DEFAULT_RRF_K = 60;

    private static final Set<String> DISTANCE_FUNCTIONS = Set.of(
            "COSINE_DISTANCE", "L2_DISTANCE", "INNER_PRODUCT");

    private RerankerInjectionRule() {}

    /**
     * Analyzes a parsed query and produces a {@link RerankerPlanNode} if reranking
     * is applicable.
     *
     * <p>Returns empty if no RERANK BY clause and no hybrid search pattern detected.</p>
     *
     * @param query     the parsed SELECT query
     * @param tableName fully qualified table name for FTS scoring context
     * @param topK      the effective LIMIT value (must be positive)
     * @return the reranker plan node, or empty if reranking is not applicable
     * @throws RerankerValidationException if RERANK BY is used with GROUP BY/HAVING
     */
    public static Optional<RerankerPlanNode> analyze(Query query, String tableName, int topK) {
        if (topK <= 0) {
            return Optional.empty();
        }

        boolean hasGroupBy = !query.groupBy().isEmpty();
        boolean hasHaving = query.having().isPresent();

        if (query.rerankBy().isPresent() && (hasGroupBy || hasHaving)) {
            throw new RerankerValidationException(
                    "RERANK BY is incompatible with GROUP BY/HAVING");
        }

        if (query.rerankBy().isPresent()) {
            return Optional.of(buildFromExplicitClause(
                    query, query.rerankBy().get(), tableName, topK));
        }

        return detectImplicitHybrid(query, tableName, topK);
    }

    private static RerankerPlanNode buildFromExplicitClause(
            Query query, RerankClause clause, String tableName, int topK) {

        RerankerPlanNode.RerankerStrategy strategy = convertStrategy(clause.strategy());
        List<String> targetColumns = clause.onColumns().stream()
                .map(RerankerInjectionRule::identifierToString)
                .toList();
        List<RetrievalSource> sources = collectSources(query, tableName, targetColumns);
        String queryText = extractQueryText(query);

        if (sources.isEmpty()) {
            throw new RerankerValidationException(
                    "RERANK BY requires at least one retrieval source "
                            + "(vector ORDER BY or MATCH() in WHERE)");
        }

        LOG.debug("event=reranker_injection_explicit strategy={} sources={} topK={} table={}",
                strategy.getClass().getSimpleName(), sources.size(), topK, tableName);

        return new RerankerPlanNode(strategy, sources, topK, queryText, targetColumns);
    }

    private static Optional<RerankerPlanNode> detectImplicitHybrid(
            Query query, String tableName, int topK) {

        boolean hasVectorOrderBy = hasVectorDistanceOrderBy(query);
        boolean hasMatchInWhere = hasMatchFunction(query);

        if (!hasVectorOrderBy || !hasMatchInWhere) {
            return Optional.empty();
        }

        List<RetrievalSource> sources = collectSources(query, tableName, List.of());
        if (sources.size() < 2) {
            return Optional.empty();
        }

        String queryText = extractQueryText(query);
        var strategy = new RerankerPlanNode.RrfStrategy(DEFAULT_RRF_K);

        LOG.debug("event=reranker_injection_implicit strategy=RRF k={} sources={} topK={} table={}",
                DEFAULT_RRF_K, sources.size(), topK, tableName);

        return Optional.of(new RerankerPlanNode(strategy, sources, topK, queryText, List.of()));
    }

    /**
     * Collects all retrieval sources from the query: vector sources from ORDER BY,
     * multi-vector sources from ON columns, and FTS source from WHERE MATCH().
     */
    private static List<RetrievalSource> collectSources(
            Query query, String tableName, List<String> targetColumns) {

        List<RetrievalSource> sources = new ArrayList<>();

        // Vector sources from ORDER BY distance functions
        for (SortItem item : query.orderBy()) {
            extractVectorSource(item.expression()).ifPresent(sources::add);
        }

        // Multi-vector ON columns not already covered by ORDER BY
        for (String col : targetColumns) {
            boolean alreadyCovered = sources.stream()
                    .anyMatch(s -> s instanceof VectorSource vs && vs.column().equals(col));
            if (!alreadyCovered) {
                sources.add(new VectorSource(col, new float[0], "cosine_distance"));
            }
        }

        // FTS source from WHERE MATCH()
        extractFullTextSource(query, tableName).ifPresent(sources::add);

        return sources;
    }

    private static Optional<VectorSource> extractVectorSource(Expression expr) {
        if (!(expr instanceof FunctionCall fn)) {
            return Optional.empty();
        }
        String funcName = identifierToString(fn.name()).toUpperCase();
        if (!DISTANCE_FUNCTIONS.contains(funcName)) {
            return Optional.empty();
        }
        if (fn.arguments().size() != 2) {
            return Optional.empty();
        }

        String column = extractColumnIdentifier(fn.arguments().get(0));
        if (column == null) {
            // Try reversed argument order: distance_func(literal, column)
            column = extractColumnIdentifier(fn.arguments().get(1));
        }
        if (column == null) {
            return Optional.empty();
        }

        // Query vector is resolved at Calcite/execution level (auto-embed for text literals).
        // At AST level we capture column and metric for plan node construction.
        return Optional.of(new VectorSource(column, new float[0], funcName.toLowerCase()));
    }

    private static Optional<FullTextSource> extractFullTextSource(Query query, String tableName) {
        if (query.where().isEmpty()) {
            return Optional.empty();
        }
        return MatchFunctionRewriter.extractMatchCall(query.where().get())
                .map(details -> new FullTextSource(
                        MatchFunctionRewriter.toScoringNode(tableName, details, List.of())));
    }

    private static boolean hasVectorDistanceOrderBy(Query query) {
        return query.orderBy().stream()
                .anyMatch(item -> isDistanceFunction(item.expression()));
    }

    private static boolean hasMatchFunction(Query query) {
        return query.where()
                .map(RerankerInjectionRule::containsMatchFunction)
                .orElse(false);
    }

    private static boolean containsMatchFunction(Expression expr) {
        if (MatchFunctionRewriter.isMatchFunction(expr)) {
            return true;
        }
        if (expr instanceof BinaryExpression bin) {
            return containsMatchFunction(bin.left()) || containsMatchFunction(bin.right());
        }
        if (expr instanceof NotExpression not) {
            return containsMatchFunction(not.expression());
        }
        return false;
    }

    private static boolean isDistanceFunction(Expression expr) {
        if (!(expr instanceof FunctionCall fn)) {
            return false;
        }
        String name = identifierToString(fn.name()).toUpperCase();
        return DISTANCE_FUNCTIONS.contains(name);
    }

    static RerankerPlanNode.RerankerStrategy convertStrategy(RerankStrategy strategy) {
        if (strategy instanceof RrfStrategy rrf) {
            return new RerankerPlanNode.RrfStrategy(extractInt(rrf.k()));
        } else if (strategy instanceof LinearCombinationStrategy lc) {
            return new RerankerPlanNode.LinearCombinationStrategy(
                    extractDouble(lc.weight1()), extractDouble(lc.weight2()));
        } else if (strategy instanceof CrossEncoderStrategy ce) {
            return new RerankerPlanNode.CrossEncoderStrategy(
                    ce.modelName(), extractInt(ce.topN()));
        } else if (strategy instanceof CascadeStrategy cascade) {
            return new RerankerPlanNode.CascadeStrategy(
                    cascade.stages().stream()
                            .map(RerankerInjectionRule::convertStrategy)
                            .toList());
        }
        throw new RerankerValidationException("Unknown rerank strategy type: " + strategy.getClass().getName());
    }

    private static String extractQueryText(Query query) {
        if (query.where().isEmpty()) {
            return null;
        }
        return MatchFunctionRewriter.extractMatchCall(query.where().get())
                .map(MatchFunctionRewriter.MatchCallDetails::queryText)
                .orElse(null);
    }

    private static String extractColumnIdentifier(Expression expr) {
        if (expr instanceof Identifier id) {
            List<String> parts = id.parts();
            return parts.isEmpty() ? null : String.join(".", parts);
        }
        return null;
    }

    private static int extractInt(Expression expr) {
        if (expr instanceof Literal lit) {
            if (lit.value() instanceof LiteralValue.IntegerValue iv) {
                return (int) iv.value();
            }
            if (lit.value() instanceof LiteralValue.DecimalValue dv) {
                return dv.value().intValue();
            }
        }
        throw new RerankerValidationException(
                "Expected integer literal in RERANK BY parameter, got: " + expr);
    }

    private static double extractDouble(Expression expr) {
        if (expr instanceof Literal lit) {
            if (lit.value() instanceof LiteralValue.DecimalValue dv) {
                return dv.value().doubleValue();
            }
            if (lit.value() instanceof LiteralValue.IntegerValue iv) {
                return (double) iv.value();
            }
        }
        throw new RerankerValidationException(
                "Expected numeric literal in RERANK BY parameter, got: " + expr);
    }

    private static String identifierToString(Identifier id) {
        return String.join(".", id.parts());
    }
}
