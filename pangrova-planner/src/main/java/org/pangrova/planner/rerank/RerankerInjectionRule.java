package org.pangrova.planner.rerank;

import org.pangrova.planner.exception.RerankerValidationException;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.SortItem;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.expression.FunctionCall;
import org.pangrova.parser.api.ast.RerankClause;
import org.pangrova.parser.api.ast.RerankClause.CascadeStrategy;
import org.pangrova.parser.api.ast.RerankClause.CrossEncoderStrategy;
import org.pangrova.parser.api.ast.RerankClause.LinearCombinationStrategy;
import org.pangrova.parser.api.ast.RerankClause.RerankStrategy;
import org.pangrova.parser.api.ast.RerankClause.RrfStrategy;
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

        Optional<RerankerPlanNode> hybrid = detectImplicitHybrid(query, tableName, topK);
        if (hybrid.isPresent()) {
            return hybrid;
        }
        return detectFullTextRetrieval(query, tableName, topK);
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
     * Represents a bounded MATCH + BM25_SCORE query as a single-source retrieval plan.
     * RRF over one source preserves BM25 ordering while reusing the same
     * distributed retrieval envelope as hybrid search.
     */
    private static Optional<RerankerPlanNode> detectFullTextRetrieval(
            Query query, String tableName, int topK) {
        Optional<FullTextSource> scoreSource = extractScoreSource(query, tableName);
        if (scoreSource.isEmpty()) {
            return Optional.empty();
        }
        FullTextSource matchSource = extractPositiveMatchSource(query, tableName)
                .orElseThrow(() -> new RerankerValidationException(
                        "BM25_SCORE requires an equivalent positive MATCH predicate"));
        if (!sameTextSearch(scoreSource.orElseThrow(), matchSource)) {
            throw new RerankerValidationException(
                    "BM25_SCORE and MATCH must use the same column and query text");
        }
        return Optional.of(new RerankerPlanNode(
                new RerankerPlanNode.RrfStrategy(DEFAULT_RRF_K),
                List.of(matchSource),
                topK,
                matchSource.scoringNode().queryText(),
                List.of()));
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

        // FTS source from a positive conjunctive MATCH predicate.
        extractPositiveMatchSource(query, tableName).ifPresent(sources::add);

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

    private static Optional<FullTextSource> extractPositiveMatchSource(
            Query query, String tableName) {
        return query.where()
                .flatMap(RerankerInjectionRule::findPositiveConjunctiveMatch)
                .map(details -> fullTextSource(tableName, details));
    }

    private static Optional<MatchFunctionRewriter.MatchCallDetails> findPositiveConjunctiveMatch(
            Expression expression) {
        Optional<MatchFunctionRewriter.MatchCallDetails> direct =
                MatchFunctionRewriter.extractMatchCall(expression);
        if (direct.isPresent()) {
            return direct;
        }
        if (expression instanceof BinaryExpression binary
                && binary.operator() == BinaryExpression.Operator.AND) {
            Optional<MatchFunctionRewriter.MatchCallDetails> left =
                    findPositiveConjunctiveMatch(binary.left());
            return left.isPresent() ? left : findPositiveConjunctiveMatch(binary.right());
        }
        return Optional.empty();
    }

    private static Optional<FullTextSource> extractScoreSource(
            Query query, String tableName) {
        FullTextSource found = null;
        for (SortItem item : query.orderBy()) {
            Optional<MatchFunctionRewriter.MatchCallDetails> score =
                    MatchFunctionRewriter.extractScoreCall(item.expression());
            if (score.isEmpty()) {
                continue;
            }
            if (item.direction().orElse(SortItem.SortDirection.ASC)
                    != SortItem.SortDirection.DESC) {
                throw new RerankerValidationException(
                        "BM25_SCORE retrieval requires ORDER BY ... DESC");
            }
            FullTextSource candidate = fullTextSource(tableName, score.orElseThrow());
            if (found != null && !sameTextSearch(found, candidate)) {
                throw new RerankerValidationException(
                        "A bounded BM25 query cannot order by multiple text searches");
            }
            found = candidate;
        }
        return Optional.ofNullable(found);
    }

    private static FullTextSource fullTextSource(
            String tableName, MatchFunctionRewriter.MatchCallDetails details) {
        return new FullTextSource(
                MatchFunctionRewriter.toScoringNode(tableName, details, List.of()));
    }

    private static boolean sameTextSearch(FullTextSource left, FullTextSource right) {
        return left.scoringNode().column().equals(right.scoringNode().column())
                && left.scoringNode().queryText().equals(right.scoringNode().queryText());
    }

    private static boolean hasVectorDistanceOrderBy(Query query) {
        return query.orderBy().stream()
                .anyMatch(item -> isDistanceFunction(item.expression()));
    }

    private static boolean hasMatchFunction(Query query) {
        return query.where()
                .flatMap(RerankerInjectionRule::findPositiveConjunctiveMatch)
                .isPresent();
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
        return extractPositiveMatchSource(query, "")
                .or(() -> extractScoreSource(query, ""))
                .map(source -> source.scoringNode().queryText())
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
