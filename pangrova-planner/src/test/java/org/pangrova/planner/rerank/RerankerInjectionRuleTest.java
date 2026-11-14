package org.pangrova.planner.rerank;

import org.pangrova.planner.exception.RerankerValidationException;

import org.pangrova.parser.api.ast.AllColumns;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.GroupByExpression;
import org.pangrova.parser.api.ast.GroupByItem;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.SortItem;
import org.pangrova.parser.api.ast.TableRef;
import org.pangrova.parser.api.ast.expression.FunctionCall;
import org.pangrova.parser.api.ast.RerankClause;
import org.pangrova.planner.rerank.RerankerPlanNode.FullTextSource;
import org.pangrova.planner.rerank.RerankerPlanNode.RrfStrategy;
import org.pangrova.planner.rerank.RerankerPlanNode.VectorSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RerankerInjectionRuleTest {

    private static final String TABLE = "catalog.schema.knowledge_base";

    @Test
    void returnsEmptyForNonPositiveTopK() {
        Query query = buildQuery(List.of(), Optional.empty(), Optional.empty());
        assertThat(RerankerInjectionRule.analyze(query, TABLE, 0)).isEmpty();
        assertThat(RerankerInjectionRule.analyze(query, TABLE, -1)).isEmpty();
    }

    @Test
    void returnsEmptyWhenNoRerankByAndNoHybridPattern() {
        Query query = buildQuery(List.of(), Optional.empty(), Optional.empty());
        assertThat(RerankerInjectionRule.analyze(query, TABLE, 10)).isEmpty();
    }

    @Test
    void throwsWhenRerankByWithGroupBy() {
        var rerankClause = new RerankClause(new RerankClause.RrfStrategy(Literal.ofInteger(60)));
        Query query = buildQueryWithGroupBy(
                List.of(), Optional.empty(), Optional.of(rerankClause),
                List.of(new GroupByExpression(Identifier.of("category"))));

        assertThatThrownBy(() -> RerankerInjectionRule.analyze(query, TABLE, 10))
                .isInstanceOf(RerankerValidationException.class)
                .hasMessageContaining("incompatible with GROUP BY");
    }

    @Test
    void throwsWhenRerankByWithHaving() {
        var rerankClause = new RerankClause(new RerankClause.RrfStrategy(Literal.ofInteger(60)));
        Query query = buildQueryWithHaving(
                List.of(), Optional.empty(), Optional.of(rerankClause),
                Optional.of(Literal.ofInteger(1)));

        assertThatThrownBy(() -> RerankerInjectionRule.analyze(query, TABLE, 10))
                .isInstanceOf(RerankerValidationException.class)
                .hasMessageContaining("incompatible with GROUP BY/HAVING");
    }

    @Test
    void explicitRrfWithVectorOrderBy() {
        var distanceCall = distanceFunction("cosine_distance", "embedding", "'query text'");
        var rerankClause = new RerankClause(new RerankClause.RrfStrategy(Literal.ofInteger(60)));
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.empty(),
                Optional.of(rerankClause));

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 10);

        assertThat(result).isPresent();
        RerankerPlanNode node = result.get();
        assertThat(node.strategy()).isInstanceOf(RrfStrategy.class);
        assertThat(((RrfStrategy) node.strategy()).k()).isEqualTo(60);
        assertThat(node.topK()).isEqualTo(10);
        assertThat(node.sources()).hasSize(1);
        assertThat(node.sources().get(0)).isInstanceOf(VectorSource.class);
    }

    @Test
    void explicitRerankWithMatchInWhere() {
        var matchCall = matchFunction("content", "consensus algorithm");
        var distanceCall = distanceFunction("cosine_distance", "embedding", "'query'");
        var rerankClause = new RerankClause(new RerankClause.RrfStrategy(Literal.ofInteger(60)));
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.of(matchCall),
                Optional.of(rerankClause));

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 5);

        assertThat(result).isPresent();
        RerankerPlanNode node = result.get();
        assertThat(node.isHybrid()).isTrue();
        assertThat(node.sources()).hasSize(2);
        assertThat(node.sources().get(0)).isInstanceOf(VectorSource.class);
        assertThat(node.sources().get(1)).isInstanceOf(FullTextSource.class);
        assertThat(node.queryText()).isEqualTo("consensus algorithm");
    }

    @Test
    void implicitHybridDefaultsToRrf60() {
        var matchCall = matchFunction("content", "distributed consensus");
        var distanceCall = distanceFunction("cosine_distance", "embedding", "'query'");
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.of(matchCall),
                Optional.empty());

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 10);

        assertThat(result).isPresent();
        RerankerPlanNode node = result.get();
        assertThat(node.strategy()).isInstanceOf(RrfStrategy.class);
        assertThat(((RrfStrategy) node.strategy()).k()).isEqualTo(60);
        assertThat(node.isHybrid()).isTrue();
    }

    @Test
    void noImplicitHybridWithOnlyVectorOrderBy() {
        var distanceCall = distanceFunction("cosine_distance", "embedding", "'query'");
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.empty(),
                Optional.empty());

        assertThat(RerankerInjectionRule.analyze(query, TABLE, 10)).isEmpty();
    }

    @Test
    void standaloneMatchIsNotIncorrectlyTruncatedIntoBoundedTopK() {
        var matchCall = matchFunction("content", "search query");
        Query query = buildQuery(List.of(), Optional.of(matchCall), Optional.empty());

        assertThat(RerankerInjectionRule.analyze(query, TABLE, 10)).isEmpty();
    }

    @Test
    void bm25ScoreWithEquivalentMatchProducesBoundedRetrievalPlan() {
        var matchCall = matchFunction("content", "search query");
        var scoreCall = bm25ScoreFunction("content", "search query");
        Query query = buildQuery(
                List.of(new SortItem(scoreCall, Optional.of(SortItem.SortDirection.DESC))),
                Optional.of(matchCall), Optional.empty());

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 10);

        assertThat(result).isPresent();
        assertThat(result.get().isHybrid()).isFalse();
        assertThat(result.get().sources()).singleElement().isInstanceOf(FullTextSource.class);
        assertThat(result.get().queryText()).isEqualTo("search query");
    }

    @Test
    void bm25ScoreWithoutMatchFailsClosed() {
        var scoreCall = bm25ScoreFunction("content", "search query");
        Query query = buildQuery(
                List.of(new SortItem(scoreCall, Optional.of(SortItem.SortDirection.DESC))),
                Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> RerankerInjectionRule.analyze(query, TABLE, 10))
                .isInstanceOf(RerankerValidationException.class)
                .hasMessageContaining("positive MATCH");
    }

    @Test
    void bm25ScoreAndMatchMustDescribeTheSameSearch() {
        var matchCall = matchFunction("content", "one query");
        var scoreCall = bm25ScoreFunction("content", "another query");
        Query query = buildQuery(
                List.of(new SortItem(scoreCall, Optional.of(SortItem.SortDirection.DESC))),
                Optional.of(matchCall), Optional.empty());

        assertThatThrownBy(() -> RerankerInjectionRule.analyze(query, TABLE, 10))
                .isInstanceOf(RerankerValidationException.class)
                .hasMessageContaining("same column and query text");
    }

    @Test
    void bm25ScoreRequiresDescendingOrder() {
        var matchCall = matchFunction("content", "search query");
        var scoreCall = bm25ScoreFunction("content", "search query");
        Query query = buildQuery(
                List.of(new SortItem(scoreCall)), Optional.of(matchCall), Optional.empty());

        assertThatThrownBy(() -> RerankerInjectionRule.analyze(query, TABLE, 10))
                .isInstanceOf(RerankerValidationException.class)
                .hasMessageContaining("DESC");
    }

    @Test
    void singleSourceRerankingSupported() {
        var distanceCall = distanceFunction("l2_distance", "embedding", "'query'");
        var rerankClause = new RerankClause(
                new RerankClause.CrossEncoderStrategy("ms_marco", Literal.ofInteger(20)));
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.empty(),
                Optional.of(rerankClause));

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 10);

        assertThat(result).isPresent();
        assertThat(result.get().isHybrid()).isFalse();
        assertThat(result.get().sources()).hasSize(1);
    }

    @Test
    void convertsCascadeStrategy() {
        var distanceCall = distanceFunction("cosine_distance", "embedding", "'query'");
        var cascade = new RerankClause.CascadeStrategy(List.of(
                new RerankClause.RrfStrategy(Literal.ofInteger(60)),
                new RerankClause.CrossEncoderStrategy("ms_marco", Literal.ofInteger(5))));
        var rerankClause = new RerankClause(cascade);
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.empty(),
                Optional.of(rerankClause));

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 10);

        assertThat(result).isPresent();
        assertThat(result.get().strategy())
                .isInstanceOf(RerankerPlanNode.CascadeStrategy.class);
        var cascadeNode = (RerankerPlanNode.CascadeStrategy) result.get().strategy();
        assertThat(cascadeNode.stages()).hasSize(2);
    }

    @Test
    void multiVectorOnColumns() {
        var distanceCall = distanceFunction("cosine_distance", "text_vec", "'query'");
        var rerankClause = new RerankClause(
                new RerankClause.RrfStrategy(Literal.ofInteger(60)),
                List.of(Identifier.of("text_vec"), Identifier.of("image_vec")));
        Query query = buildQuery(
                List.of(new SortItem(distanceCall)),
                Optional.empty(),
                Optional.of(rerankClause));

        Optional<RerankerPlanNode> result = RerankerInjectionRule.analyze(query, TABLE, 10);

        assertThat(result).isPresent();
        assertThat(result.get().targetColumns()).containsExactly("text_vec", "image_vec");
        assertThat(result.get().isMultiVector()).isTrue();
    }

    // ── Helper methods ──

    private static Query buildQuery(
            List<SortItem> orderBy,
            Optional<Expression> where,
            Optional<RerankClause> rerankBy) {
        return new Query(
                false,
                List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of(TABLE))),
                List.of(),
                where,
                List.of(),
                Optional.empty(),
                Optional.empty(),
                orderBy,
                rerankBy,
                Optional.of(Literal.ofInteger(10)),
                Optional.empty());
    }

    private static Query buildQueryWithGroupBy(
            List<SortItem> orderBy,
            Optional<Expression> where,
            Optional<RerankClause> rerankBy,
            List<GroupByItem> groupBy) {
        return new Query(
                false,
                List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of(TABLE))),
                List.of(),
                where,
                groupBy,
                Optional.empty(),
                Optional.empty(),
                orderBy,
                rerankBy,
                Optional.of(Literal.ofInteger(10)),
                Optional.empty());
    }

    private static Query buildQueryWithHaving(
            List<SortItem> orderBy,
            Optional<Expression> where,
            Optional<RerankClause> rerankBy,
            Optional<Expression> having) {
        return new Query(
                false,
                List.of(new AllColumns()),
                List.of(new TableRef(Identifier.of(TABLE))),
                List.of(),
                where,
                List.of(),
                having,
                Optional.empty(),
                orderBy,
                rerankBy,
                Optional.of(Literal.ofInteger(10)),
                Optional.empty());
    }

    private static FunctionCall distanceFunction(String funcName, String column, String queryArg) {
        return new FunctionCall(
                Identifier.of(funcName),
                List.of(Identifier.of(column), Literal.ofString(queryArg)));
    }

    private static FunctionCall matchFunction(String column, String queryText) {
        return new FunctionCall(
                Identifier.of("MATCH"),
                List.of(Identifier.of(column), Literal.ofString(queryText)));
    }

    private static FunctionCall bm25ScoreFunction(String column, String queryText) {
        return new FunctionCall(
                Identifier.of("BM25_SCORE"),
                List.of(Identifier.of(column), Literal.ofString(queryText)));
    }
}
