package org.pangrova.planner.core.embedding;

import org.pangrova.planner.core.embedding.AutoEmbeddingColumnDescriptor.EmbeddingMode;
import org.pangrova.planner.core.embedding.QueryTextEmbedRule.QueryEmbedDirective;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryTextEmbedRuleTest {

    private static final AutoEmbeddingColumnDescriptor EMBEDDING_COL = new AutoEmbeddingColumnDescriptor(
            "embedding", "bge_m3", List.of("title", "content"), EmbeddingMode.SYNC, 1024);

    private final EmbeddingColumnResolver resolver = tableName -> {
        if ("catalog.schema.knowledge_base".equals(tableName)) {
            return List.of(EMBEDDING_COL);
        }
        return List.of();
    };

    private final QueryTextEmbedRule rule = new QueryTextEmbedRule(resolver);

    @Test
    void constructorRejectsNullResolver() {
        assertThatThrownBy(() -> new QueryTextEmbedRule(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rewritesCosineDistanceWithTextLiteral() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.knowledge_base", "COSINE_DISTANCE", "embedding", "how does consensus work");

        assertThat(result).isPresent();
        QueryEmbedDirective directive = result.get();
        assertThat(directive.modelName()).isEqualTo("bge_m3");
        assertThat(directive.queryText()).isEqualTo("how does consensus work");
        assertThat(directive.dimension()).isEqualTo(1024);
        assertThat(directive.vectorColumn()).isEqualTo("embedding");
    }

    @Test
    void rewritesL2DistanceWithTextLiteral() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.knowledge_base", "L2_DISTANCE", "embedding", "vector search");

        assertThat(result).isPresent();
        assertThat(result.get().modelName()).isEqualTo("bge_m3");
    }

    @Test
    void rewritesInnerProductWithTextLiteral() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.knowledge_base", "INNER_PRODUCT", "embedding", "machine learning");

        assertThat(result).isPresent();
        assertThat(result.get().modelName()).isEqualTo("bge_m3");
    }

    @Test
    void caseInsensitiveFunctionName() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.knowledge_base", "cosine_distance", "embedding", "query text");

        assertThat(result).isPresent();
    }

    @Test
    void returnsEmptyForNonDistanceFunction() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.knowledge_base", "UPPER", "embedding", "some text");

        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyForNonEmbeddingColumn() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.knowledge_base", "COSINE_DISTANCE", "other_column", "query");

        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyForUnknownTable() {
        Optional<QueryEmbedDirective> result = rule.tryRewrite(
                "catalog.schema.unknown", "COSINE_DISTANCE", "embedding", "query");

        assertThat(result).isEmpty();
    }

    @Test
    void returnsEmptyForNullTableName() {
        assertThat(rule.tryRewrite(null, "COSINE_DISTANCE", "embedding", "query")).isEmpty();
    }

    @Test
    void returnsEmptyForBlankColumnName() {
        assertThat(rule.tryRewrite(
                "catalog.schema.knowledge_base", "COSINE_DISTANCE", "  ", "query")).isEmpty();
    }

    @Test
    void returnsEmptyForNullTextLiteral() {
        assertThat(rule.tryRewrite(
                "catalog.schema.knowledge_base", "COSINE_DISTANCE", "embedding", null)).isEmpty();
    }

    @Test
    void returnsEmptyForBlankTextLiteral() {
        assertThat(rule.tryRewrite(
                "catalog.schema.knowledge_base", "COSINE_DISTANCE", "embedding", "  ")).isEmpty();
    }

    @Test
    void isDistanceFunctionRecognizesAllFunctions() {
        assertThat(QueryTextEmbedRule.isDistanceFunction("COSINE_DISTANCE")).isTrue();
        assertThat(QueryTextEmbedRule.isDistanceFunction("L2_DISTANCE")).isTrue();
        assertThat(QueryTextEmbedRule.isDistanceFunction("INNER_PRODUCT")).isTrue();
        assertThat(QueryTextEmbedRule.isDistanceFunction("cosine_distance")).isTrue();
        assertThat(QueryTextEmbedRule.isDistanceFunction("UNKNOWN")).isFalse();
        assertThat(QueryTextEmbedRule.isDistanceFunction(null)).isFalse();
    }

    @Test
    void queryEmbedDirectiveValidation() {
        assertThatThrownBy(() -> new QueryEmbedDirective(null, "text", 128, "col"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueryEmbedDirective("model", null, 128, "col"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueryEmbedDirective("model", "text", 0, "col"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueryEmbedDirective("model", "text", 128, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
