package org.pangrova.planner.embedding;

import org.pangrova.planner.embedding.AutoEmbedRewriteRule.DmlOperation;
import org.pangrova.planner.embedding.AutoEmbedRewriteRule.EmbedDirective;
import org.pangrova.planner.embedding.AutoEmbedRewriteRule.EmbedInjectionPlan;
import org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor.EmbeddingMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AutoEmbedRewriteRuleTest {

    private static final AutoEmbeddingColumnDescriptor SYNC_COLUMN = new AutoEmbeddingColumnDescriptor(
            "embedding", "bge_m3", List.of("title", "content"), EmbeddingMode.SYNC, 1024);

    private static final AutoEmbeddingColumnDescriptor ASYNC_COLUMN = new AutoEmbeddingColumnDescriptor(
            "image_vec", "clip_vit", List.of("caption"), EmbeddingMode.ASYNC, 512);

    private final EmbeddingColumnResolver resolver = tableName -> {
        if ("catalog.schema.knowledge_base".equals(tableName)) {
            return List.of(SYNC_COLUMN);
        }
        if ("catalog.schema.products".equals(tableName)) {
            return List.of(SYNC_COLUMN, ASYNC_COLUMN);
        }
        return List.of();
    };

    private final AutoEmbedRewriteRule rule = new AutoEmbedRewriteRule(resolver);

    @Test
    void constructorRejectsNullResolver() {
        assertThatThrownBy(() -> new AutoEmbedRewriteRule(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void insertOnTableWithSyncEmbedding() {
        EmbedInjectionPlan plan = rule.analyzeInsert("catalog.schema.knowledge_base");

        assertThat(plan.isEmpty()).isFalse();
        assertThat(plan.operation()).isEqualTo(DmlOperation.INSERT);
        assertThat(plan.tableName()).isEqualTo("catalog.schema.knowledge_base");
        assertThat(plan.directives()).hasSize(1);

        EmbedDirective directive = plan.directives().get(0);
        assertThat(directive.vectorColumn()).isEqualTo("embedding");
        assertThat(directive.modelName()).isEqualTo("bge_m3");
        assertThat(directive.sourceColumns()).containsExactly("title", "content");
        assertThat(directive.mode()).isEqualTo(EmbeddingMode.SYNC);
        assertThat(directive.dimension()).isEqualTo(1024);
    }

    @Test
    void insertOnTableWithMultipleEmbeddings() {
        EmbedInjectionPlan plan = rule.analyzeInsert("catalog.schema.products");

        assertThat(plan.directives()).hasSize(2);
        assertThat(plan.hasSyncDirectives()).isTrue();
        assertThat(plan.hasAsyncDirectives()).isTrue();
    }

    @Test
    void insertOnTableWithoutEmbeddings() {
        EmbedInjectionPlan plan = rule.analyzeInsert("catalog.schema.other_table");

        assertThat(plan.isEmpty()).isTrue();
        assertThat(plan).isSameAs(EmbedInjectionPlan.EMPTY);
    }

    @Test
    void insertWithNullTableName() {
        assertThat(rule.analyzeInsert(null).isEmpty()).isTrue();
    }

    @Test
    void insertWithBlankTableName() {
        assertThat(rule.analyzeInsert("  ").isEmpty()).isTrue();
    }

    @Test
    void updateWithChangedSourceColumn() {
        EmbedInjectionPlan plan = rule.analyzeUpdate(
                "catalog.schema.knowledge_base", Set.of("title"));

        assertThat(plan.isEmpty()).isFalse();
        assertThat(plan.operation()).isEqualTo(DmlOperation.UPDATE);
        assertThat(plan.directives()).hasSize(1);
        assertThat(plan.directives().get(0).vectorColumn()).isEqualTo("embedding");
    }

    @Test
    void updateWithUnchangedSourceColumns() {
        EmbedInjectionPlan plan = rule.analyzeUpdate(
                "catalog.schema.knowledge_base", Set.of("category", "created_at"));

        assertThat(plan.isEmpty()).isTrue();
    }

    @Test
    void updateCaseInsensitiveColumnMatching() {
        EmbedInjectionPlan plan = rule.analyzeUpdate(
                "catalog.schema.knowledge_base", Set.of("TITLE"));

        assertThat(plan.isEmpty()).isFalse();
        assertThat(plan.directives()).hasSize(1);
    }

    @Test
    void updateOnlyAffectedEmbeddingsIncluded() {
        EmbedInjectionPlan plan = rule.analyzeUpdate(
                "catalog.schema.products", Set.of("caption"));

        assertThat(plan.directives()).hasSize(1);
        assertThat(plan.directives().get(0).vectorColumn()).isEqualTo("image_vec");
        assertThat(plan.directives().get(0).mode()).isEqualTo(EmbeddingMode.ASYNC);
    }

    @Test
    void updateWithNullTableName() {
        assertThat(rule.analyzeUpdate(null, Set.of("title")).isEmpty()).isTrue();
    }

    @Test
    void updateWithNullUpdatedColumns() {
        assertThat(rule.analyzeUpdate("catalog.schema.knowledge_base", null).isEmpty()).isTrue();
    }

    @Test
    void updateWithEmptyUpdatedColumns() {
        assertThat(rule.analyzeUpdate("catalog.schema.knowledge_base", Set.of()).isEmpty()).isTrue();
    }

    @Test
    void embedInjectionPlanImmutability() {
        EmbedInjectionPlan plan = rule.analyzeInsert("catalog.schema.knowledge_base");

        assertThatThrownBy(() -> plan.directives().add(
                new EmbedDirective("x", "m", List.of("a"), EmbeddingMode.SYNC, 128, null)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
