package org.pangrova.planner.embedding;

import org.pangrova.planner.analysis.PlanResult;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.api.enums.AiInvocationOperation;
import org.pangrova.protocol.type.VectorLogicalType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryTextVectorRewriteTest {

    private static final String TEXT_QUERY =
            "SELECT id, COSINE_DISTANCE(vector, 'hello world') AS distance FROM documents ORDER BY distance LIMIT 3";

    @Test
    void rewritesTextQueryVectorIntoGovernedEmbedding() {
        var compiler = GeneratedMutationFixture.queryCompiler(GeneratedMutationFixture.definition());
        PlanResult result = compiler.compileToResult(TEXT_QUERY);

        RexCall embed = singleEmbedding(result.plan());
        assertThat(RexLiteral.stringValue(embed.getOperands().get(0))).isEqualTo("embedding_model");
        assertThat(RexLiteral.stringValue(embed.getOperands().get(1))).isEqualTo("hello world");
        assertThat(RexLiteral.stringValue(embed.getOperands().get(2))).isEqualTo("QUERY");

        SerializedPlanFragment fragment = compiler.convertValidateAndSerializeFragment(
                result.plan(), result.preOptimizationColumnNames());
        assertThat(fragment.aiInvocationHints()).singleElement().satisfies(hint -> {
            assertThat(hint.functionName()).isEqualTo("ai_embed");
            assertThat(hint.modelName()).isEqualTo("embedding_model");
            assertThat(hint.operation()).isEqualTo(AiInvocationOperation.EMBEDDING);
        });
    }

    @Test
    void rewritesTextQueryVectorInsideFilters() {
        var compiler = GeneratedMutationFixture.queryCompiler(GeneratedMutationFixture.definition());
        PlanResult result = compiler.compileToResult(
                "SELECT id FROM documents WHERE L2_DISTANCE(vector, 'needle') < 2.0");

        assertThat(calls(result.plan()))
                .anySatisfy(call -> assertThat(call.getOperator().getName()).isEqualTo("AI_EMBED"));
    }

    @Test
    void leavesLiteralVectorQueriesUnchanged() {
        var compiler = GeneratedMutationFixture.queryCompiler(GeneratedMutationFixture.definition());
        PlanResult result = compiler.compileToResult(
                "SELECT COSINE_DISTANCE(vector, ARRAY[1.0, 0.0, 0.0]) AS d FROM documents");

        assertThat(calls(result.plan()))
                .noneMatch(call -> call.getOperator().getName().equals("AI_EMBED"));
    }

    @Test
    void rejectsTextQueryOnNonAutoEmbeddingColumn() {
        var compiler = GeneratedMutationFixture.queryCompilerWithoutAutoEmbedding();
        assertThatThrownBy(() -> compiler.compileToResult(TEXT_QUERY))
                .hasMessageContaining("auto-embedding");
    }

    @Test
    void rejectsDimensionMismatchBetweenColumnAndModel() {
        var definition = GeneratedMutationFixture.definition().toBuilder()
                .setOutputType(VectorLogicalType.create(4)).build();
        var compiler = GeneratedMutationFixture.queryCompiler(definition);
        assertThatThrownBy(() -> compiler.compileToResult(TEXT_QUERY))
                .hasMessageContaining("dimension");
    }

    @Test
    void rejectsTextAsTheFirstVectorOperand() {
        var compiler = GeneratedMutationFixture.queryCompiler(GeneratedMutationFixture.definition());
        assertThatThrownBy(() -> compiler.compileToResult(
                "SELECT COSINE_DISTANCE('hello', vector) FROM documents"))
                .isInstanceOf(RuntimeException.class);
    }

    private static RexCall singleEmbedding(RelNode node) {
        List<RexCall> embeddings = calls(node).stream()
                .filter(call -> call.getOperator().getName().equals("AI_EMBED"))
                .toList();
        assertThat(embeddings).hasSize(1);
        return embeddings.get(0);
    }

    private static List<RexCall> calls(RelNode node) {
        List<RexCall> calls = new ArrayList<>();
        node.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalProject project) {
                project.getProjects().forEach(expression -> collect(expression, calls));
                return super.visit(project);
            }

            @Override
            public RelNode visit(LogicalFilter filter) {
                collect(filter.getCondition(), calls);
                return super.visit(filter);
            }

            @Override
            public RelNode visit(LogicalJoin join) {
                collect(join.getCondition(), calls);
                return super.visit(join);
            }
        });
        return calls;
    }

    private static void collect(RexNode expression, List<RexCall> calls) {
        expression.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                calls.add(call);
                return super.visitCall(call);
            }
        });
    }
}
