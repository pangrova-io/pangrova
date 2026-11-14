package org.pangrova.planner.analysis;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import org.pangrova.planner.api.result.ResultColumnModel;

import org.pangrova.planner.api.invocation.AiInvocationPlanHint;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for the end-to-end SQL compilation pipeline.
 * Validates Requirements 11.1, 11.2, 11.3.
 */
class SqlCompilerTest {

    private static SqlCompiler compiler;

    @BeforeAll
    static void setUp() {
        compiler = new SqlCompiler();
    }

    @Test
    void compileSelectIntegerLiteral() {
        byte[] bytes = compiler.compile("SELECT 1");
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void compileSelectStringLiteral() {
        byte[] bytes = compiler.compile("SELECT 'hello'");
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void compileSelectConstantFolding() {
        byte[] bytes = compiler.compile("SELECT 1 + 2");
        assertThat(bytes).isNotNull().isNotEmpty();
    }

    @Test
    void compileToRelNodeReturnsRelNode() {
        RelNode relNode = compiler.compileToRelNode("SELECT 1");
        assertThat(relNode).isNotNull();
        assertThat(relNode.getRowType().getFieldCount()).isGreaterThan(0);
    }

    @Test
    void validatedSerializationProducesSameBytesAsCompile() {
        String sql = "SELECT 1 + 2";
        byte[] directBytes = compiler.compile(sql);
        PlanResult result = compiler.compileToResult(sql);
        byte[] splitBytes = compiler.convertValidateAndSerialize(result.plan(), result.preOptimizationColumnNames());
        assertThat(splitBytes).isEqualTo(directBytes);
    }

    @Test
    void aiExpressionEmitsStableTypedInvocationHintAndHiddenIdentity()
            throws Exception {
        PlanResult result = compiler.compileToResult(
                "SELECT AI_COMPLETE('catalog.llm', 'hello')");

        SerializedPlanFragment fragment =
                compiler.convertValidateAndSerializeFragment(
                        result.plan(), result.preOptimizationColumnNames());

        assertThat(fragment.aiInvocationHints()).singleElement().satisfies(hint -> {
            assertThat(hint.invocationId()).isEqualTo("ai-invoke-0");
            assertThat(hint.functionName()).isEqualTo("ai_complete");
            assertThat(hint.modelName()).isEqualTo("catalog.llm");
            assertThat(hint.operation())
                    .isEqualTo(AiInvocationPlanHint.Operation.TEXT_GENERATION);
            assertThat(hint.mediaInput()).isFalse();
        });
        io.substrait.proto.Rel root = io.substrait.proto.Plan
                .parseFrom(fragment.planBytes())
                .getRelations(0)
                .getRoot()
                .getInput();
        assertThat(root.hasProject()).isTrue();
        var invocation = root.getProject().getExpressionsList().stream()
                .filter(io.substrait.proto.Expression::hasScalarFunction)
                .findFirst()
                .orElseThrow()
                .getScalarFunction();
        assertThat(invocation.getArgumentsCount()).isEqualTo(3);
        assertThat(invocation.getArguments(2).getValue().getLiteral().getString())
                .isEqualTo("ai-invoke-0");
    }

    @Test
    void aiEmbedKeywordParsesAsOrdinaryScalarInvocation() throws Exception {
        PlanResult result = compiler.compileToResult(
                "SELECT AI_EMBED('catalog.embedding_model', 'hello')");

        assertThat(new PlanFragment(result.plan(), -1, -1)
                .resultColumnModels())
                .containsExactly(new ResultColumnModel(
                        0, "catalog.embedding_model"));

        SerializedPlanFragment fragment =
                compiler.convertValidateAndSerializeFragment(
                        result.plan(), result.preOptimizationColumnNames());

        assertThat(fragment.aiInvocationHints()).singleElement().satisfies(hint -> {
            assertThat(hint.functionName()).isEqualTo("ai_embed");
            assertThat(hint.modelName()).isEqualTo("catalog.embedding_model");
            assertThat(hint.operation())
                    .isEqualTo(AiInvocationPlanHint.Operation.EMBEDDING);
        });
        io.substrait.proto.Rel root = io.substrait.proto.Plan
                .parseFrom(fragment.planBytes())
                .getRelations(0)
                .getRoot()
                .getInput();
        var invocation = root.getProject().getExpressionsList().stream()
                .filter(io.substrait.proto.Expression::hasScalarFunction)
                .findFirst()
                .orElseThrow()
                .getScalarFunction();
        assertThat(invocation.getArgumentsCount()).isEqualTo(4);
        assertThat(invocation.getArguments(2).getValue().getLiteral().getString())
                .isEqualTo("RAW");
        assertThat(invocation.getArguments(3).getValue().getLiteral().getString())
                .isEqualTo("ai-invoke-0");
    }

    @Test
    void aiEmbedConstantSourceHasOneTaskAndKeepsModelOriginAcrossExchange() {
        PlanResult result = compiler.compileToResult(
                "SELECT AI_EMBED('catalog.embedding_model', 'hello')");
        FragmentedPlan fragmented = PlanFragmenter.fragment(result.plan(), 4);

        assertThat(fragmented.stages().stream()
                .filter(stage -> !stage.fragment().toSerializedPlan(
                        compiler, stage.fragment().fieldNames())
                        .aiInvocationHints().isEmpty()))
                .isNotEmpty()
                .allSatisfy(stage -> assertThat(stage.parallelism()).isOne());
        assertThat(fragmented.stages().stream()
                .map(stage -> stage.fragment().resultColumnModels())
                .filter(models -> !models.isEmpty()))
                .isNotEmpty()
                .allSatisfy(models -> assertThat(models)
                        .containsExactly(new ResultColumnModel(
                                0, "catalog.embedding_model")));
    }

    @Test
    void resultModelIntentsKeepEachExactModelAndExcludeOrdinaryArrays() {
        PlanResult result = compiler.compileToResult(
                "SELECT ARRAY[1.0, 2.0] AS ordinary_array, "
                        + "AI_EMBED('catalog.first_model', 'hello') AS first_embedding, "
                        + "AI_EMBED('catalog.second_model', 'world') AS second_embedding");

        assertThat(new PlanFragment(result.plan(), -1, -1)
                .resultColumnModels())
                .containsExactly(new ResultColumnModel(
                        1, "catalog.first_model"),
                        new ResultColumnModel(
                                2, "catalog.second_model"));
    }

}
