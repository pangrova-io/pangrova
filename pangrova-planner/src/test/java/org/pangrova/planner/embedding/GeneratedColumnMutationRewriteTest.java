package org.pangrova.planner.embedding;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.LogicalType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedColumnMutationRewriteTest {
    @Test
    void valuesAndAstInsertUseTheActualNewSourcesAndTypedWorkerInvocation() {
        String sql = "INSERT INTO documents (id, tenant, title, body) VALUES (1, 1, 'new-title', 'new-body')";
        for (boolean ast : List.of(false, true)) {
            var modify = GeneratedMutationFixture.plan(sql, GeneratedMutationFixture.definition(), ast);
            var project = (LogicalProject) modify.getInput();
            RexCall embed = embed(project, 4);
            assertSourceOrder(embed, project.getProjects().get(2), project.getProjects().get(3));
            assertThat(RexLiteral.stringValue(embed.getOperands().get(0))).isEqualTo("embedding_model");
            assertThat(embed.getType().getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
            assertThat(embed.getType().getComponentType().getSqlTypeName()).isEqualTo(SqlTypeName.REAL);
            for (int index = 0; index < project.getProjects().size(); index++) {
                assertThat(project.getProjects().get(index).getType())
                        .isEqualTo(modify.getTable().getRowType().getFieldList().get(index).getType());
            }
            assertInvocation(modify, 1);
        }
    }

    @Test
    void insertSelectEmbedsSelectedNewValuesInsteadOfReadingTheOldTarget() {
        var modify = GeneratedMutationFixture.plan("INSERT INTO documents (id, tenant, title, body) "
                + "SELECT id, tenant, body, title FROM source_rows");
        var project = (LogicalProject) modify.getInput();
        assertSourceOrder(embed(project, 4), project.getProjects().get(2), project.getProjects().get(3));
        assertThat(project.getInput().explain()).contains("source_rows").doesNotContain("documents");
        assertInvocation(modify, 1);
    }

    @Test
    void updateReadsSimultaneousNewAssignmentsNotSequentiallyReplacedValues() {
        var modify = GeneratedMutationFixture.plan("UPDATE documents SET title = body, body = title WHERE id = 1");
        var project = (LogicalProject) modify.getInput();
        assertThat(modify.getUpdateColumnList()).containsExactly("title", "body", "vector");
        assertThat(project.getProjects()).hasSize(8);
        assertSourceOrder(embed(project, 7), project.getProjects().get(5), project.getProjects().get(6));
        var source = (RexCall) embed(project, 7).getOperands().get(1);
        assertThat(((RexInputRef) source.getOperands().get(0)).getIndex()).isEqualTo(3);
        assertThat(((RexInputRef) source.getOperands().get(2)).getIndex()).isEqualTo(2);
        assertThat(modify.getSourceExpressionList()).hasSize(3);
        assertInvocation(modify, 1);
    }

    @Test
    void unrelatedUpdateAndDeleteNeverInvokeEmbedding() {
        var update = GeneratedMutationFixture.plan("UPDATE documents SET tenant = 2 WHERE id = 1");
        assertThat(update.getUpdateColumnList()).containsExactly("tenant");
        assertInvocation(update, 0);
        assertInvocation(GeneratedMutationFixture.plan("DELETE FROM documents WHERE id = 1"), 0);
    }

    @Test
    void nullSourceRemainsNullWithoutEmptyStringSubstitution() {
        var modify = GeneratedMutationFixture.plan("INSERT INTO documents (id, tenant, title, body) "
                + "VALUES (1, 1, NULL, 'body')");
        var project = (LogicalProject) modify.getInput();
        var source = (RexCall) embed(project, 4).getOperands().get(1);
        assertThat(source.getOperator().getName()).isEqualTo("CONCAT");
        assertThat(source.getOperands().get(0)).isEqualTo(project.getProjects().get(2));
        assertThat(source.getOperands()).hasSize(3);
        assertThat(source.getType().isNullable()).isTrue();
        assertInvocation(modify, 1);
    }

    @Test
    void mergeInsertAndUpdateActionsHaveDistinctNewExpressions() {
        var modify = GeneratedMutationFixture.plan("MERGE INTO documents t USING source_rows s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET title = s.body "
                + "WHEN NOT MATCHED THEN INSERT (id, tenant, title, body) VALUES (s.id, s.tenant, s.title, s.body)");
        var project = (LogicalProject) modify.getInput();
        assertThat(modify.getUpdateColumnList()).containsExactly("title", "vector");
        assertThat(project.getProjects()).hasSize(12);
        assertSourceOrder(embed(project, 4), project.getProjects().get(2), project.getProjects().get(3));
        assertSourceOrder(embed(project, 11), project.getProjects().get(10), project.getProjects().get(8));
        assertInvocation(modify, 2);
    }

    @Test
    void mergeSingleActionsAndAstDeleteDoNotFabricateOtherActions() {
        assertInvocation(GeneratedMutationFixture.plan("MERGE INTO documents t USING source_rows s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET title = s.title"), 1);
        assertInvocation(GeneratedMutationFixture.plan("MERGE INTO documents t USING source_rows s ON t.id = s.id "
                + "WHEN NOT MATCHED THEN INSERT (id, tenant, title, body) VALUES (s.id, s.tenant, s.title, s.body)"), 1);
        assertInvocation(GeneratedMutationFixture.plan("MERGE INTO documents t USING source_rows s ON t.id = s.id "
                + "WHEN MATCHED THEN DELETE", GeneratedMutationFixture.definition(), true), 0);
    }

    @Test
    void explicitGeneratedWritesIncludingNullAndImplicitAllColumnsAreRejected() {
        for (String sql : List.of("INSERT INTO documents (id, tenant, title, body, vector) VALUES (1, 1, 'a', 'b', NULL)",
                "UPDATE documents SET vector = NULL WHERE id = 1",
                "MERGE INTO documents t USING source_rows s ON t.id = s.id WHEN MATCHED THEN UPDATE SET vector = NULL")) {
            assertThatThrownBy(() -> GeneratedMutationFixture.plan(sql)).hasMessageContaining("Explicit writes");
        }
        assertThatThrownBy(() -> GeneratedMutationFixture.plan("INSERT INTO documents VALUES (1, 1, 'a', 'b', NULL)"))
                .hasMessageContaining("explicit non-generated target column list");
    }

    @Test
    void asynchronousDefinitionIsNotMistakenForAnInlineExpression() {
        var definition = GeneratedMutationFixture.definition().toBuilder()
                .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC).build();
        var modify = GeneratedMutationFixture.plan("INSERT INTO documents (id, tenant, title, body) "
                + "VALUES (1, 1, 'a', 'b')", definition, false);
        assertInvocation(modify, 0);
        assertThatThrownBy(() -> GeneratedMutationFixture.plan("UPDATE documents SET vector = NULL", definition, false))
                .hasMessageContaining("Explicit writes");
    }

    @Test
    void frozenDimensionAndCapabilityMismatchFailClosedBeforeDispatch() {
        String sql = "INSERT INTO documents (id, tenant, title, body) VALUES (1, 1, 'a', 'b')";
        var wrongType = GeneratedMutationFixture.definition().toBuilder()
                .setOutputType(LogicalType.newBuilder().setCanonicalSql("VECTOR(4, COSINE)")).build();
        assertThatThrownBy(() -> GeneratedMutationFixture.plan(sql, wrongType, false))
                .hasMessageContaining("compiled storage declaration");
        var wrongCapability = GeneratedMutationFixture.definition().toBuilder().setCapability(
                GeneratedMutationFixture.definition().getCapability().toBuilder().setSemanticVersion("999")).build();
        assertThatThrownBy(() -> GeneratedMutationFixture.plan(sql, wrongCapability, false))
                .hasMessageContaining("canonical text embedding");
    }

    private static RexCall embed(LogicalProject project, int index) {
        RexCall call = (RexCall) project.getProjects().get(index);
        assertThat(call.getOperator().getName()).isEqualTo("AI_EMBED");
        assertThat(call.getOperands()).hasSize(3);
        assertThat(RexLiteral.stringValue(call.getOperands().get(2))).isEqualTo("DOCUMENT");
        return call;
    }

    private static void assertSourceOrder(RexCall embed, org.apache.calcite.rex.RexNode first,
            org.apache.calcite.rex.RexNode second) {
        var source = (RexCall) embed.getOperands().get(1);
        assertThat(source.getOperands()).containsExactly(first, source.getOperands().get(1), second);
        assertThat(RexLiteral.stringValue(source.getOperands().get(1))).isEqualTo(GeneratedColumnSourceEncoding.SEPARATOR);
    }

    private static void assertInvocation(LogicalTableModify modify, int count) {
        var result = new SubstraitConverter().convertWithResult(modify, List.of());
        assertThat(result.aiInvocationHints()).hasSize(count);
        assertThat(result.aiInvocationHints()).allSatisfy(hint -> {
            assertThat(hint.modelName()).isEqualTo("embedding_model");
            assertThat(hint.functionName()).isEqualTo("ai_embed");
        });
        assertThat(new SubstraitPlanSerializer().serialize(result.plan())).isNotEmpty();
    }
}
