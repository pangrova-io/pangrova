package org.pangrova.planner.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.common.enums.PangrovaErrorCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real public SQL-to-canonical checks, not an independent provider/value oracle. */
class AiEmbeddingPurposeSqlProtocolTest {
    @Test
    void publicTwoArgumentEmbeddingMaterializesRawBeforeInvocationIdentity() throws Exception {
        var invocation = invocation("SELECT AI_EMBED('catalog.model', 'document')");
        assertThat(invocation.getArgumentsCount()).isEqualTo(4);
        assertThat(invocation.getArguments(0).getValue().getLiteral().getString()).isEqualTo("catalog.model");
        assertThat(invocation.getArguments(1).getValue().getLiteral().getString()).isEqualTo("document");
        assertThat(invocation.getArguments(2).getValue().getLiteral().getString()).isEqualTo("RAW");
        assertThat(invocation.getArguments(3).getValue().getLiteral().getString()).isEqualTo("ai-invoke-0");
    }

    @ParameterizedTest
    @ValueSource(strings = {"RAW", "QUERY", "DOCUMENT"})
    void explicitPurposeIsPreservedRegardlessOfInputRowCount(String purpose) throws Exception {
        var invocation = invocation("SELECT AI_EMBED('catalog.model', text, '" + purpose
                + "') FROM (SELECT 'a' AS text UNION ALL SELECT 'b' AS text) AS t");
        assertThat(invocation.getArgumentsCount()).isEqualTo(4);
        assertThat(invocation.getArguments(2).getValue().getLiteral().getString()).isEqualTo(purpose);
        assertThat(invocation.getArguments(3).getValue().getLiteral().getString()).isEqualTo("ai-invoke-0");
    }

    @ParameterizedTest
    @ValueSource(strings = {"'AUTO'", "'query'", "NULL", "12", "purpose", "CAST('DOCUMENT' AS VARCHAR(3))"})
    void invalidNullOrRowDependentPurposeIsRejectedAtThePublicEntry(String purpose) throws Exception {
        String source = " FROM (SELECT 'a' AS text, 'QUERY' AS purpose"
                + " UNION ALL SELECT 'b' AS text, 'DOCUMENT' AS purpose) AS t";
        var admitted = invocation("SELECT AI_EMBED('catalog.model', text, 'RAW')" + source);
        assertThat(admitted.getArguments(2).getValue().getLiteral().getString()).isEqualTo("RAW");
        String sql = "SELECT AI_EMBED('catalog.model', text, " + purpose
                + ")" + source;
        assertThatThrownBy(() -> new SqlCompiler().compileToResult(sql))
                .isInstanceOfSatisfying(PlannerException.class, failure -> assertThat(failure.getErrorCode())
                        .isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE))
                .hasMessageContaining("Validation failed:")
                .hasMessageContaining("AI_EMBED purpose");
    }

    @Test
    void losslessConstantCastNormalizesToTheSameDeclaredPurposeLiteral() throws Exception {
        var invocation = invocation("SELECT AI_EMBED('catalog.model', 'text', CAST('QUERY' AS VARCHAR(20)))");
        assertThat(invocation.getArguments(2).getValue().hasLiteral()).isTrue();
        assertThat(invocation.getArguments(2).getValue().getLiteral().getString()).isEqualTo("QUERY");
    }

    private static Expression.ScalarFunction invocation(String sql) throws Exception {
        var compiler = new SqlCompiler();
        var result = compiler.compileToResult(sql);
        var fragment = compiler.convertValidateAndSerializeFragment(result.plan(), result.preOptimizationColumnNames());
        var relation = Plan.parseFrom(fragment.planBytes()).getRelations(0).getRoot().getInput();
        assertThat(relation.hasProject()).isTrue();
        return relation.getProject().getExpressionsList().stream()
                .filter(Expression::hasScalarFunction).findFirst().orElseThrow().getScalarFunction();
    }
}
