package org.pangrova.planner.substrait;

import com.google.protobuf.Message;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.Type;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SQL planning and production IR emission; no provider or Native inventory is substituted. */
class CallbackProducerSqlTest {
    private static final String SOURCE =
            " FROM (SELECT 'a' AS text UNION ALL SELECT 'b' AS text) AS input";

    @Test
    void identicalCallbackOccurrencesRetainDistinctInvocationIdentity() throws Exception {
        var compiler = new SqlCompiler();
        var planned = compiler.compileToResult(
                "SELECT AI_COMPLETE('catalog.model', text) AS first_result,"
                        + " AI_COMPLETE('catalog.model', text) AS second_result" + SOURCE);
        var fragment = compiler.convertValidateAndSerializeFragment(
                planned.plan(), planned.preOptimizationColumnNames());
        assertThat(fragment.aiInvocationHints()).hasSize(2);
        assertThat(fragment.aiInvocationHints()).extracting(hint -> hint.invocationId())
                .containsExactly("ai-invoke-0", "ai-invoke-1");
        assertThat(Plan.parseFrom(fragment.planBytes()).getRelations(0).getRoot().getNamesList())
                .containsExactly("first_result", "second_result");
        assertThat(scalarCalls(Plan.parseFrom(fragment.planBytes()))).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "AI_COMPLETE('catalog.model', text)",
            "AI_EMBED('catalog.model', text)",
            "FILE_FROM_URL(text)"
    })
    void repeatedProducerAliasKeepsOneProducerBelowTwoColumnReferences(String call) throws Exception {
        var compiler = new SqlCompiler();
        var planned = compiler.compileToResult(
                "SELECT generated AS first_result, generated AS second_result FROM (SELECT "
                        + call + " AS generated" + SOURCE + ") produced");
        assertThat(planned.plan()).isInstanceOfSatisfying(Project.class, outer -> {
            assertThat(outer.getProjects()).hasSize(2).allSatisfy(expression ->
                    assertThat(expression).isInstanceOfSatisfying(RexInputRef.class,
                            reference -> assertThat(reference.getIndex()).isZero()));
            assertThat(outer.getInput()).isInstanceOf(Project.class);
        });
        var converted = new SubstraitConverter().convertWithResult(
                planned.plan(), planned.preOptimizationColumnNames());
        var ir = Plan.parseFrom(new SubstraitPlanSerializer().serialize(converted.plan()));
        var callback = FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR,
                call.substring(0, call.indexOf('(')), "").orElseThrow();
        assertThat(scalarCalls(ir)).singleElement().satisfies(scalar ->
                assertThat(scalar.getFunctionReference())
                        .isEqualTo(functionAnchor(ir, callback.namespace(), callback.name())));
        var root = ir.getRelations(0).getRoot();
        var schema = SubstraitRelationLayout.rootSchema(root);
        assertThat(SubstraitRelationLayout.topLevelNames(schema))
                .containsExactly("first_result", "second_result");
        assertThat(root.getInput().hasProject()).isTrue();
        var outer = root.getInput().getProject();
        assertThat(outer.getExpressionsList()).hasSize(2).allSatisfy(expression ->
                assertThat(expression).isEqualTo(SubstraitRelationLayout.rootFieldReference(0)));
        var layouts = SubstraitRelationLayout.describeTree(root.getInput());
        assertThat(layouts.layout(root.getInput()).outputMapping()).containsExactly(1, 2);
        assertThat(layouts.layout(outer.getInput()).outputMapping()).containsExactly(1);
        var producerType = layouts.layout(outer.getInput()).outputSchema().getStruct().getTypes(0);
        assertThat(schema.getStruct().getTypesList()).containsExactly(producerType, producerType);
        assertThat(planned.toPlannedQuery().isResultCacheSafe()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "AI_COMPLETE('catalog.model', text)",
            "AI_EMBED('catalog.model', text)",
            "FILE_FROM_URL(text)"
    })
    void filterOfProducerAliasStaysAboveTheSingleProducer(String call) throws Exception {
        var compiler = new SqlCompiler();
        var planned = compiler.compileToResult(
                "SELECT generated FROM (SELECT " + call + " AS generated" + SOURCE
                        + ") produced WHERE generated IS NOT NULL");
        assertThat(planned.plan()).isInstanceOfSatisfying(Filter.class, filter ->
                assertThat(filter.getInput()).isInstanceOf(Project.class));
        var converted = new SubstraitConverter().convertWithResult(
                planned.plan(), planned.preOptimizationColumnNames());
        var ir = Plan.parseFrom(new SubstraitPlanSerializer().serialize(converted.plan()));
        var callback = FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR,
                call.substring(0, call.indexOf('(')), "").orElseThrow();
        int callbackAnchor = functionAnchor(ir, callback.namespace(), callback.name());
        int notAnchor = functionAnchor(ir, "boolean", "not");
        int nullAnchor = functionAnchor(ir, "comparison", "is_null");
        assertThat(scalarCalls(ir)).extracting(Expression.ScalarFunction::getFunctionReference)
                .containsExactlyInAnyOrder(callbackAnchor, notAnchor, nullAnchor);
        var relation = ir.getRelations(0).getRoot().getInput();
        assertThat(relation.hasFilter()).isTrue();
        var filter = relation.getFilter();
        assertThat(filter.getInput().hasProject()).isTrue();
        assertThat(scalarCalls(filter.getInput())).singleElement().satisfies(scalar ->
                assertThat(scalar.getFunctionReference()).isEqualTo(callbackAnchor));
        assertThat(SubstraitRelationLayout.describe(filter.getInput()).outputMapping())
                .containsExactly(1);
        assertThat(filter.getCondition().hasScalarFunction()).isTrue();
        var predicate = filter.getCondition().getScalarFunction();
        assertThat(predicate.getFunctionReference()).isEqualTo(notAnchor);
        assertThat(predicate.getArgumentsList()).singleElement().satisfies(argument -> {
            assertThat(argument.getValue().hasScalarFunction()).isTrue();
            var isNull = argument.getValue().getScalarFunction();
            assertThat(isNull.getFunctionReference()).isEqualTo(nullAnchor);
            assertThat(isNull.getArgumentsList()).singleElement().satisfies(input ->
                    assertThat(input.getValue()).isEqualTo(SubstraitRelationLayout.rootFieldReference(0)));
        });
        assertThat(converted.aiInvocationHints()).hasSize(call.startsWith("AI_") ? 1 : 0);
    }

    @Test
    void constantCallbackAndOptionalNullArgumentRemainExecutableCalls() throws Exception {
        var compiler = new SqlCompiler();
        var planned = compiler.compileToResult(
                "SELECT AI_COMPLETE('catalog.model', 'prompt', CAST(NULL AS VARCHAR)) AS result");
        var fragment = compiler.convertValidateAndSerializeFragment(
                planned.plan(), planned.preOptimizationColumnNames());
        assertThat(fragment.aiInvocationHints()).hasSize(1);
        assertThat(planned.plan()).isInstanceOf(Project.class);
        assertThat(scalarCalls(Plan.parseFrom(fragment.planBytes()))).singleElement()
                .satisfies(call -> assertThat(call.getArguments(2).getValue().getLiteral().hasNull()).isTrue());
    }

    @Test
    void liveCaseBranchRetainsCallbackAndSelectedRowConditionInFinalIr() throws Exception {
        var compiler = new SqlCompiler();
        var planned = compiler.compileToResult(
                "SELECT CASE WHEN text = 'a' THEN AI_COMPLETE('catalog.model', text) ELSE NULL END AS result"
                        + SOURCE);
        var fragment = compiler.convertValidateAndSerializeFragment(
                planned.plan(), planned.preOptimizationColumnNames());
        assertThat(fragment.aiInvocationHints()).hasSize(1);
        var relation = Plan.parseFrom(fragment.planBytes()).getRelations(0).getRoot().getInput();
        assertThat(relation.hasProject()).isTrue();
        assertThat(relation.getProject().getExpressionsList()).singleElement().satisfies(expression -> {
            assertThat(expression.hasIfThen()).isTrue();
            assertThat(expression.getIfThen().getIfsList()).singleElement().satisfies(branch -> {
                assertThat(branch.getIf().hasScalarFunction()).isTrue();
                assertThat(branch.getThen().hasScalarFunction()).isTrue();
            });
        });
    }

    @Test
    void deadCaseBranchDisappearsWhileOrdinaryConstantFoldingStillWorks() throws Exception {
        var compiler = new SqlCompiler();
        var planned = compiler.compileToResult(
                "SELECT CASE WHEN FALSE THEN AI_COMPLETE('catalog.model', 'unused') ELSE 'kept' END AS result,"
                        + " 1 + 2 AS folded");
        var fragment = compiler.convertValidateAndSerializeFragment(
                planned.plan(), planned.preOptimizationColumnNames());
        assertThat(fragment.aiInvocationHints()).isEmpty();
        var ir = Plan.parseFrom(fragment.planBytes());
        assertThat(scalarCalls(ir)).isEmpty();
        var root = ir.getRelations(0).getRoot();
        var types = SubstraitRelationLayout.rootSchema(root).getStruct().getTypesList();
        assertThat(types).hasSize(2);
        assertThat(types.get(0).hasString()).isTrue();
        assertThat(types.get(1).hasI32()).isTrue();
        assertThat(types.get(0).getString().getNullability()).isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        assertThat(types.get(1).getI32().getNullability()).isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(root.getInput().hasProject()).isTrue();
        var project = root.getInput().getProject();
        var layout = SubstraitRelationLayout.describe(root.getInput());
        assertThat(layout.outputMapping()).containsExactly(1, 2);
        assertThat(layout.expressionSchema().getStruct().getTypesCount()).isEqualTo(1);
        assertThat(project.getInput().hasRead()).isTrue();
        var rows = project.getInput().getRead().getVirtualTable().getExpressionsList();
        assertThat(rows).singleElement().satisfies(row -> assertThat(row.getFieldsList()).singleElement()
                .satisfies(seed -> {
                    assertThat(seed.hasLiteral()).isTrue();
                    assertThat(seed.getLiteral().hasI32()).isTrue();
                    assertThat(seed.getLiteral().getI32()).isZero();
                }));
        assertThat(project.getExpressionsList()).hasSize(2);
        for (int output = 0; output < layout.outputMapping().size(); output++) {
            var expression = project.getExpressions(layout.outputMapping().get(output)
                    - layout.expressionSchema().getStruct().getTypesCount());
            assertThat(SubstraitRelationLayout.typeOf(expression, layout.expressionSchema()))
                    .isEqualTo(types.get(output));
        }
        var text = literalBelowCasts(project.getExpressions(0));
        assertThat(text.hasString()).isTrue();
        assertThat(text.getString()).isEqualTo("kept");
        var number = literalBelowCasts(project.getExpressions(1));
        assertThat(number.hasI32()).isTrue();
        assertThat(number.getI32()).isEqualTo(3);
    }

    private static int functionAnchor(Plan plan, String namespace, String name) {
        var functions = plan.getExtensionsList().stream().filter(extension -> extension.hasExtensionFunction())
                .map(extension -> extension.getExtensionFunction())
                .filter(function -> function.getName().equals(name + ":")).toList();
        assertThat(functions).hasSize(1);
        var function = functions.get(0);
        assertThat(plan.getExtensionUrnsList().stream()
                .filter(urn -> urn.getExtensionUrnAnchor() == function.getExtensionUrnReference())
                .map(urn -> urn.getUrn()).toList()).containsExactly(FunctionBindingDeclarations.canonicalUrn(
                        FunctionKind.FUNCTION_KIND_SCALAR, namespace, name));
        return function.getFunctionAnchor();
    }

    private static Expression.Literal literalBelowCasts(Expression expression) {
        while (expression.hasCast()) {
            expression = expression.getCast().getInput();
        }
        assertThat(expression.hasLiteral()).isTrue();
        return expression.getLiteral();
    }

    private static List<Expression.ScalarFunction> scalarCalls(Message message) {
        List<Expression.ScalarFunction> result = new ArrayList<>();
        if (message instanceof Expression.ScalarFunction scalar) {
            result.add(scalar);
        }
        for (Object value : message.getAllFields().values()) {
            if (value instanceof Message nested) {
                result.addAll(scalarCalls(nested));
            } else if (value instanceof List<?> values) {
                for (Object item : values) {
                    if (item instanceof Message nested) {
                        result.addAll(scalarCalls(nested));
                    }
                }
            }
        }
        return result;
    }
}
