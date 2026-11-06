package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.error.PlannerException;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;
import org.pangrova.planner.core.validation.NativeCapabilityRegistry;
import org.pangrova.planner.core.validation.SubstraitPlanValidator;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Admission must not turn a governed invocation into a general fallback permission. */
class NativeAdmissionTest {

    @Test
    void defersOnlyTheExactGovernedInvocation() {
        SqlCompiler compiler = compiler(Set.of("ReadRel", "ProjectRel"),
                Set.of("FieldReference", "Literal", "ScalarFunction"));
        var plan = compiler.compileToResult("SELECT AI_COMPLETE('catalog.model', name) FROM people");
        assertThat(compiler.convertValidateAndSerializeFragment(plan.plan(), plan.preOptimizationColumnNames())
                .aiInvocationHints()).singleElement()
                .satisfies(hint -> assertThat(hint.functionName()).isEqualTo("ai_complete"));
    }

    @Test
    void governedInvocationDoesNotAuthorizeAnotherUnsupportedFunction() {
        SqlCompiler compiler = compiler(Set.of("ReadRel", "ProjectRel"),
                Set.of("FieldReference", "Literal", "ScalarFunction"));
        var plan = compiler.compileToResult(
                "SELECT AI_COMPLETE('catalog.model', name), UPPER(name) FROM people");
        assertThatThrownBy(() -> compiler.convertValidateAndSerialize(
                plan.plan(), plan.preOptimizationColumnNames()))
                .isInstanceOf(PlannerException.class).hasMessageContaining("upper");
    }

    @Test
    void governedInvocationDoesNotAuthorizeAnUnsupportedRelation() {
        SqlCompiler compiler = compiler(Set.of("ReadRel"),
                Set.of("FieldReference", "Literal", "ScalarFunction"));
        var plan = compiler.compileToResult("SELECT AI_COMPLETE('catalog.model', name) FROM people");
        assertThatThrownBy(() -> compiler.convertValidateAndSerialize(
                plan.plan(), plan.preOptimizationColumnNames()))
                .isInstanceOf(PlannerException.class).hasMessageContaining("ProjectRel");
    }

    @Test
    void governedInvocationDoesNotAuthorizeAnUnsupportedExpressionKind() {
        SqlCompiler compiler = compiler(Set.of("ReadRel", "ProjectRel"),
                Set.of("FieldReference", "Literal"));
        var plan = compiler.compileToResult("SELECT AI_COMPLETE('catalog.model', name) FROM people");
        assertThatThrownBy(() -> compiler.convertValidateAndSerialize(
                plan.plan(), plan.preOptimizationColumnNames()))
                .isInstanceOf(PlannerException.class).hasMessageContaining("ScalarFunction");
    }

    private static SqlCompiler compiler(Set<String> relations, Set<String> expressions) {
        var registry = NativeCapabilityRegistry.defaults();
        registry.update(relations, expressions, Set.of());
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("people", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("name", SqlTypeName.VARCHAR, 100).build();
            }
        });
        return new SqlCompiler(new PangrovaPlanner(schema), new SubstraitConverter(),
                new SubstraitPlanSerializer(), new SubstraitPlanValidator(registry));
    }
}
