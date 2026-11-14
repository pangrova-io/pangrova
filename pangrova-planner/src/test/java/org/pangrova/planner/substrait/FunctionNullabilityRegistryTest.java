package org.pangrova.planner.substrait;

import org.pangrova.planner.enums.FunctionNullability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class FunctionNullabilityRegistryTest {

    private final FunctionNullabilityRegistry registry = new FunctionNullabilityRegistry();

    private static FunctionReference ref(String ns, String name) {
        return new FunctionReference(ns, name);
    }

    @Test
    void comparisonMirrorsArgumentNullability() {
        FunctionReference lt = ref("comparison", "lt");
        assertThat(registry.rule(lt)).isEqualTo(FunctionNullability.NULL_IF_ANY_NULL);
        assertThat(registry.outputNullable(lt, List.of(false, false))).isFalse();
        assertThat(registry.outputNullable(lt, List.of(false, true))).isTrue();
    }

    @Test
    void arithmeticMirrorsArgumentNullability() {
        assertThat(registry.outputNullable(ref("arithmetic", "add"), List.of(true, false))).isTrue();
        assertThat(registry.outputNullable(ref("arithmetic", "add"), List.of(false, false))).isFalse();
    }

    @Test
    void nullPredicatesAreNeverNull() {
        assertThat(registry.rule(ref("comparison", "is_null"))).isEqualTo(FunctionNullability.NEVER_NULL);
        // Even a nullable argument yields a non-null boolean.
        assertThat(registry.outputNullable(ref("comparison", "is_null"), List.of(true))).isFalse();
        assertThat(registry.outputNullable(ref("comparison", "is_not_null"), List.of(true))).isFalse();
    }

    @Test
    void coalesceIsNullOnlyIfAllArgsNullable() {
        FunctionReference coalesce = ref("boolean", "coalesce");
        assertThat(registry.rule(coalesce)).isEqualTo(FunctionNullability.NULL_IF_ALL_NULL);
        assertThat(registry.outputNullable(coalesce, List.of(true, true, true))).isTrue();
        assertThat(registry.outputNullable(coalesce, List.of(true, false, true))).isFalse();
    }

    @Test
    void booleanAndOrAreNullableUnderThreeValuedLogic() {
        // and(null,false)=false and or(null,true)=true, so they are not pure MIRROR; declared nullable.
        assertThat(registry.rule(ref("boolean", "and"))).isEqualTo(FunctionNullability.ALWAYS_NULLABLE);
        assertThat(registry.rule(ref("boolean", "or"))).isEqualTo(FunctionNullability.ALWAYS_NULLABLE);
        // not(null)=null → mirrors.
        assertThat(registry.rule(ref("boolean", "not"))).isEqualTo(FunctionNullability.NULL_IF_ANY_NULL);
    }

    @Test
    void countAndSum0AreNonNullButSumIsNullable() {
        assertThat(registry.outputNullable(ref("aggregate", "count"), List.of(true))).isFalse();
        assertThat(registry.outputNullable(ref("aggregate", "sum0"), List.of(true))).isFalse();
        // sum/min/max/avg return null on empty groups.
        assertThat(registry.outputNullable(ref("aggregate", "sum"), List.of(false))).isTrue();
        assertThat(registry.outputNullable(ref("aggregate", "max"), List.of(false))).isTrue();
    }

    @Test
    void unknownFunctionFallsBackToAlwaysNullable() {
        FunctionReference unknown = ref("custom", "mystery_udf");
        assertThat(registry.isDeclared(unknown)).isFalse();
        assertThat(registry.rule(unknown)).isEqualTo(FunctionNullability.ALWAYS_NULLABLE);
        assertThat(registry.outputNullable(unknown, List.of(false, false))).isTrue();
    }

    @Test
    void declaredFunctionsAreReportedAsDeclared() {
        assertThat(registry.isDeclared(ref("string", "upper"))).isTrue();
        assertThat(registry.isDeclared(ref("comparison", "equal"))).isTrue();
    }

    @Test
    void nullArgumentsRejected() {
        assertThatThrownBy(() -> registry.rule(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> registry.outputNullable(ref("string", "upper"), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void everyFunctionMapperEntryHasADeclaredRule() {
        // Guards against drift: each function FunctionMapper maps should have explicit nullability.
        List<FunctionReference> mapped = List.of(
                ref("arithmetic", "add"), ref("arithmetic", "subtract"),
                ref("arithmetic", "multiply"), ref("arithmetic", "divide"),
                ref("comparison", "equal"), ref("comparison", "not_equal"),
                ref("comparison", "lt"), ref("comparison", "gt"),
                ref("comparison", "lte"), ref("comparison", "gte"),
                ref("comparison", "is_null"), ref("comparison", "is_not_null"),
                ref("boolean", "and"), ref("boolean", "or"),
                ref("boolean", "not"), ref("boolean", "coalesce"),
                ref("string", "concat"), ref("string", "upper"), ref("string", "lower"),
                ref("string", "substring"), ref("string", "trim"), ref("string", "char_length"),
                ref("string", "like"),
                ref("aggregate", "sum"), ref("aggregate", "sum0"), ref("aggregate", "count"),
                ref("aggregate", "min"), ref("aggregate", "max"), ref("aggregate", "avg"),
                ref("aggregate", "approx_distinct"));
        assertThat(mapped).allMatch(registry::isDeclared);
    }
}
