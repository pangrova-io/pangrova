package org.pangrova.coordinator.property.query;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.SqlCompiler;
import org.apache.calcite.rel.RelNode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 7: Constant Folding path correctness.
 *
 * <p>For any constant expression query (e.g. SELECT 1, SELECT 1+2, SELECT 'abc'),
 * the planner may fold the expression, but execution still belongs to Native.
 *
 * <p><b>Validates: Requirements 1.9</b>
 */
@Tag("Feature: full-pipeline-audit-fix, Property 7: Constant Folding path correctness")
class ConstantFoldingPropertyTest {

    private final SqlCompiler compiler = new SqlCompiler();

    // ── Generators ──

    @Provide
    Arbitrary<String> constantIntSql() {
        return Arbitraries.integers().between(1, 9999)
                .map(n -> "SELECT " + n);
    }

    @Provide
    Arbitrary<String> arithmeticSql() {
        // Use positive integers to avoid edge cases where Calcite
        // Interpreter evaluation may fail (e.g. division by zero)
        Arbitrary<Integer> ints = Arbitraries.integers().between(1, 999);
        Arbitrary<String> ops = Arbitraries.of("+", "-", "*");
        return Combinators.combine(ints, ops, ints)
                .as((a, op, b) -> "SELECT " + a + " " + op + " " + b);
    }

    @Provide
    Arbitrary<String> stringLiteralSql() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "SELECT '" + s + "'");
    }

    @Provide
    Arbitrary<String> booleanSql() {
        return Arbitraries.of("SELECT TRUE", "SELECT FALSE");
    }

    @Provide
    Arbitrary<String> multiColumnConstantSql() {
        Arbitrary<Integer> ints = Arbitraries.integers().between(1, 100);
        Arbitrary<String> strs = Arbitraries.strings().alpha()
                .ofMinLength(1).ofMaxLength(10);
        return Combinators.combine(ints, strs)
                .as((n, s) -> "SELECT " + n + ", '" + s + "', TRUE");
    }

    @Provide
    Arbitrary<String> allConstantSql() {
        return Arbitraries.oneOf(
                constantIntSql(),
                arithmeticSql(),
                stringLiteralSql(),
                booleanSql(),
                multiColumnConstantSql()
        );
    }

    // ── Property 7: Constant Folding path correctness ──

    /**
     * Property 7a: Planner detection — simple constant literals are recognized
     * as foldable by Calcite (LogicalValues with no inputs).
     */
    @Property(tries = 10)
    void simpleLiteralsDetectedAsFoldedConstant(
            @ForAll("constantIntSql") String sql) {
        RelNode relNode = compiler.compileToRelNode(sql);
        assertThat(PangrovaPlanner.isFoldedConstant(relNode))
                .as("Simple literal SQL '%s' should be detected as folded constant", sql)
                .isTrue();
    }

}
