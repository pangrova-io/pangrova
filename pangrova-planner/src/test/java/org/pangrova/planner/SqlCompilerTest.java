package org.pangrova.planner;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.PlanResult;

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
    void convertAndSerializeProducesSameBytesAsCompile() {
        String sql = "SELECT 1 + 2";
        byte[] directBytes = compiler.compile(sql);
        PlanResult result = compiler.compileToResult(sql);
        byte[] splitBytes = compiler.convertAndSerialize(result.plan(), result.preOptimizationColumnNames());
        assertThat(splitBytes).isEqualTo(directBytes);
    }
}
